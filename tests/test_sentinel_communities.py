"""Community adapters and persistent respectful backoff. No live site traffic."""
import asyncio
from datetime import timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from qa_sentinel.core import Database, now, stamp, parse_date
from qa_sentinel.crawlers import CrawlBatch, crawl, safe_get, validate_community_response
from qa_sentinel.politeness import SiteBlocked, CooldownActive, REQUEST_GUARD, retry_after_seconds, wait_request_slot


def dc_source(ident):
    return {"id": ident, "name": ident, "kind": "dcinside", "url": f"https://gall.dcinside.com/board/lists/?id={ident}", "enabled": True, "config": {}}


def arca_source():
    return {"id": "arca", "name": "게임 채널", "kind": "arcalive", "url": "https://arca.live/b/game", "enabled": True, "config": {}}


def post():
    return {"title": "접속 불가", "body": "최근 테스트 글", "url": "https://gall.dcinside.com/board/view/?id=one&no=123",
            "published_at": stamp(now()-timedelta(seconds=5))}


class CommunityAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_arcalive_extracts_post_without_notice_crossboard_or_duplicate(self):
        listing = '''<div class="article-list">
            <a class="vrow" href="/b/game/999"><span class="col-id">공지</span><span class="col-title"><span class="title">notice</span></span></a>
            <a class="vrow" href="/b/game/123"><span class="col-id">123</span><span class="col-title"><span class="title">레이드 접속 불가</span></span><span>글쓴이</span><time datetime="2026-10-17T12:00:00Z"></time></a>
            <a class="vrow" href="/b/other/456"><span class="col-id">456</span><span class="title">other board</span></a>
            <a class="vrow" href="/b/game/123?utm_source=duplicate"><span class="col-id">123</span><span class="title">duplicate</span></a>
            </div>'''.encode()
        get = AsyncMock(return_value=(listing, "https://arca.live/b/game"))
        with patch('qa_sentinel.crawlers.safe_get', get), patch('qa_sentinel.crawlers.asyncio.sleep', AsyncMock()):
            batch = await crawl(arca_source(), set())
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0]['title'], '레이드 접속 불가')
        self.assertEqual(batch[0]['body'], '')
        self.assertEqual(batch[0]['published_at'], '2026-10-17T12:00:00+00:00')
        self.assertEqual(get.await_count, 1)

    async def test_arcalive_known_post_is_not_refetched(self):
        listing = b'<div class="article-list"><a class="vrow" href="/b/game/123"><span class="col-id">123</span><span class="title">known</span></a></div>'
        get = AsyncMock(return_value=(listing, 'https://arca.live/b/game'))
        with patch('qa_sentinel.crawlers.safe_get', get):
            self.assertEqual(await crawl(arca_source(), {'https://arca.live/b/game/123'}), [])
        self.assertEqual(get.await_count, 1)

    async def test_dc_blocked_listing_stops_without_retry(self):
        blocked = SiteBlocked('gall.dcinside.com', '접근 차단')
        get = AsyncMock(side_effect=blocked)
        with patch('qa_sentinel.crawlers.safe_get', get):
            with self.assertRaises(SiteBlocked):
                await crawl(dc_source('one'), set())
        self.assertEqual(get.await_count, 1)

    async def test_dc_reads_up_to_100_rows_in_one_listing_request(self):
        listing = b'<table>' + b''.join(f'<tr class="ub-content"><td class="gall_num">{i}</td><td class="gall_tit"><a href="/board/view/?id=one&amp;no={i}">post {i}</a></td><td class="gall_date" title="2026.10.17 21:00:00"></td></tr>'.encode() for i in range(150)) + b'</table>'
        get = AsyncMock(return_value=(listing, dc_source('one')['url']))
        with patch('qa_sentinel.crawlers.safe_get', get):
            batch = await crawl(dc_source('one'), set())
        self.assertEqual(len(batch), 100)
        self.assertTrue(all(post['body'] == '' for post in batch))
        self.assertEqual(get.await_count, 1)

    def test_blank_body_with_normal_title_is_suspected_block_not_success(self):
        with self.assertRaisesRegex(SiteBlocked, '차단 의심'):
            validate_community_response(b'<html><head><title>DC Inside</title></head><body> </body></html>', 'dcinside.com')

    def test_real_post_about_captcha_is_not_mistaken_for_challenge(self):
        validate_community_response(b'<title>Captcha problem report</title><div class="article-content">How to report captcha bugs</div>', 'arca.live')

    async def test_http_403_honors_retry_after_without_a_retry(self):
        requests = []
        def handler(request):
            requests.append(request)
            return httpx.Response(403, headers={'Retry-After': '3600'}, content=b'blocked')
        dns = [(2, 1, 6, '', ('8.8.8.8', 443))]
        with patch('qa_sentinel.crawlers.socket.getaddrinfo', return_value=dns), patch('qa_sentinel.crawlers.wait_request_slot', AsyncMock()):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
                with self.assertRaises(SiteBlocked) as result:
                    await safe_get(client, dc_source('one')['url'])
        self.assertEqual(result.exception.retry_after, 3600)
        self.assertEqual(len(requests), 1)

    async def test_active_cooldown_stops_request_before_dns(self):
        def guard(domain):
            raise CooldownActive('자동 대기')
        token = REQUEST_GUARD.set(guard)
        try:
            with patch('qa_sentinel.crawlers.socket.getaddrinfo') as dns:
                async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client:
                    with self.assertRaises(CooldownActive):
                        await safe_get(client, dc_source('one')['url'])
                dns.assert_not_called()
        finally:
            REQUEST_GUARD.reset(token)

    async def test_empty_http_200_is_not_treated_as_success(self):
        transport = httpx.MockTransport(lambda _: httpx.Response(200, content=b''))
        dns = [(2, 1, 6, '', ('8.8.8.8', 443))]
        with patch('qa_sentinel.crawlers.socket.getaddrinfo', return_value=dns), patch('qa_sentinel.crawlers.wait_request_slot', AsyncMock()):
            async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
                with self.assertRaisesRegex(SiteBlocked, '차단 의심'):
                    await safe_get(client, dc_source('one')['url'])

    async def test_redirect_cannot_request_a_site_in_cooldown(self):
        requests = []
        def handler(request):
            requests.append(str(request.url))
            return httpx.Response(302, headers={'Location':dc_source('one')['url']})
        def guard(domain):
            if domain == 'dcinside.com':
                raise CooldownActive('자동 대기')
        token = REQUEST_GUARD.set(guard)
        dns = [(2, 1, 6, '', ('8.8.8.8', 443))]
        try:
            with patch('qa_sentinel.crawlers.socket.getaddrinfo', return_value=dns):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
                    with self.assertRaises(CooldownActive):
                        await safe_get(client, 'https://example.com/feed')
            self.assertEqual(requests, ['https://example.com/feed'])
        finally:
            REQUEST_GUARD.reset(token)

    async def test_dc_request_pacing_uses_random_gaps_with_a_hard_minimum(self):
        class Clock:
            value = 0
            def time(self):
                return self.value
        clock = Clock()
        async def advance(seconds):
            clock.value += seconds
        sleep = AsyncMock(side_effect=advance)
        with patch('qa_sentinel.politeness.asyncio.get_running_loop', return_value=clock), patch('qa_sentinel.politeness.asyncio.sleep', sleep), patch('qa_sentinel.politeness.random.uniform', side_effect=[10.0, 19.5]) as jitter:
            for _ in range(3):
                await wait_request_slot('dcinside.com')
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [10.0, 19.5])
        self.assertEqual([c.args for c in jitter.call_args_list], [(10.0,20.0), (10.0,20.0)])

    def test_retry_after_http_date_and_invalid_value(self):
        at = parse_date('2026-10-17T12:00:00Z')
        self.assertEqual(retry_after_seconds('Sat, 17 Oct 2026 13:00:00 GMT', at), 3600)
        self.assertEqual(retry_after_seconds('invalid', at), 1800)


class CollectionBackoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from qa_sentinel import app
        self.app = app
        self.temp = tempfile.TemporaryDirectory()
        self.saved = {k: getattr(app, k) for k in ('DATA', 'DATABASES', 'OFFLINE_TEST', 'collection_lock', 'collection_status', 'collection_tasks')}
        app.DATA, app.DATABASES, app.OFFLINE_TEST = Path(self.temp.name), {}, False
        app.collection_lock, app.collection_tasks = asyncio.Lock(), set()
        app.collection_status = {'started_at': None, 'finished_at': None, 'total': 0, 'completed': 0, 'results': []}
        self.db = app.database('live')

    async def asyncTearDown(self):
        for key, value in self.saved.items():
            setattr(self.app, key, value)
        self.temp.cleanup()

    async def test_one_block_defers_all_dc_channels_but_other_sites_continue(self):
        self.db.save_source(dc_source('one'))
        self.db.save_source(dc_source('two'))
        self.db.save_source({'id':'rss','name':'rss','kind':'rss','url':'https://example.com/feed','enabled':True,'config':{}})
        calls = []
        async def fetch(src, known):
            calls.append(src['id'])
            if src['kind'] == 'dcinside':
                raise SiteBlocked('gall.dcinside.com', '빈 화면 응답(차단 의심)')
            return [post()]
        with patch('qa_sentinel.app.crawl', fetch):
            result = await self.app.collect()
        self.assertEqual(len([i for i in calls if i in ('one','two')]), 1)
        self.assertIn('rss', calls)
        self.assertEqual(len(self.db.posts()), 1)
        self.assertEqual(len([r for r in result['results'] if 'skipped' in r]), 1)
        for src in self.app.state('live')['sources']:
            if src['kind'] == 'dcinside':
                self.assertIsNotNone(src['cooldown_until'])

    async def test_pause_survives_restart_and_source_toggle_and_manual_collect(self):
        src = dc_source('one')
        self.db.save_source(src)
        original = self.db.defer_site('dcinside.com', '접근 제한')
        for enabled in (False, True):
            self.db.save_source({**src, 'enabled': enabled})
        self.app.DATABASES['live'] = Database(self.db.path)
        with patch('qa_sentinel.app.crawl', AsyncMock()) as fetch:
            result = await self.app.collect()
        fetch.assert_not_awaited()
        self.assertIn('자동 대기', result['message'])
        self.assertEqual(self.app.DATABASES['live'].site_cooldown('dcinside.com')['until_at'], original['until_at'])

    async def test_partial_block_preserves_posts_and_still_defers_site(self):
        self.db.save_source(dc_source('one'))
        blocked = SiteBlocked('dcinside.com', '사이트 제한')
        batch = CrawlBatch([post()], ['마지막 글에서 사이트 제한'], blocked=blocked)
        with patch('qa_sentinel.app.crawl', AsyncMock(return_value=batch)):
            result = await self.app.collect()
        self.assertEqual(result['results'][0]['inserted'], 1)
        self.assertIsNotNone(self.db.site_cooldown('dcinside.com'))
        self.assertIn('사이트 제한', self.db.sources()[0]['error'])

    async def test_manual_collect_on_dc_cannot_repeat_within_fifteen_minutes(self):
        self.db.save_source(dc_source('one'))
        with patch('qa_sentinel.app.crawl', AsyncMock(return_value=[])) as fetch:
            await self.app.collect()
            await self.app.collect()
        self.assertEqual(fetch.await_count, 1)

    def test_repeated_blocks_extend_backoff_and_old_success_cannot_clear_it(self):
        at = now()
        first = self.db.defer_site('dcinside.com', 'first', at=at)
        second = self.db.defer_site('dcinside.com', 'second', at=at+timedelta(minutes=31))
        self.assertEqual(parse_date(first['until_at'])-parse_date(stamp(at)), timedelta(minutes=30))
        self.assertEqual(parse_date(second['until_at'])-parse_date(stamp(at+timedelta(minutes=31))), timedelta(minutes=60))
        self.db.site_recovered('dcinside.com')
        self.assertIsNotNone(self.db.site_cooldown('dcinside.com'))


class CommunityAPITests(unittest.TestCase):
    def setUp(self):
        from qa_sentinel import app
        self.app = app
        self.temp = tempfile.TemporaryDirectory()
        self.saved = app.DATA, app.DATABASES, app.OFFLINE_TEST
        app.DATA, app.DATABASES, app.OFFLINE_TEST = Path(self.temp.name), {}, False
        self.client = TestClient(app.app)

    def tearDown(self):
        self.client.close()
        self.app.DATA, self.app.DATABASES, self.app.OFFLINE_TEST = self.saved
        self.temp.cleanup()

    def test_arcalive_channel_can_be_saved_without_css_selectors(self):
        result = self.client.put('/api/sources?mode=live', json=arca_source())
        self.assertEqual(result.status_code, 200)
        self.assertEqual(self.client.get('/api/state?mode=live').json()['sources'][0]['kind'], 'arcalive')

    def test_arcalive_wrong_host_or_article_url_is_rejected(self):
        for url in ('https://example.com/b/game', 'https://arca.live/b/game/123'):
            with self.subTest(url=url):
                self.assertEqual(self.client.put('/api/sources?mode=live', json={**arca_source(),'url':url}).status_code, 400)

    def test_background_manual_collect_reports_active_site_pause(self):
        self.client.put('/api/sources?mode=live', json=dc_source('one'))
        self.app.database('live').defer_site('dcinside.com','빈 화면 응답')
        result = self.client.post('/api/collect?mode=live&background=true').json()
        self.assertFalse(result['started'])
        self.assertIn('자동 대기', result['message'])

    def test_background_collect_does_not_claim_start_without_a_ready_channel(self):
        first = self.client.post('/api/collect?mode=live&background=true').json()
        self.assertFalse(first['started'])
        self.client.put('/api/sources?mode=live', json=dc_source('one'))
        with self.app.database('live').connect() as db:
            db.execute('UPDATE sources SET last_attempt=?', (stamp(now()),))
        with patch('qa_sentinel.app.crawl', AsyncMock()) as fetch:
            result = self.client.post('/api/collect?mode=live&background=true').json()
        self.assertFalse(result['started'])
        self.assertIn('15분', result['message'])
        fetch.assert_not_awaited()
