"""Title-only collection, shared-host limits and rotation without block retries."""
import asyncio
from datetime import timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from qa_sentinel.core import Database, now, stamp
from qa_sentinel.crawlers import USER_AGENTS, crawl, safe_get
from qa_sentinel.politeness import SiteBlocked, interval_seconds


def source(ident='one'):
    return {'id': ident, 'name': ident, 'kind': 'rss', 'url': 'https://example.com/feed', 'enabled': True, 'config': {}}


class TitleOnlyAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_dc_list_date_never_falls_back_to_detail(self):
        listing = b'<table><tr class="ub-content"><td class="gall_num">1</td><td class="gall_tit"><a href="/board/view/?id=test&amp;no=1">crash</a></td></tr></table>'
        get = AsyncMock(return_value=(listing, 'https://gall.dcinside.com/board/lists/?id=test'))
        with patch('qa_sentinel.crawlers.safe_get', get):
            with self.assertRaisesRegex(ValueError, '목록에서 작성 시각'):
                await crawl({**source(), 'kind': 'dcinside'}, set())
        self.assertEqual(get.await_count, 1)

    async def test_old_html_body_selector_is_unused_and_list_date_is_required(self):
        listing = b'<div class="row"><a href="/post">crash</a><time datetime="2026-10-17T12:00:00Z"></time><article>must not be saved</article></div>'
        get = AsyncMock(return_value=(listing, 'https://example.com/list'))
        cfg = {'item_selector': '.row', 'link_selector': 'a', 'time_selector': 'time', 'body_selector': 'article'}
        with patch('qa_sentinel.crawlers.safe_get', get):
            posts = await crawl({**source(), 'kind': 'html', 'config': cfg}, set())
        self.assertEqual(posts[0]['body'], '')
        self.assertEqual(posts[0]['published_at'], '2026-10-17T12:00:00+00:00')
        self.assertEqual(get.await_count, 1)

    async def test_user_agent_rotates_between_runs_and_stays_fixed_through_redirect(self):
        clients, observed = [], []
        real_client = httpx.AsyncClient

        def handler(request):
            observed.append((request.url.path, request.headers['user-agent']))
            if request.url.path == '/feed':
                return httpx.Response(302, headers={'Location': '/rss'})
            return httpx.Response(200, content=b'<rss><channel><item><title>crash</title><link>https://example.com/1</link><pubDate>2026-10-17T12:00:00Z</pubDate><description>never saved</description></item></channel></rss>')

        def factory(**kwargs):
            client = real_client(transport=httpx.MockTransport(handler), trust_env=False, **kwargs)
            clients.append(client)
            return client

        dns = [(2, 1, 6, '', ('8.8.8.8', 443))]
        with patch('qa_sentinel.crawlers.httpx.AsyncClient', side_effect=factory), patch('qa_sentinel.crawlers.socket.getaddrinfo', return_value=dns), patch('qa_sentinel.crawlers.wait_request_slot', AsyncMock()), patch('qa_sentinel.crawlers.random.choice', side_effect=USER_AGENTS[:2]) as choose:
            for _ in range(2):
                posts = await crawl(source(), set())
                self.assertEqual(posts[0]['body'], '')
        self.assertEqual(choose.call_count, 2)
        self.assertEqual(len(observed), 4)
        self.assertEqual(observed[0][1], observed[1][1])
        self.assertEqual(observed[2][1], observed[3][1])
        self.assertNotEqual(observed[0][1], observed[2][1])
        self.assertTrue(all('QASentinel/' in ua for _, ua in observed))

    async def test_generic_site_403_and_503_pause_without_retry_or_rotation(self):
        dns = [(2, 1, 6, '', ('8.8.8.8', 443))]
        for status in (403, 429, 503):
            requests = []
            def handler(request):
                requests.append(request)
                return httpx.Response(status, headers={'Retry-After': '7200'})
            with self.subTest(status=status), patch('qa_sentinel.crawlers.socket.getaddrinfo', return_value=dns), patch('qa_sentinel.crawlers.wait_request_slot', AsyncMock()):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
                    with self.assertRaises(SiteBlocked) as blocked:
                        await safe_get(client, 'https://example.com/feed')
            self.assertEqual(blocked.exception.retry_after, 7200)
            self.assertEqual(len(requests), 1)

    def test_random_request_intervals_never_fall_below_site_minimum(self):
        for domain, lower, upper in [('dcinside.com',10,20), ('arca.live',8,16), ('example.com',5,10)]:
            samples = [interval_seconds(domain) for _ in range(100)]
            self.assertTrue(all(lower <= value <= upper for value in samples))
            self.assertGreater(len(set(samples)), 1)


class TitleOnlyCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from qa_sentinel import app
        self.app = app
        self.temp = tempfile.TemporaryDirectory()
        self.saved = {key: getattr(app, key) for key in ['DATA','DATABASES','OFFLINE_TEST','collection_lock','collection_tasks','collection_status']}
        app.DATA, app.DATABASES, app.OFFLINE_TEST = Path(self.temp.name), {}, False
        app.collection_lock, app.collection_tasks = asyncio.Lock(), set()
        app.collection_status = {'started_at':None,'finished_at':None,'total':0,'completed':0,'results':[]}
        self.db = app.database('live')
        self.db.begin_session(now() - timedelta(minutes=2))

    async def asyncTearDown(self):
        for key,value in self.saved.items():
            setattr(self.app,key,value)
        self.temp.cleanup()

    async def test_manual_and_scheduled_rss_both_enforce_15_minutes_across_restart(self):
        self.db.save_source(source())
        at = now().replace(microsecond=0)
        with patch('qa_sentinel.app.now', return_value=at), patch('qa_sentinel.app.crawl', AsyncMock(return_value=[])) as fetch:
            await self.app.collect()
            self.app.DATABASES['live'] = Database(self.db.path)
            for offset in (300, 899):
                with patch('qa_sentinel.app.now', return_value=at+timedelta(seconds=offset)):
                    await self.app.collect()
                    await self.app.collect(due_only=True)
            self.assertEqual(fetch.await_count, 1)
            with patch('qa_sentinel.app.now', return_value=at+timedelta(seconds=900)):
                await self.app.collect()
            self.assertEqual(fetch.await_count, 2)

    async def test_all_channels_on_same_site_are_serialized(self):
        for ident in ('one','two','three'):
            self.db.save_source(source(ident))
        active = maximum = 0
        async def fetch(src, known):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            return []
        with patch('qa_sentinel.app.crawl', fetch):
            await self.app.collect()
        self.assertEqual(maximum, 1)
        self.assertEqual(self.app.collection_status['completed'], 3)

    async def test_legacy_body_is_preserved_but_never_matches_or_exports(self):
        self.app.OFFLINE_TEST = True
        self.db.save_source(source())
        self.db.ingest('one', [{'title':'일반 질문','body':'접속 불가','published_at':stamp(now()-timedelta(seconds=5)),'url':'https://example.com/1'}])
        self.assertEqual(self.db.posts()[0]['body'], '')
        # Simulate a database collected before title-only mode was introduced.
        with self.db.connect() as conn:
            conn.execute("UPDATE posts SET body='접속 불가'")
        reopened = Database(self.db.path)
        self.assertEqual(reopened.posts()[0]['body'], '접속 불가')
        self.assertEqual(reopened.detect()['count'], 0)
        with TestClient(self.app.app) as client:
            health = client.get('/api/health').json()
            self.assertEqual(health['poll_interval_seconds'], 900)
            self.assertEqual(health['collection_scope'], 'title_only')
            exported = client.get('/api/export?mode=live').text
            self.assertNotIn('본문', exported)
            self.assertNotIn('접속 불가', exported)

    def test_html_channel_requires_only_three_listing_selectors(self):
        self.app.OFFLINE_TEST = True
        with TestClient(self.app.app) as client:
            src = {**source(), 'kind':'html', 'config':{'item_selector':'.row','link_selector':'a','time_selector':'time'}}
            self.assertEqual(client.put('/api/sources?mode=live',json=src).status_code, 200)
            src['config'].pop('time_selector')
            self.assertEqual(client.put('/api/sources?mode=live',json=src).status_code, 400)


if __name__ == '__main__':
    unittest.main()
