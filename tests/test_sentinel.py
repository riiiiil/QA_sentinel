"""Meaningful engine, ingestion adapter and API checks; no external requests."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from qa_sentinel.core import Database, DEFAULT_KEYWORDS, canonical_url, match_keywords, stamp
from qa_sentinel.crawlers import crawl, date_value, parse_feed, safe_get

AT = datetime(2026, 10, 17, 12, tzinfo=timezone.utc)


def post(i, age=1, title="접속 불가 후 팅김"):
    return {"title": title, "body": "재현 설명", "published_at": stamp(AT - timedelta(minutes=age)),
            "url": f"https://example.com/post/{i}"}


def source(ident="test"):
    return {"id": ident, "name": ident, "kind": "rss", "url": "https://example.com/feed", "enabled": True, "config": {}}


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / "test.db")
        self.db.save_source(source())

    def tearDown(self):
        self.temp.cleanup()

    def test_matching_title_case_unicode_and_disabled(self):
        self.assertIn("팅김", match_keywords("ＣＲＡＳＨ", "", DEFAULT_KEYWORDS))
        self.assertEqual(match_keywords("팅김", "", [{"name": "팅김", "terms": ["팅김"], "enabled": False}]), [])

    def test_thresholds_count_unique_posts_not_keywords(self):
        self.db.ingest("test", [post(i) for i in range(2)], AT)
        self.assertEqual(self.db.detect(AT)["level"], "normal")
        self.db.ingest("test", [post(2)], AT)
        self.assertEqual(self.db.detect(AT)["level"], "warning")
        self.db.ingest("test", [post(3), post(4)], AT)
        result = self.db.detect(AT)
        self.assertEqual((result["level"], result["count"]), ("danger", 5))
        self.assertEqual(len(result["links"]), 3)
        self.assertEqual([e["level"] for e in self.db.events()], ["danger", "warning"])

    def test_duplicate_urls_and_tracking_do_not_inflate_count(self):
        p = post(1)
        self.db.ingest("test", [p], AT)
        duplicate = {**p, "url": p["url"] + "?utm_source=other#section"}
        self.assertEqual(self.db.ingest("test", [p, duplicate], AT), {"inserted": 0, "duplicates": 2})
        self.assertEqual(self.db.detect(AT)["count"], 1)
        self.assertEqual(canonical_url("https://example.com/x?b=2&a=1"), "https://example.com/x?a=1&b=2")

    def test_time_window_boundary_and_recovery(self):
        self.db.ingest("test", [post(1, 30), post(2, 30.1), post(3, 1), post(4, 1)], AT)
        self.assertEqual(self.db.detect(AT)["count"], 3)
        self.assertEqual(self.db.detect(AT + timedelta(minutes=31))["level"], "normal")
        self.db.ingest("test", [post(i, -31) for i in range(5, 8)], AT + timedelta(minutes=32))
        self.assertEqual(self.db.detect(AT + timedelta(minutes=32))["level"], "warning")
        self.assertEqual(len(self.db.events()), 2)

    def test_events_are_deduplicated_and_links_are_latest(self):
        self.db.ingest("test", [post(i, i + 1) for i in range(6)], AT)
        for _ in range(5):
            result = self.db.detect(AT)
        self.assertEqual(len(self.db.events()), 1)
        self.assertTrue(result["links"][0]["url"].endswith('/0'))

    def test_disabled_sources_excluded_but_history_preserved(self):
        self.db.ingest("test", [post(i) for i in range(5)], AT)
        self.assertEqual(self.db.detect(AT)["count"], 5)
        self.db.save_source({**source(), "enabled": False})
        self.assertEqual(self.db.detect(AT)["count"], 0)
        self.assertEqual(len(self.db.posts()), 5)

    def test_dictionary_updates_reclassify_existing_posts(self):
        self.db.ingest("test", [post(1, title="이동 불능")], AT)
        self.assertEqual(self.db.detect(AT)["count"], 0)
        self.db.put("keywords", [{"name": "이동 불능", "terms": ["이동 불능"], "enabled": True}])
        self.assertEqual(self.db.detect(AT)["count"], 1)

    def test_invalid_import_is_atomic(self):
        with self.assertRaises(ValueError):
            self.db.ingest("test", [post(1), {**post(2), "published_at": "2026-10-17"}], AT)
        self.assertEqual(len(self.db.posts()), 0)
        with self.assertRaises(ValueError):
            self.db.ingest("test", [post(1, -5)], AT)

    def test_settings_and_posts_survive_reopen(self):
        self.db.ingest("test", [post(1)], AT)
        self.db.put("settings", {"window_minutes": 15, "warning": 2, "danger": 4, "running": False})
        reopened = Database(self.db.path)
        self.assertEqual(reopened.get("settings")["window_minutes"], 15)
        self.assertEqual(len(reopened.posts()), 1)


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def test_rss_and_atom_preserve_title_time_url_without_body(self):
        rss = b'''<rss><channel><item><title>Payment crash</title><link>https://example.com/1</link><description>&lt;p&gt;Full text&lt;/p&gt;</description><pubDate>Sat, 17 Oct 2026 12:00:00 +0000</pubDate></item></channel></rss>'''
        result = parse_feed(rss, "https://example.com/feed")[0]
        self.assertEqual(result["body"], "")
        self.assertEqual(result["title"], "Payment crash")
        self.assertEqual(result["published_at"], stamp(AT))
        atom = b'''<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Crash</title><link href="/2"/><content>Details</content><updated>2026-10-17T12:00:00Z</updated></entry></feed>'''
        self.assertEqual(parse_feed(atom, "https://example.com/feed")[0]["url"], "https://example.com/2")

    def test_missing_feed_timestamp_is_error(self):
        with self.assertRaises(ValueError):
            parse_feed(b'<rss><channel><item><title>x</title></item></channel></rss>', "https://example.com")
        self.assertEqual(date_value("2026.10.17 21:00:00"), stamp(AT))

    async def test_dc_adapter_reads_title_and_time_from_listing_without_detail_request(self):
        listing = b'<table><tr class="ub-content"><td class="gall_num">123</td><td class="gall_tit"><a href="/board/view/?id=test&amp;no=123">crash report</a></td><td class="gall_date" title="2026.10.17 21:00:00"></td></tr></table>'
        get = AsyncMock(return_value=(listing, 'https://gall.dcinside.com/board/lists/?id=test'))
        with patch('qa_sentinel.crawlers.safe_get', get):
            result = await crawl({**source(), "kind": "dcinside"}, set())
        self.assertEqual(result[0]["body"], "")
        self.assertEqual(result[0]["title"], "crash report")
        self.assertEqual(result[0]["published_at"], stamp(AT))
        self.assertEqual(get.await_count, 1)

    async def test_javascript_only_page_is_not_reported_as_success(self):
        with patch('qa_sentinel.crawlers.safe_get', AsyncMock(return_value=(b'<div id="app"></div>', 'https://example.com'))):
            with self.assertRaisesRegex(ValueError, '글 목록'):
                await crawl({**source(), "kind": "html", "config": {"item_selector": ".post", "link_selector": "a", "body_selector": "article", "time_selector": "time"}}, set())

    async def test_private_network_fetch_is_rejected(self):
        dns = [(2, 1, 6, '', ('127.0.0.1', 443))]
        with patch('qa_sentinel.crawlers.socket.getaddrinfo', return_value=dns):
            async with httpx.AsyncClient() as client:
                with self.assertRaisesRegex(ValueError, '공개 인터넷'):
                    await safe_get(client, 'https://localhost/data')


class APITests(unittest.TestCase):
    def setUp(self):
        from qa_sentinel import app as module
        self.module = module
        self.temp = tempfile.TemporaryDirectory()
        self.previous_data, self.previous_dbs = module.DATA, module.DATABASES
        module.DATA, module.DATABASES = Path(self.temp.name), {}
        self.client = TestClient(module.app)

    def tearDown(self):
        self.client.close()
        self.module.DATA, self.module.DATABASES = self.previous_data, self.previous_dbs
        self.temp.cleanup()

    def test_ui_and_separated_demo_live_state(self):
        self.assertEqual(self.client.get('/').status_code, 200)
        self.assertEqual(self.client.get('/static/app.js').status_code, 200)
        demo = self.client.get('/api/state').json()
        live = self.client.get('/api/state?mode=live').json()
        self.assertEqual(len(demo['posts']), 132)
        self.assertEqual(live['posts'], [])
        self.assertEqual(live['sources'], [])

    def test_threshold_and_duplicate_keyword_validation(self):
        res = self.client.put('/api/settings', json={"window_minutes": 30, "warning": 5, "danger": 3})
        self.assertEqual(res.status_code, 422)
        res = self.client.put('/api/keywords', json={"keywords": [DEFAULT_KEYWORDS[0], DEFAULT_KEYWORDS[0]]})
        self.assertEqual(res.status_code, 422)

    def test_source_import_detection_and_export(self):
        src = source()
        self.assertEqual(self.client.put('/api/sources?mode=live', json=src).status_code, 200)
        from qa_sentinel.core import now
        posts = [{**post(i), "published_at": stamp(now() - timedelta(seconds=10))} for i in range(5)]
        data = {"source_id": "test", "posts": posts}
        result = self.client.post('/api/import?mode=live', json=data).json()
        self.assertEqual(result['inserted'], 5)
        self.assertEqual(self.client.post('/api/import?mode=live', json=data).json()['duplicates'], 5)
        state = self.client.get('/api/state?mode=live').json()
        self.assertEqual(state['detection']['level'], 'danger')
        self.assertEqual(len(state['events']), 1)
        export = self.client.get('/api/export?mode=live')
        self.assertIn('text/csv', export.headers['content-type'])
        self.assertIn('접속 불가', export.text)

    def test_cross_origin_write_rejected(self):
        res = self.client.put('/api/settings', headers={"Origin": "https://other.example"}, json={})
        self.assertEqual(res.status_code, 403)

    def test_invalid_source_and_invalid_mode(self):
        res = self.client.put('/api/sources?mode=live', json={**source(), "kind": "dcinside"})
        self.assertEqual(res.status_code, 400)
        self.assertEqual(self.client.get('/api/state?mode=typo').status_code, 400)

    def test_failed_collection_records_error_without_fake_posts(self):
        self.client.put('/api/sources?mode=live', json=source())
        with patch('qa_sentinel.app.crawl', AsyncMock(side_effect=ValueError('글 목록을 찾지 못했습니다.'))):
            result = self.client.post('/api/collect?mode=live').json()
        self.assertIn('error', result['results'][0])
        result = self.client.get('/api/state?mode=live').json()
        self.assertEqual(len(result['posts']), 0)
        self.assertTrue(result['sources'][0]['error'])
        self.assertIsNone(result['sources'][0]['last_success'])

    def test_scheduled_collection_waits_fifteen_minutes_between_attempts(self):
        self.client.put('/api/sources?mode=live', json=source())
        mock = AsyncMock(return_value=[])
        with patch('qa_sentinel.app.crawl', mock):
            asyncio.run(self.module.collect(due_only=True))
            asyncio.run(self.module.collect(due_only=True))
        self.assertEqual(mock.await_count, 1)
        db = self.module.database('live')
        from qa_sentinel.core import now
        with db.connect() as conn:
            conn.execute('UPDATE sources SET last_attempt=?', (stamp(now() - timedelta(seconds=901)),))
        with patch('qa_sentinel.app.crawl', mock):
            asyncio.run(self.module.collect(due_only=True))
        self.assertEqual(mock.await_count, 2)

    def test_paused_monitoring_never_fetches(self):
        self.client.put('/api/sources?mode=live', json=source())
        self.client.put('/api/settings?mode=live', json={'window_minutes': 30, 'warning': 3, 'danger': 5, 'running': False})
        mock = AsyncMock()
        with patch('qa_sentinel.app.crawl', mock):
            result = self.client.post('/api/collect?mode=live').json()
        self.assertIn('일시정지', result['message'])
        mock.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
