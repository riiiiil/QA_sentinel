"""Session accumulation, triage exclusion, migration and bounded collection scope."""
from datetime import datetime, timedelta, timezone
import asyncio
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from qa_sentinel.core import Database, now, parse_date, stamp

AT = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)
SOURCE = {"id": "one", "name": "테스트 채널", "kind": "rss", "url": "https://example.com/feed", "enabled": True, "config": {}}


def posts(at, count=5):
    return [{"title": "결제 후 팅김", "published_at": stamp(at), "url": f"https://example.com/post/{i}"} for i in range(count)]


class SessionEngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / 'test.sqlite3')
        self.db.begin_session(AT)
        self.db.save_source(SOURCE)

    def tearDown(self):
        self.temp.cleanup()

    def test_default_scope_is_session_start_and_boundary_is_inclusive(self):
        result = self.db.ingest('one', posts(AT - timedelta(seconds=1), 3), AT)
        self.assertEqual(result, {'inserted': 0, 'duplicates': 0, 'outside_scope': 3})
        self.assertEqual(self.db.ingest('one', posts(AT), AT)['inserted'], 5)
        self.assertEqual(self.db.detect(AT)['count'], 5)

    def test_cumulative_keyword_warning_survives_thirty_minutes_and_twelve_hours(self):
        self.db.ingest('one', posts(AT, 3), AT)
        for elapsed in (31, 720, 2000):
            result = self.db.detect(AT + timedelta(minutes=elapsed))
            self.assertEqual(result['level'], 'warning')
            self.assertEqual(dict((k['name'], k['count']) for k in result['keywords']), {'결제': 3, '팅김': 3})
        self.db.ingest('one', posts(AT + timedelta(hours=1))[3:], AT + timedelta(hours=1))
        self.assertEqual(self.db.detect(AT + timedelta(hours=2))['level'], 'danger')

    def test_twelve_hour_lookback_is_fixed_and_does_not_slide_with_clock(self):
        self.db.put('settings', {**self.db.get('settings'), 'lookback_minutes': 720})
        self.assertEqual(self.db.scope()['start_at'], stamp(AT - timedelta(hours=12)))
        self.assertEqual(self.db.ingest('one', posts(AT - timedelta(hours=12)), AT)['inserted'], 5)
        result = self.db.detect(AT + timedelta(days=2))
        self.assertEqual(result['count'], 5)
        self.assertEqual(result['start_at'], stamp(AT - timedelta(hours=12)))

    def test_all_three_triage_states_exclude_posts_and_reactivation_restores_counts(self):
        self.db.ingest('one', posts(AT), AT)
        ids = [p['id'] for p in self.db.posts()]
        for status in ('non_issue', 'in_progress', 'resolved'):
            with self.subTest(status=status):
                for ident in ids:
                    self.assertTrue(self.db.set_feedback_status(ident, status))
                result = self.db.detect(AT + timedelta(hours=1))
                self.assertEqual((result['level'], result['count'], result['keywords'], result['links']), ('normal', 0, [], []))
                self.assertEqual(len(self.db.posts()), 5)
                for ident in ids:
                    self.db.set_feedback_status(ident, 'active')
                self.assertEqual(self.db.detect(AT)['level'], 'danger')

    def test_duplicate_collection_does_not_reactivate_triaged_post(self):
        self.db.ingest('one', posts(AT, 1), AT)
        ident = self.db.posts()[0]['id']
        self.db.set_feedback_status(ident, 'resolved')
        self.assertEqual(self.db.ingest('one', posts(AT, 1), AT)['duplicates'], 1)
        self.assertEqual(self.db.posts()[0]['feedback_status'], 'resolved')

    def test_new_session_resets_scope_but_retains_records_statuses_and_cooldown(self):
        self.db.ingest('one', posts(AT), AT)
        self.db.set_feedback_status(self.db.posts()[0]['id'], 'non_issue')
        self.db.put('settings', {**self.db.get('settings'), 'lookback_minutes': 720, 'warning': 2, 'danger': 4})
        self.db.defer_site('example.com', '접근 제한', at=AT)
        reopened = Database(self.db.path)
        reopened.begin_session(AT + timedelta(minutes=5))
        self.assertEqual(reopened.scope()['start_at'], stamp(AT + timedelta(minutes=5)))
        self.assertEqual(reopened.get('settings')['lookback_minutes'], 0)
        self.assertEqual(reopened.get('settings')['warning'], 2)
        self.assertEqual(reopened.detect(AT + timedelta(minutes=6))['count'], 0)
        self.assertEqual(len(reopened.posts()), 5)
        self.assertEqual(sum(p['feedback_status'] == 'non_issue' for p in reopened.posts()), 1)
        self.assertIsNotNone(reopened.site_cooldown('example.com', AT + timedelta(minutes=5)))
        reopened.put('settings', {**reopened.get('settings'), 'lookback_minutes': 10})
        self.assertEqual(reopened.detect(AT + timedelta(minutes=6))['count'], 4)

    def test_legacy_database_gains_active_status_without_losing_body_or_settings(self):
        path = Path(self.temp.name) / 'legacy.sqlite3'
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE posts(id INTEGER PRIMARY KEY AUTOINCREMENT,source_id TEXT,title TEXT,body TEXT,published_at TEXT,url TEXT,collected_at TEXT,UNIQUE(source_id,url))')
            conn.execute('INSERT INTO posts(source_id,title,body,published_at,url,collected_at) VALUES (?,?,?,?,?,?)', ('one', '결제 문제', '보존할 본문', stamp(AT), 'https://example.com/old', stamp(AT)))
            conn.execute('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
            conn.execute('INSERT INTO meta VALUES (?,?)', ('settings', json.dumps({'window_minutes': 30, 'warning': 7, 'danger': 9, 'running': False})))
        upgraded = Database(path)
        post = upgraded.posts()[0]
        self.assertEqual((post['title'], post['body'], post['feedback_status']), ('결제 문제', '보존할 본문', 'active'))
        self.assertEqual(upgraded.get('settings'), {'lookback_minutes': 0, 'warning': 7, 'danger': 9, 'running': False})


class SessionAPITests(unittest.TestCase):
    def setUp(self):
        from qa_sentinel import app
        self.app = app
        self.temp = tempfile.TemporaryDirectory()
        self.saved = app.DATA, app.DATABASES, app.OFFLINE_TEST
        app.DATA, app.DATABASES, app.OFFLINE_TEST = Path(self.temp.name), {}, True
        self.client = TestClient(app.app)
        self.db = app.database('live')
        self.db.save_source(SOURCE)

    def tearDown(self):
        self.client.close()
        self.app.DATA, self.app.DATABASES, self.app.OFFLINE_TEST = self.saved
        self.temp.cleanup()

    def test_api_validates_twelve_hour_limit_and_status_values(self):
        settings = {'warning': 3, 'danger': 5, 'running': False}
        for value in (-1, 721):
            self.assertEqual(self.client.put('/api/settings?mode=live', json={**settings, 'lookback_minutes': value}).status_code, 422)
        self.assertEqual(self.client.put('/api/settings?mode=live', json={**settings, 'lookback_minutes': 720}).status_code, 200)
        self.assertEqual(self.client.patch('/api/posts/999/status?mode=live', json={'status': 'resolved'}).status_code, 404)
        self.assertEqual(self.client.patch('/api/posts/999/status?mode=live', json={'status': 'hidden'}).status_code, 422)

    def test_status_change_refreshes_detection_and_scope_flags_explain_history(self):
        at = now()
        self.db.begin_session(at - timedelta(minutes=1))
        self.db.ingest('one', posts(at), at)
        state = self.client.get('/api/state?mode=live').json()
        self.assertEqual(state['detection']['level'], 'danger')
        for post in state['posts'][:3]:
            self.assertEqual(self.client.patch(f"/api/posts/{post['id']}/status?mode=live", json={'status': 'in_progress'}).status_code, 200)
        state = self.client.get('/api/state?mode=live').json()
        self.assertEqual(state['detection']['count'], 2)
        self.assertEqual(len(state['posts']), 5)
        self.db.begin_session(at + timedelta(minutes=1))
        state = self.client.get('/api/state?mode=live').json()
        self.assertFalse(any(p['in_scope'] for p in state['posts']))
        self.assertEqual(state['detection']['count'], 0)

    def test_import_reports_old_posts_separately_from_duplicates(self):
        at = now()
        self.db.begin_session(at)
        result = self.client.post('/api/import?mode=live', json={'source_id': 'one', 'posts': posts(at - timedelta(hours=1))}).json()
        self.assertEqual(result, {'inserted': 0, 'duplicates': 0, 'outside_scope': 5})

    def test_settings_changes_never_trigger_community_requests(self):
        self.app.OFFLINE_TEST = False
        with patch('qa_sentinel.app.crawl', AsyncMock()) as crawl:
            self.client.put('/api/settings?mode=live', json={'lookback_minutes': 720, 'warning': 3, 'danger': 5, 'running': True})
            self.client.get('/api/state?mode=live')
        crawl.assert_not_awaited()

    def test_one_collection_filters_old_titles_and_lookback_change_keeps_fifteen_minute_gap(self):
        self.app.OFFLINE_TEST = False
        at = now()
        self.db.begin_session(at - timedelta(minutes=1))
        batch = [posts(at, 1)[0], {**posts(at - timedelta(hours=2), 1)[0], 'url': 'https://example.com/older'}]
        with patch('qa_sentinel.app.crawl', AsyncMock(return_value=batch)) as crawl:
            result = asyncio.run(self.app.collect())
            self.assertEqual(result['results'][0]['inserted'], 1)
            self.assertEqual(result['results'][0]['outside_scope'], 1)
            previous_attempt = self.db.sources()[0]['last_attempt']
            self.client.put('/api/settings?mode=live', json={'lookback_minutes': 720, 'warning': 3, 'danger': 5, 'running': True})
            asyncio.run(self.app.collect())
        self.assertEqual(crawl.await_count, 1)
        self.assertEqual(self.db.sources()[0]['last_attempt'], previous_attempt)
