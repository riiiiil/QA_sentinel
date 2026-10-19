"""Collection isolation, offline user testing and real localhost startup checks."""
import asyncio
from datetime import timedelta
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx
from fastapi.testclient import TestClient

from qa_sentinel.core import Database, now, stamp
from qa_sentinel.crawlers import CrawlBatch, crawl, parse_feed
from qa_sentinel.runtime import ProcessLock

ROOT = Path(__file__).resolve().parents[1]


def source(ident="test"):
    return {"id": ident, "name": ident, "kind": "rss", "url": f"https://{ident}.example.com/feed", "enabled": True, "config": {}}


def post(ident=1):
    return {"title": "결제 후 팅김", "body": "수동 테스트", "url": f"https://example.com/posts/{ident}",
            "published_at": stamp(now() - timedelta(seconds=5))}


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from qa_sentinel import app
        self.app = app
        self.temp = tempfile.TemporaryDirectory()
        self.saved = {key: getattr(app, key) for key in ["DATA", "DATABASES", "collection_lock", "collection_tasks", "collection_status", "OFFLINE_TEST"]}
        app.DATA, app.DATABASES, app.OFFLINE_TEST = Path(self.temp.name), {}, False
        app.collection_lock, app.collection_tasks = asyncio.Lock(), set()
        app.collection_status = {"started_at": None, "finished_at": None, "total": 0, "completed": 0, "results": []}
        self.db = app.database("live")
        self.db.begin_session(now() - timedelta(minutes=2))

    async def asyncTearDown(self):
        tasks = list(self.app.collection_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for key, value in self.saved.items():
            setattr(self.app, key, value)
        self.temp.cleanup()

    async def test_slow_channel_does_not_delay_other_channel(self):
        for ident in ("slow", "fast"):
            self.db.save_source(source(ident))
        entered, release, fast = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def fake_crawl(src, known):
            if src["id"] == "slow":
                entered.set()
                await release.wait()
            else:
                fast.set()
            return [post(src["id"])]
        with patch("qa_sentinel.app.crawl", fake_crawl):
            task = asyncio.create_task(self.app.collect())
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.wait_for(fast.wait(), 2)
            self.assertEqual([p["source_id"] for p in self.db.posts()], ["fast"])
            release.set()
            result = await asyncio.wait_for(task, 2)
        self.assertEqual(sum(r["inserted"] for r in result["results"]), 2)

    async def test_parallel_channel_limit_is_respected(self):
        for i in range(7):
            self.db.save_source(source(str(i)))
        active = maximum = 0
        async def fake_crawl(src, known):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            return [post(src["id"])]
        with patch("qa_sentinel.app.crawl", fake_crawl):
            await self.app.collect()
        self.assertEqual(maximum, self.app.CHANNEL_CONCURRENCY)
        self.assertEqual(self.app.collection_status["completed"], 7)

    async def test_timeout_is_limited_to_one_channel(self):
        self.db.save_source(source("slow"))
        self.db.save_source(source("fast"))
        async def fake_crawl(src, known):
            if src["id"] == "slow":
                await asyncio.Event().wait()
            return [post(src["id"])]
        with patch("qa_sentinel.app.crawl", fake_crawl), patch("qa_sentinel.app.CHANNEL_TIMEOUT_SECONDS", 0.02):
            result = await self.app.collect()
        records = {r["source"]: r for r in result["results"]}
        self.assertIn("시간 제한", records["slow"]["error"])
        self.assertEqual(records["fast"]["inserted"], 1)

    async def test_partial_batch_preserves_good_posts_and_diagnostics(self):
        self.db.save_source(source())
        bad = {**post(2), "published_at": "not-a-date"}
        batch = CrawlBatch([post(1), bad], ["목록의 다른 글을 읽을 수 없습니다."])
        with patch("qa_sentinel.app.crawl", AsyncMock(return_value=batch)):
            result = await self.app.collect()
        self.assertEqual(result["results"][0]["inserted"], 1)
        self.assertIn("2개 글", self.db.sources()[0]["warning"])
        self.assertIsNone(self.db.sources()[0]["error"])

    async def test_deleted_channel_does_not_receive_old_inflight_response(self):
        self.db.save_source(source())
        async def fake_crawl(src, known):
            with self.db.connect() as conn:
                conn.execute("DELETE FROM sources WHERE id=?", (src["id"],))
            return [post()]
        with patch("qa_sentinel.app.crawl", fake_crawl):
            result = await self.app.collect()
        self.assertIn("skipped", result["results"][0])
        self.assertEqual(len(self.db.posts()), 0)

    async def test_overlapping_collection_is_not_started_twice(self):
        self.db.save_source(source())
        entered, release = asyncio.Event(), asyncio.Event()
        async def fake_crawl(src, known):
            entered.set()
            await release.wait()
            return []
        with patch("qa_sentinel.app.crawl", fake_crawl):
            first = asyncio.create_task(self.app.collect())
            await asyncio.wait_for(entered.wait(), 2)
            second = await self.app.collect()
            self.assertIn("이미 수집", second["message"])
            release.set()
            await first

    async def test_detection_clock_continues_while_collection_waits(self):
        self.db.save_source(source())
        entered, detected_twice = asyncio.Event(), asyncio.Event()
        loop = asyncio.get_running_loop()
        calls = 0
        def detect():
            nonlocal calls
            calls += 1
            if calls >= 2:
                loop.call_soon_threadsafe(detected_twice.set)
        async def fake_crawl(src, known):
            entered.set()
            await asyncio.Event().wait()
        with patch("qa_sentinel.app.crawl", fake_crawl), patch.object(self.db, "detect", detect), patch("qa_sentinel.app.SCHEDULER_TICK_SECONDS", 0.01):
            scheduler = asyncio.create_task(self.app.scheduler())
            try:
                await asyncio.wait_for(entered.wait(), 2)
                await asyncio.wait_for(detected_twice.wait(), 2)
                self.assertTrue(self.app.collection_lock.locked())
            finally:
                scheduler.cancel()
                await asyncio.gather(scheduler, return_exceptions=True)


class AdapterRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_one_bad_feed_item_does_not_discard_good_item(self):
        xml = b'<rss><channel><item><title>bad</title></item><item><title>good</title><link>https://example.com/1</link><pubDate>2026-10-17T12:00:00Z</pubDate></item></channel></rss>'
        batch = parse_feed(xml, "https://example.com/feed")
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0]["title"], "good")
        self.assertEqual(len(batch.warnings), 1)

    def test_missing_feed_permalink_is_not_replaced_with_feed_url(self):
        xml = b'<rss><channel><item><title>bad</title><pubDate>2026-10-17T12:00:00Z</pubDate></item></channel></rss>'
        with self.assertRaisesRegex(ValueError, "URL"):
            parse_feed(xml, "https://example.com/feed")

    def test_atom_xhtml_body_is_not_collected(self):
        xml = b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>crash</title><link href="https://example.com/1"/><content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml"><p>full text</p></div></content><published>2026-10-17T12:00:00Z</published></entry></feed>'
        self.assertEqual(parse_feed(xml, "https://example.com/feed")[0]["body"], "")

    async def test_bad_listing_date_is_skipped_without_opening_any_article(self):
        cfg = {"item_selector": ".post", "link_selector": "a", "time_selector": "time"}
        listing = b'<div class="post"><a href="/1">first</a></div><div class="post"><a href="/2">second</a><time datetime="2026-10-17T12:00:00Z"></time></div>'
        get = AsyncMock(return_value=(listing, "https://example.com/feed"))
        with patch("qa_sentinel.crawlers.safe_get", get):
            batch = await crawl({**source(), "kind": "html", "config": cfg}, set())
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0]["title"], "second")
        self.assertEqual(batch[0]["body"], "")
        self.assertEqual(len(batch.warnings), 1)
        self.assertEqual(get.await_count, 1)


class UserTestingTests(unittest.TestCase):
    def setUp(self):
        from qa_sentinel import app
        self.app = app
        self.temp = tempfile.TemporaryDirectory()
        self.saved = app.DATA, app.DATABASES, app.OFFLINE_TEST
        app.DATA, app.DATABASES, app.OFFLINE_TEST = Path(self.temp.name), {}, True
        self.client = TestClient(app.app)

    def tearDown(self):
        self.client.close()
        self.app.DATA, self.app.DATABASES, self.app.OFFLINE_TEST = self.saved
        self.temp.cleanup()

    def test_five_manual_scenarios_match_expected_state_and_leave_demo_untouched(self):
        demo_before = self.client.get('/api/state?mode=demo').json()
        for scenario in ("normal", "warning", "danger", "duplicate", "expired"):
            with self.subTest(scenario=scenario):
                result = self.client.post('/api/test/scenario', json={"scenario": scenario})
                self.assertEqual(result.status_code, 200)
                self.assertTrue(result.json()["passed"])
                if scenario == "duplicate":
                    self.assertEqual(result.json()["duplicates"], 6)
        self.assertEqual(self.client.get('/api/state?mode=demo').json()["posts"], demo_before["posts"])

    def test_manual_reset_route_is_forbidden_in_production(self):
        self.app.OFFLINE_TEST = False
        result = self.client.post('/api/test/scenario', json={"scenario": "danger"})
        self.assertEqual(result.status_code, 403)

    def test_offline_mode_blocks_all_external_collection(self):
        self.client.put('/api/sources?mode=live', json=source())
        with patch('qa_sentinel.app.crawl', AsyncMock()) as fetch:
            for background in ("true", "false"):
                result = self.client.post('/api/collect?mode=live&background='+background)
                self.assertIn('차단', result.json()['message'])
            fetch.assert_not_awaited()

    def test_invalid_css_selector_is_rejected_on_save(self):
        result = self.client.put('/api/sources?mode=live', json={**source(), "kind": "html", "config": {"item_selector": "["}})
        self.assertEqual(result.status_code, 422)

    def test_health_and_lifespan_start_and_stop(self):
        with TestClient(self.app.app) as client:
            result = client.get('/api/health').json()
            self.assertEqual(result['status'], 'ok')
            self.assertTrue(result['offline_test'])
            self.assertEqual(result['poll_interval_seconds'], 900)
        self.assertFalse(self.app.collection_tasks)


class LocalStartupTests(unittest.TestCase):
    def test_existing_database_is_upgraded_without_losing_source(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'old.sqlite3'
            with sqlite3.connect(path) as conn:
                conn.execute('CREATE TABLE sources(id TEXT PRIMARY KEY,name TEXT,kind TEXT,url TEXT,enabled INTEGER,config TEXT,last_attempt TEXT,last_success TEXT,error TEXT,collected INTEGER DEFAULT 0)')
                conn.execute("INSERT INTO sources(id,name,kind,url,enabled,config) VALUES ('old','기존 채널','rss','https://example.com/feed',1,'{}')")
            upgraded = Database(path)
            self.assertEqual(upgraded.sources()[0]['name'], '기존 채널')
            self.assertIsNone(upgraded.sources()[0]['warning'])

    def test_simultaneous_detection_creates_only_one_event(self):
        from concurrent.futures import ThreadPoolExecutor
        with tempfile.TemporaryDirectory() as directory:
            db = Database(Path(directory)/'test.sqlite3')
            db.save_source(source())
            db.begin_session(now() - timedelta(minutes=2))
            db.ingest('test', [post(i) for i in range(5)])
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(lambda _: db.detect(), range(20)))
            self.assertTrue(all(result['level'] == 'danger' for result in results))
            self.assertEqual(len(db.events()), 1)

    def test_process_lock_prevents_double_crawlers_and_releases(self):
        with tempfile.TemporaryDirectory() as directory:
            with ProcessLock(directory):
                with self.assertRaisesRegex(RuntimeError, "이미 실행"):
                    with ProcessLock(directory):
                        pass
            with ProcessLock(directory):
                pass

    def test_ready_environment_launches_without_reinstalling_or_network_access(self):
        spec = importlib.util.spec_from_file_location('bootstrap_test', ROOT/'bootstrap.py')
        bootstrap = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bootstrap)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bootstrap.ROOT = root
            (root/'requirements.txt').write_text('example==1\n')
            python = root/'.venv'/('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
            python.parent.mkdir(parents=True)
            python.touch()
            (root/'.venv/.qa-sentinel-requirements').write_text(hashlib.sha256((root/'requirements.txt').read_bytes()).hexdigest())
            with patch.object(sys, 'argv', ['bootstrap.py', '--check']), patch('subprocess.run', return_value=SimpleNamespace(returncode=0)) as run:
                self.assertEqual(bootstrap.main(), 0)
            self.assertFalse(any('pip' in call.args[0] for call in run.call_args_list))

    def test_real_local_server_starts_serves_assets_and_detects_imported_posts(self):
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            environment = {**os.environ, 'PYTHONUNBUFFERED': '1', 'QA_SENTINEL_OFFLINE_TEST': '1'}
            with (Path(directory)/'server.log').open('w+') as output:
                process = subprocess.Popen([sys.executable, str(ROOT/'run.py'), '--port', str(port), '--data-dir', directory], cwd=ROOT,
                                           env=environment, stdout=output, stderr=subprocess.STDOUT)
                try:
                    with httpx.Client(base_url=f'http://127.0.0.1:{port}', trust_env=False, timeout=2) as client:
                        deadline = time.monotonic()+12
                        while True:
                            try:
                                if client.get('/api/health').status_code == 200:
                                    break
                            except httpx.HTTPError:
                                pass
                            if process.poll() is not None or time.monotonic()>deadline:
                                output.seek(0)
                                self.fail('Server failed to start: '+output.read()[-2500:])
                            time.sleep(0.1)
                        self.assertIn('QA Sentinel', client.get('/').text)
                        self.assertEqual(client.get('/static/app.js').status_code, 200)
                        self.assertEqual(client.get('/static/style.css').status_code, 200)
                        self.assertTrue(client.post('/api/test/scenario', json={'scenario':'danger'}).json()['passed'])
                        state = client.get('/api/state?mode=live').json()
                        self.assertEqual(state['detection']['level'], 'danger')
                        self.assertEqual(len(state['detection']['links']), 3)
                        self.assertIn('text/csv', client.get('/api/export?mode=live').headers['content-type'])
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            reopened = Database(Path(directory)/'live.sqlite3')
            self.assertEqual(len(reopened.posts()), 5)
