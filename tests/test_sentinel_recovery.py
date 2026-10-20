"""Bounded reconnect failures, request pacing and retained feedback history."""
import asyncio
from datetime import timedelta
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from qa_sentinel.core import Database, now, stamp
from qa_sentinel.crawlers import safe_get


def source(ident, host='example.com'):
    return {'id':ident,'name':ident,'kind':'rss','url':f'https://{host}/feed?board={ident}','enabled':True,'config':{}}


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from qa_sentinel import app
        self.app=app;self.temp=tempfile.TemporaryDirectory()
        names=('DATA','DATABASES','OFFLINE_TEST','collection_lock','collection_tasks','collection_status')
        self.saved={name:getattr(app,name) for name in names}
        app.DATA=Path(self.temp.name);app.DATABASES={};app.OFFLINE_TEST=False
        app.collection_lock=asyncio.Lock();app.collection_tasks=set()
        app.collection_status={'started_at':None,'finished_at':None,'total':0,'completed':0,'results':[]}
        self.db=app.database('live');self.db.begin_session(now()-timedelta(minutes=5))

    async def asyncTearDown(self):
        jobs=list(self.app.collection_tasks)
        for job in jobs:job.cancel()
        await asyncio.gather(*jobs,return_exceptions=True)
        for name,value in self.saved.items():setattr(self.app,name,value)
        self.temp.cleanup()

    def seed(self):
        self.db.save_source(source('0'))
        self.db.ingest('0',[{'title':f'결제 오류 {i}','published_at':stamp(now()-timedelta(minutes=1)),'url':f'https://example.com/{i}'} for i in range(3)])
        ident=self.db.posts()[0]['id'];self.db.set_feedback_status(ident,'resolved')

    async def test_round_deadline_covers_queue_and_preserves_posts_and_triage(self):
        self.seed()
        for i in range(1,4):self.db.save_source(source(str(i)))
        attempts=[]
        async def blocked(src,known):attempts.append(src['id']);await asyncio.Event().wait()
        before=self.db.posts();started=time.monotonic()
        async with self.app.collection_notifications.subscribe() as notifications:
            with patch('qa_sentinel.app.crawl',blocked),patch('qa_sentinel.app.CHANNEL_TIMEOUT_SECONDS',.08),patch('qa_sentinel.app.COLLECTION_TIMEOUT_SECONDS',.12):
                result=await asyncio.wait_for(self.app.collect(),1)
            last=await notifications.get()
        self.assertLess(time.monotonic()-started,1)
        self.assertEqual(result['outcome'],'timed_out')
        self.assertEqual(attempts,['0','1'])
        self.assertEqual(len(result['results']),4)
        self.assertFalse(last['collecting']);self.assertFalse(self.app.collection_lock.locked())
        self.assertEqual(last['collection']['failed'],2);self.assertEqual(last['collection']['skipped'],2)
        self.assertEqual(self.db.posts(),before)
        self.assertIsNone(self.db.get('active_collection'))
        with patch('qa_sentinel.app.crawl',AsyncMock()) as fetch:
            await self.app.collect();await self.app.collect(due_only=True)
        fetch.assert_not_awaited()
        self.app.DATABASES={};self.app.collection_status={'started_at':None}
        reopened=self.app.database('live')
        with patch('qa_sentinel.app.crawl',AsyncMock()) as fetch:await self.app.collect()
        fetch.assert_not_awaited();self.assertEqual(len(reopened.posts()),3)

    async def test_failed_channel_does_not_erase_successful_other_site(self):
        self.db.save_source(source('slow'));self.db.save_source(source('fast','other.example.com'))
        async def fetch(src,known):
            if src['id']=='slow':await asyncio.Event().wait()
            return [{'title':'결제 새 글','published_at':stamp(now()),'url':'https://other.example.com/1'}]
        with patch('qa_sentinel.app.crawl',fetch),patch('qa_sentinel.app.CHANNEL_TIMEOUT_SECONDS',.03):
            result=await self.app.collect()
        self.assertEqual(result['outcome'],'partial')
        self.assertEqual([p['source_id'] for p in self.db.posts()],['fast'])
        last=self.db.get('last_collection')
        self.assertEqual((last['succeeded'],last['failed'],last['skipped']),(1,1,0))

    async def test_background_start_ack_is_fast_while_collection_is_blocked(self):
        self.db.save_source(source('slow'));entered=asyncio.Event()
        async def fetch(*args):entered.set();await asyncio.Event().wait()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app.app),base_url='http://local') as client:
            with patch('qa_sentinel.app.crawl',fetch):
                response=await asyncio.wait_for(client.post('/api/collect?mode=live&background=true'),1)
                self.assertTrue(response.json()['started'])
                await asyncio.wait_for(entered.wait(),1)
                progress=await asyncio.wait_for(client.get('/api/collection'),1)
                self.assertTrue(progress.json()['collecting'])
                for job in list(self.app.collection_tasks):job.cancel()
                await asyncio.gather(*list(self.app.collection_tasks),return_exceptions=True)

    async def test_forced_restart_keeps_history_but_starts_new_scope(self):
        self.seed();self.db.save_source(source('1'))
        self.db.put('collection_retry_after',stamp(now()+timedelta(minutes=15)))
        self.db.put('active_collection',{'run_id':'unfinished','started_at':stamp(now()),'total':2,'completed':1,'outcome':'running',
            'sources':[{'id':'0','name':'0'},{'id':'1','name':'1'}],
            'results':[{'source_id':'0','source':'0','inserted':3,'duplicates':0}]})
        self.app.DATABASES={}
        state=self.app.state('live')
        self.assertEqual(len(state['posts']),3)
        self.assertEqual(sum(p['feedback_status']=='resolved' for p in state['posts']),1)
        self.assertEqual(state['settings']['lookback_minutes'],0)
        self.assertEqual(state['detection']['count'],0)
        self.assertFalse(any(p['in_scope'] for p in state['posts']))
        self.assertFalse(state['collecting']);self.assertEqual(state['collection']['outcome'],'interrupted')
        self.assertEqual(state['collection']['succeeded'],1);self.assertEqual(state['collection']['skipped'],1)
        revision=state['completed_revision'];self.app.DATABASES={}
        self.assertEqual(self.app.state('live')['completed_revision'],revision)

    async def test_dns_timeout_never_opens_http_stream_and_next_resolution_can_recover(self):
        async def blocked(*args,**kwargs):await asyncio.Event().wait()
        with patch('qa_sentinel.crawlers.asyncio.to_thread',blocked),patch('qa_sentinel.crawlers.DNS_TIMEOUT_SECONDS',.02),patch('qa_sentinel.crawlers.wait_request_slot',AsyncMock()) as pacing:
            with self.assertRaises(TimeoutError):await asyncio.wait_for(safe_get(None,'https://example.com/feed'),1)
        pacing.assert_not_awaited()
        transport=httpx.MockTransport(lambda request:httpx.Response(200,content=b'<rss><channel><title>available</title></channel></rss>'))
        dns=[(2,1,6,'',('8.8.8.8',443))]
        with patch('qa_sentinel.crawlers.socket.getaddrinfo',return_value=dns),patch('qa_sentinel.crawlers.wait_request_slot',AsyncMock()):
            async with httpx.AsyncClient(transport=transport,trust_env=False) as client:
                raw,_=await safe_get(client,'https://example.com/feed')
        self.assertIn(b'available',raw)

    async def test_success_clears_reservation_but_keeps_channel_minimum_interval(self):
        self.db.save_source(source('0'))
        with patch('qa_sentinel.app.crawl',AsyncMock(return_value=[])) as fetch:
            await self.app.collect();await self.app.collect()
        self.assertEqual(fetch.await_count,1)
        self.assertIsNone(self.db.get('collection_retry_after'))


if __name__=='__main__':unittest.main()
