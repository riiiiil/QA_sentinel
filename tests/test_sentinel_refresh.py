"""Collection notifications, durable collection times and readable color contrast."""
import asyncio
from datetime import timedelta
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from qa_sentinel.core import now
from qa_sentinel.notifications import CollectionNotifications
from qa_sentinel.politeness import SiteBlocked


class RefreshTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from qa_sentinel import app
        self.app=app
        self.temp=tempfile.TemporaryDirectory()
        names=('DATA','DATABASES','OFFLINE_TEST','collection_lock','collection_tasks','collection_status','collection_notifications')
        self.saved={key:getattr(app,key) for key in names}
        app.DATA,app.DATABASES,app.OFFLINE_TEST=Path(self.temp.name),{},False
        app.collection_lock,app.collection_tasks=asyncio.Lock(),set()
        app.collection_status={'started_at':None,'finished_at':None,'total':0,'completed':0,'results':[]}
        app.collection_notifications=CollectionNotifications()
        self.db=app.database('live')
        self.db.save_source({'id':'one','name':'one','kind':'rss','url':'https://example.com/feed','enabled':True,'config':{}})

    async def asyncTearDown(self):
        for key,value in self.saved.items():setattr(self.app,key,value)
        self.temp.cleanup()

    async def test_scheduled_start_and_finish_notifications_include_durable_revision(self):
        entered,release=asyncio.Event(),asyncio.Event()
        async def fetch(*args):entered.set();await release.wait();return []
        async with self.app.collection_notifications.subscribe() as queue:
            with patch('qa_sentinel.app.crawl',fetch):
                task=asyncio.create_task(self.app.collect(due_only=True))
                await asyncio.wait_for(entered.wait(),2)
                initial=await queue.get()
                self.assertTrue(initial['collecting'])
                self.assertEqual(initial['completed_revision'],0)
                release.set();await task
                completed=await queue.get()
                self.assertFalse(completed['collecting'])
                self.assertEqual(completed['completed_revision'],1)
                self.assertEqual(completed['collection']['completed'],1)
                self.assertTrue(completed['collection']['finished_at'])
        self.assertEqual(self.db.get('last_collection')['revision'],1)

    async def test_no_eligible_channels_do_not_publish_or_change_collection_time(self):
        with patch('qa_sentinel.app.crawl',AsyncMock(return_value=[])):
            await self.app.collect()
        last=self.db.get('last_collection')
        async with self.app.collection_notifications.subscribe() as queue:
            with patch('qa_sentinel.app.crawl',AsyncMock()) as fetch:
                await self.app.collect(due_only=True)
            fetch.assert_not_awaited();self.assertTrue(queue.empty())
        self.assertEqual(self.db.get('last_collection'),last)

    async def test_restart_and_state_reads_preserve_time_of_failed_collection(self):
        with patch('qa_sentinel.app.crawl',AsyncMock(side_effect=SiteBlocked('example.com','blocked'))):
            await self.app.collect()
        last=self.db.get('last_collection')
        self.assertIn('error',last['results'][0])
        self.app.collection_status={'started_at':None,'finished_at':None,'results':[]}
        self.app.DATABASES={}
        with patch('qa_sentinel.app.now',return_value=now()+timedelta(minutes=10)):
            state=self.app.state('live')
        self.assertEqual(state['collection']['finished_at'],last['finished_at'])
        self.assertEqual(state['completed_revision'],1)
        self.assertNotEqual(state['at'],last['finished_at'])

    async def test_cancellation_publishes_end_and_records_interruption(self):
        entered=asyncio.Event()
        async def fetch(*args):entered.set();await asyncio.Event().wait()
        async with self.app.collection_notifications.subscribe() as queue:
            with patch('qa_sentinel.app.crawl',fetch):
                task=asyncio.create_task(self.app.collect())
                await asyncio.wait_for(entered.wait(),2);await queue.get()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
                payload=await queue.get()
        self.assertFalse(payload['collecting'])
        self.assertEqual(payload['collection']['outcome'],'interrupted')
        self.assertFalse(self.app.collection_lock.locked())

    async def test_stream_subscribes_before_snapshot_and_cleans_up_on_close(self):
        request=type('Request',(),{'is_disconnected':AsyncMock(return_value=False)})()
        stream=self.app.collection_stream(request)
        initial=await anext(stream)
        self.assertIn('event: collection',initial)
        data=json.loads(initial.split('data: ',1)[1])
        self.assertNotIn('posts',data);self.assertNotIn('settings',data)
        self.app.collection_notifications.publish({**data,'completed_revision':3})
        self.assertIn('"completed_revision": 3',await anext(stream))
        with patch('qa_sentinel.app.asyncio.wait_for',side_effect=TimeoutError),patch('qa_sentinel.app.database') as db:
            # Closing the coroutine passed to the mocked wait avoids leaked awaits.
            async def timeout(awaitable,**kwargs):awaitable.close();raise TimeoutError
            with patch('qa_sentinel.app.asyncio.wait_for',timeout):self.assertEqual(await anext(stream),': keep-alive\n\n')
            db.assert_not_called()
        await stream.aclose()
        self.assertTrue(all(not clients for clients in self.app.collection_notifications.listeners.values()))

    async def test_slow_subscriber_keeps_latest_snapshot_and_broadcasts_to_all(self):
        async with self.app.collection_notifications.subscribe() as one,self.app.collection_notifications.subscribe() as two:
            for revision in range(100):self.app.collection_notifications.publish({'completed_revision':revision})
            self.assertEqual(one.qsize(),1);self.assertEqual(two.qsize(),1)
            self.assertEqual((await one.get())['completed_revision'],99)
            self.assertEqual((await two.get())['completed_revision'],99)


class ReadabilityTests(unittest.TestCase):
    def test_text_palette_has_aa_contrast_and_small_text_has_a_readable_floor(self):
        def luminance(color):
            rgb=[int(color[i:i+2],16)/255 for i in (1,3,5)]
            channels=[x/12.92 if x<=0.04045 else ((x+0.055)/1.055)**2.4 for x in rgb]
            return sum(a*b for a,b in zip(channels,(0.2126,0.7152,0.0722)))
        for text,background in [('#52647b','#ffffff'),('#172b4d','#ffffff'),('#ffffff','#1d4ed8'),('#c6d5e8','#132d46'),('#166534','#e4f4e9'),('#9f2117','#fee4df'),('#754407','#fff0c0'),('#1e40af','#edf3ff')]:
            values=sorted((luminance(text),luminance(background)))
            self.assertGreaterEqual((values[1]+0.05)/(values[0]+0.05),4.5,(text,background))
        css=(Path(__file__).parents[1]/'qa_sentinel/static/style.css').read_text()
        sizes=[int(value) for value in re.findall(r'font-size:(\d+)px',css)]
        self.assertGreaterEqual(min(value for value in sizes if value),12)
        self.assertIn('body{font-size:16px',css)


if __name__=='__main__':unittest.main()
