"""Bounded, event-loop-local subscriptions for collection start/finish signals."""
import asyncio
from contextlib import asynccontextmanager
from weakref import WeakKeyDictionary


class CollectionNotifications:
    def __init__(self):
        self.listeners = WeakKeyDictionary()

    @asynccontextmanager
    async def subscribe(self):
        loop = asyncio.get_running_loop()
        listeners = self.listeners.setdefault(loop, set())
        queue = asyncio.Queue(maxsize=1)
        listeners.add(queue)
        try:
            yield queue
        finally:
            listeners.discard(queue)

    def publish(self, value):
        # All publishers run inside the collector's loop, never API worker threads.
        for queue in self.listeners.get(asyncio.get_running_loop(), ()):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(value)
