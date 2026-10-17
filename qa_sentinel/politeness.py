"""Respect community access limits; never solve or bypass access challenges."""
import asyncio
from contextvars import ContextVar
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import random
from urllib.parse import urlsplit
from weakref import WeakKeyDictionary

REQUEST_GUARD = ContextVar("community_request_guard", default=None)
_loop_pacing = WeakKeyDictionary()


def site_domain(host_or_url):
    host = (urlsplit(host_or_url).hostname if "://" in host_or_url else host_or_url).lower()
    if host == "dcinside.com" or host.endswith(".dcinside.com"):
        return "dcinside.com"
    if host == "arca.live" or host.endswith(".arca.live"):
        return "arca.live"
    return host


class SiteBlocked(ValueError):
    def __init__(self, domain, message, retry_after=1800):
        super().__init__(message)
        self.domain = site_domain(domain)
        self.retry_after = max(1800, retry_after)


class CooldownActive(ValueError):
    pass


def retry_after_seconds(value, at=None):
    if not value:
        return 1800
    try:
        seconds = int(value)
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return 1800
            seconds = int((date - (at or datetime.now(timezone.utc))).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 1800
    # Reject unreasonable values rather than overflowing datetime arithmetic.
    return max(1800, min(seconds, 30 * 86400))


def interval_seconds(domain):
    bounds = (10.0, 20.0) if domain == "dcinside.com" else (8.0, 16.0) if domain == "arca.live" else (5.0, 10.0)
    return random.uniform(*bounds)


async def wait_request_slot(domain):
    guard = REQUEST_GUARD.get()
    if guard:
        guard(domain)
    loop = asyncio.get_running_loop()
    pacing = _loop_pacing.setdefault(loop, {})
    state = pacing.setdefault(domain, {"lock": asyncio.Lock(), "last": None})
    async with state["lock"]:
        if state["last"] is not None:
            delay = state["last"] + interval_seconds(domain) - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
        if guard:
            guard(domain)
        state["last"] = loop.time()
