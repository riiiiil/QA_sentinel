from __future__ import annotations

import asyncio
import ipaddress
import socket
import re
import random
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from . import __version__
from .core import canonical_url, parse_date, stamp, valid_url
from .politeness import SiteBlocked, CooldownActive, REQUEST_GUARD, retry_after_seconds, site_domain, wait_request_slot

KST = ZoneInfo("Asia/Seoul")
DNS_TIMEOUT_SECONDS = 5
USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
)


def choose_user_agent():
    # Pick once per collection, retain the application identity on every profile.
    return random.choice(USER_AGENTS) + f" QASentinel/{__version__} (community title monitoring)"


class CrawlBatch(list):
    """Valid posts plus visible diagnostics for items that could not be read."""
    def __init__(self, posts=(), warnings=(), blocked=None):
        super().__init__(posts)
        self.warnings = list(warnings)
        self.blocked = blocked


def validate_community_response(raw, domain):
    soup = BeautifulSoup(raw, "html.parser")
    content = soup.body if soup.body is not None else soup
    if not content.get_text(" ", strip=True):
        raise SiteBlocked(domain, "빈 화면 응답(차단 의심)을 받았습니다. 소프트밴으로 단정할 수는 없으며 사이트 수집을 자동 대기합니다.")
    title = soup.title.get_text(" ", strip=True).casefold() if soup.title else ""
    has_content = soup.select_one(".gall_list, .write_div, .article-list, .article-content")
    challenge = soup.select_one("#challenge-form, #challenge-running, #cf-challenge-running, .cf-browser-verification")
    if has_content is None and (challenge is not None or title in {"just a moment...", "access denied", "attention required! | cloudflare"}):
        raise SiteBlocked(domain, "사이트의 자동 접근 확인/차단 화면을 받았습니다. 확인 절차를 우회하지 않고 수집을 자동 대기합니다.")


def error_message(exc):
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        reasons = {401: "로그인이 필요합니다", 403: "접근이 차단되었습니다", 404: "주소를 찾을 수 없습니다",
                   429: "요청 제한에 도달했습니다. 다음 수집에서 다시 시도합니다"}
        return f"HTTP {code}: {reasons.get(code, '서버가 오류를 반환했습니다')}."
    if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
        return "수집 시간 제한을 초과했습니다. 다른 채널의 수집은 계속됩니다."
    if isinstance(exc, httpx.NetworkError):
        return "네트워크 연결에 실패했습니다. 인터넷 연결을 확인한 뒤 다음 수집 시각에 다시 시도해 주세요."
    if isinstance(exc, ValueError):
        return str(exc)[:250]
    if isinstance(exc, socket.gaierror):
        return "서버 주소를 찾을 수 없습니다. URL과 인터넷 연결을 확인해 주세요."
    return f"수집 실패 ({type(exc).__name__}). 주소와 접근 권한을 확인해 주세요."


async def safe_get(client, url):
    """Public HTTPS only, including every redirect; bounded response size and time."""
    for _ in range(4):
        valid_url(url)
        host = urlsplit(url).hostname
        domain = site_domain(host)
        guard = REQUEST_GUARD.get()
        if guard:
            guard(domain)
        addresses = await asyncio.wait_for(asyncio.to_thread(socket.getaddrinfo, host, 443, type=socket.SOCK_STREAM), timeout=DNS_TIMEOUT_SECONDS)
        if not addresses or any(not ipaddress.ip_address(row[4][0]).is_global for row in addresses):
            raise ValueError("공개 인터넷 주소만 수집할 수 있습니다.")
        await wait_request_slot(domain)
        async with client.stream("GET", url) as response:
            if response.is_redirect:
                location = response.headers.get("location")
                if not location:
                    raise ValueError("서버의 리다이렉트 응답에 이동 주소가 없습니다.")
                url = urljoin(url, location)
                continue
            if response.status_code in {403, 429, 503}:
                raise SiteBlocked(domain, f"HTTP {response.status_code}: 사이트 접근 제한 또는 서버 응답 오류로 자동 대기합니다.",
                                  retry_after_seconds(response.headers.get("retry-after")))
            response.raise_for_status()
            chunks, size = [], 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > 4_000_000:
                    raise ValueError("수집 응답이 4MB를 초과했습니다.")
                chunks.append(chunk)
            raw = b"".join(chunks)
            validate_community_response(raw, domain)
            return raw, str(response.url)
    raise ValueError("리다이렉트가 너무 많습니다.")


def clean(value):
    return BeautifulSoup(value or "", "html.parser").get_text(" ", strip=True)


def date_value(value):
    value = (value or "").strip()
    try:
        return stamp(parse_date(value))
    except ValueError:
        pass
    try:
        d = parsedate_to_datetime(value)
        if d.tzinfo is None:
            raise ValueError()
        return stamp(d)
    except (ValueError, TypeError):
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S", "%Y.%m.%d. %H:%M", "%Y.%m.%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return stamp(datetime.strptime(value, fmt).replace(tzinfo=KST))
        except ValueError:
            continue
    raise ValueError("게시글 작성 시각을 해석할 수 없습니다. 날짜 선택자를 확인해 주세요.")


def feed_post(title, url, dt, base):
    title = clean(title)
    if not title:
        raise ValueError("게시글 제목이 없습니다.")
    if not url or not url.strip():
        raise ValueError("게시글 원문 URL이 없습니다.")
    if not dt:
        raise ValueError("작성 시각이 없습니다. 수집 시각으로 대체하지 않습니다.")
    return {"title": title, "body": "", "url": valid_url(urljoin(base, url)), "published_at": date_value(dt)}


def atom_text(item, name, namespaces):
    node = item.find(name, namespaces)
    if node is None:
        return ""
    return (node.text or "") + "".join(ET.tostring(child, encoding="unicode") for child in node)


def parse_feed(raw, base):
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("유효한 RSS/Atom 문서가 아닙니다. 피드 주소를 확인해 주세요.") from exc
    posts, warnings = [], []
    atom = {"a": "http://www.w3.org/2005/Atom"}
    items = root.findall("./channel/item")
    if items:
        for index, item in enumerate(items[:50], 1):
            try:
                dt = item.findtext("pubDate") or item.findtext("{http://purl.org/dc/elements/1.1/}date")
                posts.append(feed_post(item.findtext("title", ""), item.findtext("link", ""), dt, base))
            except ValueError as exc:
                warnings.append(f"피드 {index}번 글 제외: {error_message(exc)}")
    else:
        for index, item in enumerate(root.findall("a:entry", atom)[:50], 1):
            try:
                link = next((x for x in item.findall("a:link", atom) if x.get("rel", "alternate") == "alternate"), None)
                dt = item.findtext("a:published", namespaces=atom) or item.findtext("a:updated", namespaces=atom)
                posts.append(feed_post(atom_text(item, "a:title", atom),
                    link.get("href", "") if link is not None else "", dt, base))
            except ValueError as exc:
                warnings.append(f"피드 {index}번 글 제외: {error_message(exc)}")
    if not posts:
        raise ValueError("피드에서 저장 가능한 게시글을 찾지 못했습니다. " + (warnings[0] if warnings else "RSS/Atom 주소를 확인해 주세요."))
    return CrawlBatch(posts, warnings)


async def crawl(source, known_urls):
    """One listing/feed fetch; never open article pages to read body or dates."""
    headers = {"User-Agent": choose_user_agent(), "Accept-Language": "ko-KR,ko;q=0.9"}
    async with httpx.AsyncClient(timeout=20, follow_redirects=False, headers=headers) as client:
        raw, base = await safe_get(client, source["url"])
        if source["kind"] == "rss":
            return parse_feed(raw, base)
        soup = BeautifulSoup(raw, "html.parser")
        is_dc = source["kind"] == "dcinside"
        is_arca = source["kind"] == "arcalive"
        cfg = source["config"]
        item_sel = "tr.ub-content" if is_dc else ".article-list a.vrow, .list-table a.vrow" if is_arca else cfg.get("item_selector", "")
        link_sel = ".gall_tit a:not(.reply_numbox)" if is_dc else "" if is_arca else cfg.get("link_selector", "")
        time_sel = ".gall_date" if is_dc else "time[datetime], time[title], .col-time[datetime], .col-time[title]" if is_arca else cfg.get("time_selector", "")
        if not all((item_sel, time_sel)) or (not is_arca and not link_sel):
            raise ValueError("HTML 채널의 목록 행·링크·목록 행 내부 작성 시각 CSS 선택자가 필요합니다.")
        rows = soup.select(item_sel)
        if not rows:
            raise ValueError("글 목록을 찾지 못했습니다. 로그인/자바스크립트 전용 페이지는 공개 RSS 또는 서버 렌더링 주소가 필요합니다.")
        posts, warnings, seen = [], [], set(known_urls)
        attempted, linked = 0, False
        for index, row in enumerate(rows[:100], 1):
            anchor = row if is_arca else row.select_one(link_sel)
            if not anchor or not anchor.get("href"):
                continue
            linked = True
            if is_dc:
                num = row.select_one(".gall_num")
                if num is None or not num.get_text(strip=True).isdigit():
                    continue
            url = urljoin(base, anchor["href"])
            if urlsplit(url).hostname != urlsplit(base).hostname:
                continue
            if is_arca:
                board = urlsplit(base).path.rstrip("/")
                if not re.fullmatch(re.escape(board) + r"/\d+/?", urlsplit(url).path):
                    continue
                num = row.select_one(".col-id")
                if num is not None and not num.get_text(strip=True).isdigit():
                    continue
            try:
                key = canonical_url(url)
            except ValueError:
                warnings.append(f"목록 {index}번 글 제외: HTTPS 원문 주소가 아닙니다.")
                continue
            if key in seen:
                continue
            attempted += 1
            try:
                dt = row.select_one(time_sel)
                if dt is None:
                    raise ValueError("목록에서 작성 시각을 찾지 못했습니다. 상세 페이지를 요청하지 않습니다. 날짜 선택자를 확인해 주세요.")
                published = date_value(dt.get("datetime") or dt.get("title") or dt.get_text(strip=True))
                title_node = row.select_one(".col-title .title, .col-title .text, .title") if is_arca else None
                title = (title_node if title_node is not None else anchor).get_text(" ", strip=True)
                if not title:
                    raise ValueError("게시글 제목이 없습니다.")
                posts.append({"title": title, "body": "", "published_at": published, "url": valid_url(url)})
                seen.add(key)
            except (ValueError, TypeError) as exc:
                warnings.append(f"목록 {index}번 글 제외: {error_message(exc)}")
        if attempted and not posts:
            raise ValueError(f"목록의 신규 글 {attempted}개를 모두 수집하지 못했습니다. " + (warnings[0] if warnings else "선택자를 확인해 주세요."))
        if not linked:
            raise ValueError("게시글 링크를 찾지 못했습니다. 링크 선택자를 확인해 주세요.")
        return CrawlBatch(posts, warnings)
