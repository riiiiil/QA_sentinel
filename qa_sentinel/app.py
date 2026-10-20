from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import logging
import threading
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator

from . import __version__
from .core import Database, DEFAULT_SETTINGS, DEFAULT_KEYWORDS, now, parse_date, stamp, valid_url
from .crawlers import crawl, error_message
from .politeness import REQUEST_GUARD, SiteBlocked, CooldownActive, site_domain
from .notifications import CollectionNotifications

DATA = Path(os.environ.get("QA_SENTINEL_DATA", Path(__file__).resolve().parents[1] / ".sentinel-data"))
DATABASES = {}
DATABASES_LOCK = threading.RLock()
PROGRAM_STARTED_AT = stamp(now())
collection_lock = asyncio.Lock()
collection_tasks = set()
collection_status = {"started_at": None, "finished_at": None, "total": 0, "completed": 0, "results": []}
scheduler_heartbeat = None
CHANNEL_TIMEOUT_SECONDS = 45
COLLECTION_TIMEOUT_SECONDS = 120
CHANNEL_CONCURRENCY = 3
SCHEDULER_TICK_SECONDS = 10
COLLECTION_INTERVAL_SECONDS = 15 * 60
OFFLINE_TEST = os.environ.get("QA_SENTINEL_OFFLINE_TEST") == "1"
SERVER_ID = uuid.uuid4().hex
collection_notifications = CollectionNotifications()


def collection_snapshot(collecting=None):
    last = database("live").get("last_collection") or {}
    current = collection_status if collection_status.get("started_at") else last
    return {"server_id": SERVER_ID, "completed_revision": last.get("revision", 0),
            "collecting": collection_lock.locked() if collecting is None else collecting,
            "collection": {**current, "results": list(current.get("results", []))}}


async def collection_stream(request):
    # Register before reading the snapshot so a completion cannot be missed.
    async with collection_notifications.subscribe() as queue:
        yield "retry: 3000\nevent: collection\ndata: " + json.dumps(collection_snapshot(), ensure_ascii=False) + "\n\n"
        while not await request.is_disconnected():
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=20)
            except TimeoutError:
                yield ": keep-alive\n\n"
            else:
                yield "event: collection\ndata: " + json.dumps(payload, ensure_ascii=False) + "\n\n"



def collection_counts(status):
    results = status.get("results", [])
    return {"succeeded": sum("inserted" in r for r in results),
            "failed": sum("error" in r for r in results),
            "skipped": sum("skipped" in r for r in results)}


def finish_collection(db, status):
    # End marker and revision are committed together, including forced-stop recovery.
    with db.connect() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key='last_collection'").fetchone()
        previous = json.loads(row[0]) if row else {}
        saved = {**status, **collection_counts(status), "results": list(status.get("results", [])), "revision": (previous or {}).get("revision", 0) + 1}
        values = [("last_collection", saved), ("active_collection", None)]
        if status["outcome"] == "completed":
            values.append(("collection_retry_after", None))
        for key, value in values:
            conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value, ensure_ascii=False)))


def database(mode):
    if mode not in {"demo", "live"}:
        raise HTTPException(400, "모드를 확인해 주세요.")
    with DATABASES_LOCK:
        if mode not in DATABASES:
            db = Database(DATA / f"{mode}.sqlite3")
            if mode == "demo":
                db.seed_demo()
                db.begin_session(parse_date(db.get("demo_at")) - timedelta(hours=1))
            else:
                db.begin_session(parse_date(PROGRAM_STARTED_AT))
                active = db.get("active_collection")
                if active:
                    reason = "프로그램 재시작으로 이전 수집이 중단되었습니다. 저장한 글은 보존했습니다."
                    recorded = {r.get("source_id") for r in active.get("results", [])}
                    active.setdefault("results", []).extend({"source_id": source["id"], "source": source["name"], "skipped": reason}
                        for source in active.get("sources", []) if source["id"] not in recorded)
                    active.update(finished_at=PROGRAM_STARTED_AT, outcome="interrupted", stop_reason=reason, completed=active.get("total", 0))
                    finish_collection(db, active)
            DATABASES[mode] = db
        return DATABASES[mode]


def collection_candidates(db, due_only=False):
    active = [s for s in db.sources() if s["enabled"]]
    waiting = [s for s in active if db.site_cooldown(site_domain(s["url"]))]
    retry_after = db.get("collection_retry_after")
    if retry_after and parse_date(retry_after) > now():
        return [], waiting, active
    eligible = [s for s in active if s not in waiting and (not s["last_attempt"]
                or (now() - parse_date(s["last_attempt"])).total_seconds() >= COLLECTION_INTERVAL_SECONDS)]
    return eligible, waiting, active


def waiting_message(waiting, active):
    if waiting:
        return "사이트 접근 제한으로 자동 대기 중입니다. 수집 채널에서 재시도 시각을 확인해 주세요."
    if active:
        return "수집 예정 시각을 기다리고 있습니다. 모든 채널은 수동 수집도 최소 15분 간격입니다."
    return "활성 채널을 먼저 등록해 주세요."


async def collect(due_only=False):
    if OFFLINE_TEST:
        return {"message": "수동 테스트 모드입니다. 외부 커뮤니티 수집이 차단되어 있습니다.", "results": []}
    if collection_lock.locked():
        return {"message": "이미 수집 중입니다.", "results": []}
    async with collection_lock:
        db = database("live")
        if not db.get("settings")["running"]:
            return {"message": "모니터링이 일시정지되어 있습니다.", "results": []}
        eligible, waiting, active = collection_candidates(db, due_only)
        if not eligible:
            return {"message": waiting_message(waiting, active), "results": []}
        collection_status.update(run_id=uuid.uuid4().hex, started_at=stamp(now()), finished_at=None, total=len(eligible), completed=0, results=[], outcome="running")
        collection_status.update(stop_reason=None, succeeded=0, failed=0, skipped=0,
            sources=[{"id": s["id"], "name": s["name"]} for s in eligible],
            retry_after=stamp(now() + timedelta(seconds=COLLECTION_INTERVAL_SECONDS)))
        db.put("collection_retry_after", collection_status["retry_after"])
        db.put("active_collection", dict(collection_status))
        collection_notifications.publish(collection_snapshot(collecting=True))
        semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY)
        site_locks = defaultdict(asyncio.Lock)

        def guard(domain):
            pause = db.site_cooldown(domain)
            if pause:
                raise CooldownActive(f"사이트 접근 제한으로 {pause['until_at']}까지 수집을 대기합니다.")

        def unchanged(source):
            current = next((s for s in db.sources() if s["id"] == source["id"]), None)
            return current and current["enabled"] and all(current[k] == source[k] for k in ("url", "kind", "config"))

        async def collect_source(source):
            result = {"source": source["name"], "source_id": source["id"]}
            domain = site_domain(source["url"])
            guard_token = None
            attempted = False
            try:
                # The round deadline also covers waiting for this site's slot.
                async with site_locks[domain], semaphore:
                    guard_token = REQUEST_GUARD.set(guard)
                    if not db.get("settings")["running"] or not unchanged(source):
                        result["skipped"] = "일시정지 또는 채널 설정 변경으로 건너뛰었습니다."
                        return result
                    guard(domain)
                    with db.connect() as conn:
                        conn.execute("UPDATE sources SET last_attempt=? WHERE id=?", (stamp(now()), source["id"]))
                        known = {r[0] for r in conn.execute("SELECT url FROM posts WHERE source_id=?", (source["id"],))}
                    attempted = True
                    batch = await asyncio.wait_for(crawl(source, known), timeout=CHANNEL_TIMEOUT_SECONDS)
                    # A deleted/edited channel cannot receive results from its old request.
                    with db.lock:
                        if not unchanged(source):
                            result["skipped"] = "수집 중 채널이 변경되어 이전 응답을 저장하지 않았습니다."
                            return result
                        warnings = list(getattr(batch, "warnings", []))
                        # Invalid source data must not discard other correctly fetched posts.
                        valid = []
                        for index, post in enumerate(batch, 1):
                            try:
                                published = parse_date(post["published_at"])
                                if published > now() + timedelta(minutes=1):
                                    raise ValueError("미래 시각입니다.")
                                valid_url(post["url"])
                                if not post["title"].strip():
                                    raise ValueError("제목이 없습니다.")
                                valid.append({**post, "body": ""})
                            except (ValueError, KeyError, TypeError, AttributeError):
                                warnings.append(f"수집 글 {index}번 제외: 제목·URL·작성 시각을 확인해 주세요.")
                        if batch and not valid:
                            raise ValueError("수집된 모든 글의 필수 정보가 잘못되어 저장하지 않았습니다.")
                        stats = db.ingest(source["id"], valid)
                        warning = f"{len(warnings)}개 글 수집 제외. " + " ".join(warnings[:3]) if warnings else None
                        blocked = getattr(batch, "blocked", None)
                        if blocked:
                            pause = db.defer_site(blocked.domain, error_message(blocked), blocked.retry_after)
                            result["cooldown_until"] = pause["until_at"]
                        else:
                            db.site_recovered(domain)
                        with db.connect() as conn:
                            conn.execute("UPDATE sources SET last_success=?,error=?,warning=?,collected=collected+? WHERE id=?",
                                         (stamp(now()), error_message(blocked) if blocked else None, warning, stats["inserted"], source["id"]))
                        result.update(stats)
                        if warning:
                            result["warning"] = warning
                    await asyncio.to_thread(db.detect)
            except asyncio.CancelledError:
                if "inserted" not in result:
                    reason = collection_status.get("stop_reason") or "수집이 중단되었습니다. 저장한 글은 보존했습니다."
                    result["error" if attempted else "skipped"] = reason
                    if attempted and unchanged(source):
                        with db.connect() as conn:
                            conn.execute("UPDATE sources SET error=?,warning=NULL WHERE id=?", (reason, source["id"]))
                raise
            except CooldownActive as exc:
                result["skipped"] = str(exc)
            except SiteBlocked as exc:
                message = error_message(exc)
                pause = db.defer_site(exc.domain, message, exc.retry_after)
                with db.connect() as conn:
                    conn.execute("UPDATE sources SET error=?,warning=NULL WHERE id=?", (message, source["id"]))
                result.update(error=message, cooldown_until=pause["until_at"])
                logging.getLogger("qa_sentinel").warning("Site %s paused until %s", exc.domain, pause["until_at"])
            except Exception as exc:
                message = error_message(exc)
                with db.lock:
                    if unchanged(source):
                        with db.connect() as conn:
                            conn.execute("UPDATE sources SET error=?,warning=NULL WHERE id=?", (message, source["id"]))
                result["error"] = message
                logging.getLogger("qa_sentinel").warning("Channel %s: %s", source["id"], message)
            finally:
                if guard_token is not None:
                    REQUEST_GUARD.reset(guard_token)
                collection_status["completed"] += 1
                collection_status["results"].append(result)
                collection_status.update(collection_counts(collection_status))
                db.put("active_collection", {**collection_status, "results": list(collection_status["results"])})
            return result

        jobs = [asyncio.create_task(collect_source(source)) for source in eligible]
        outcome = "interrupted"
        try:
            _, pending = await asyncio.wait(jobs, timeout=COLLECTION_TIMEOUT_SECONDS)
            if pending:
                outcome = "timed_out"
                collection_status["stop_reason"] = f"전체 수집 {COLLECTION_TIMEOUT_SECONDS:g}초 제한을 초과하여 남은 작업을 중단했습니다. 인터넷 연결과 채널 오류를 확인해 주세요."
                for job in pending:
                    job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
            if outcome != "timed_out":
                counts = collection_counts(collection_status)
                outcome = ("partial" if counts["succeeded"] else "failed") if counts["failed"] else "completed"
        except asyncio.CancelledError:
            collection_status["stop_reason"] = "수집이 중단되었습니다. 저장한 글과 처리 상태는 보존했습니다."
            for job in jobs:
                if not job.done():
                    job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
            raise
        finally:
            collection_status.update(finished_at=stamp(now()), outcome=outcome)
            if outcome == "completed":
                collection_status["retry_after"] = None
            finish_collection(db, collection_status)
            collection_notifications.publish(collection_snapshot(collecting=False))
        counts = collection_counts(collection_status)
        message = f"수집 {'시간 초과' if outcome == 'timed_out' else '종료'} · 완료 {counts['succeeded']}개 · 실패 {counts['failed']}개 · 건너뜀 {counts['skipped']}개"
        return {"message": message, "results": list(collection_status["results"]), "outcome": outcome}


def start_collection(due_only=False):
    if collection_lock.locked() or collection_tasks:
        return False
    task = asyncio.create_task(collect(due_only))
    collection_tasks.add(task)
    def complete(done):
        collection_tasks.discard(done)
        if not done.cancelled() and done.exception():
            logging.getLogger("qa_sentinel").error("Collection job failed: %s", type(done.exception()).__name__)
    task.add_done_callback(complete)
    return True


async def scheduler():
    global scheduler_heartbeat
    while True:
        try:
            scheduler_heartbeat = stamp(now())
            if not OFFLINE_TEST and database("live").get("settings")["running"]:
                start_collection(due_only=True)
            await asyncio.to_thread(database("live").detect)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger("qa_sentinel").exception("Scheduler iteration failed")
        await asyncio.sleep(SCHEDULER_TICK_SECONDS)


@asynccontextmanager
async def lifespan(app):
    database("demo")
    task = asyncio.create_task(scheduler())
    try:
        yield
    finally:
        task.cancel()
        jobs = list(collection_tasks)
        for job in jobs:
            job.cancel()
        await asyncio.gather(task, *jobs, return_exceptions=True)


app = FastAPI(title="QA Sentinel", version=__version__, lifespan=lifespan)
STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.middleware("http")
async def same_origin(request: Request, call_next):
    if request.method in {"POST", "PUT", "DELETE", "PATCH"}:
        origin = request.headers.get("origin")
        if origin and origin != f"{request.url.scheme}://{request.headers.get('host')}":
            return Response("Cross-origin writes are disabled", status_code=403)
        if request.headers.get("sec-fetch-site") == "cross-site":
            return Response("Cross-site writes are disabled", status_code=403)
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
    return response


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health():
    return {"status": "ok", "version": __version__, "server_time": stamp(now()),
            "scheduler_heartbeat": scheduler_heartbeat, "poll_interval_seconds": COLLECTION_INTERVAL_SECONDS,
            "collection_scope": "title_only",
            "collecting": collection_lock.locked() or bool(collection_tasks), "offline_test": OFFLINE_TEST}


@app.get("/api/state")
def state(mode: str = "demo"):
    db = database(mode)
    with db.lock:
        return state_snapshot(db, mode)


def state_snapshot(db, mode):
    at = parse_date(db.get("demo_at")) if mode == "demo" else now()
    detection = db.detect(at)
    sources = db.sources()
    for source in sources:
        pause = db.site_cooldown(site_domain(source["url"]))
        source["cooldown_until"] = pause["until_at"] if pause else None
        source["cooldown_reason"] = pause["reason"] if pause else None
    scope = db.scope()
    posts = db.posts()
    for post in posts:
        post["in_scope"] = parse_date(scope["start_at"]) <= parse_date(post["published_at"]) <= at
    return {"mode": mode, "version": __version__, "offline_test": OFFLINE_TEST, "at": stamp(at), "started_at": scope["session_started_at"], "scope": scope,
            "settings": db.get("settings"), "keywords": db.get("keywords"), "sources": sources,
            "posts": posts, "detection": detection, "events": db.events(),
            **(collection_snapshot() if mode == "live" else {"server_id": SERVER_ID, "completed_revision": 0, "collecting": False, "collection": None})}


@app.get("/api/collection")
async def collection_progress():
    return collection_snapshot()


@app.get("/api/collection/events")
async def collection_events(request: Request):
    return StreamingResponse(collection_stream(request), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class Settings(BaseModel):
    lookback_minutes: int = Field(default=0, ge=0, le=720)
    warning: int = Field(ge=1, le=100000)
    danger: int = Field(ge=2, le=100000)
    running: bool = True

    @model_validator(mode="after")
    def check_thresholds(self):
        if self.warning >= self.danger:
            raise ValueError("위험 기준은 주의 기준보다 커야 합니다.")
        return self


@app.put("/api/settings")
def settings(data: Settings, mode: str = "demo"):
    db = database(mode)
    db.put("settings", data.model_dump())
    return {"ok": True}


class FeedbackStatus(BaseModel):
    status: Literal["active", "non_issue", "in_progress", "resolved"]


@app.patch("/api/posts/{post_id}/status")
def feedback_status(post_id: int, data: FeedbackStatus, mode: str = "demo"):
    db = database(mode)
    with db.lock:
        if not db.set_feedback_status(post_id, data.status):
            raise HTTPException(404, "피드백을 찾을 수 없습니다.")
        db.detect(parse_date(db.get("demo_at")) if mode == "demo" else None)
    return {"ok": True, "status": data.status}


class Keyword(BaseModel):
    name: str = Field(min_length=1, max_length=40)
    terms: list[str] = Field(min_length=1, max_length=30)
    enabled: bool = True

    @field_validator("name")
    @classmethod
    def strip_name(cls, value):
        if not value.strip():
            raise ValueError("키워드 이름이 필요합니다.")
        return value.strip()

    @field_validator("terms")
    @classmethod
    def check_terms(cls, value):
        terms = list(dict.fromkeys(t.strip() for t in value))
        if not terms or any(not t or len(t) > 100 for t in terms):
            raise ValueError("각 검색어는 1~100자로 입력해 주세요.")
        return terms


class Dictionary(BaseModel):
    keywords: list[Keyword] = Field(max_length=200)

    @model_validator(mode="after")
    def unique(self):
        names = [k.name.casefold() for k in self.keywords]
        if len(names) != len(set(names)):
            raise ValueError("이미 등록된 키워드입니다.")
        return self


@app.put("/api/keywords")
def keywords(data: Dictionary, mode: str = "demo"):
    database(mode).put("keywords", [k.model_dump() for k in data.keywords])
    return {"ok": True}


class Source(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex, min_length=1, max_length=40, pattern=r"^[a-zA-Z0-9_-]+$")
    name: str = Field(min_length=1, max_length=60)
    kind: Literal["dcinside", "arcalive", "rss", "html"]
    url: str = Field(max_length=2000)
    enabled: bool = True
    config: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_source(self):
        self.name = self.name.strip()
        if not self.name:
            raise ValueError("채널 이름이 필요합니다.")
        valid_url(self.url)
        if any(len(k) > 80 or len(v) > 300 for k, v in self.config.items()):
            raise ValueError("CSS 선택자가 너무 깁니다.")
        from soupsieve import compile as compile_selector
        for key in ("item_selector", "link_selector", "time_selector"):
            if self.config.get(key):
                try:
                    compile_selector(self.config[key])
                except Exception as exc:
                    raise ValueError(f"CSS 선택자 문법을 확인해 주세요: {key}") from exc
        return self


@app.put("/api/sources")
def sources(data: Source, mode: str = "demo"):
    if mode == "live":
        if data.kind == "dcinside" and urlsplit(data.url).hostname != "gall.dcinside.com":
            raise HTTPException(400, "디시인사이드 갤러리 목록 주소를 입력해 주세요.")
        if data.kind == "arcalive":
            import re
            parsed = urlsplit(data.url)
            if parsed.hostname not in {"arca.live", "www.arca.live"} or not re.fullmatch(r"/b/[A-Za-z0-9_-]+/?", parsed.path):
                raise HTTPException(400, "아카라이브 공개 채널 목록 주소를 입력해 주세요. 예: https://arca.live/b/game")
        if data.kind == "html" and not all(data.config.get(k) for k in ["item_selector", "link_selector", "time_selector"]):
            raise HTTPException(400, "HTML 채널에는 목록 행·링크·목록 행 내부 작성 시각 CSS 선택자 3개가 필요합니다.")
    database(mode).save_source(data.model_dump())
    return {"ok": True}


@app.delete("/api/sources/{source_id}")
def delete_source(source_id: str, mode: str = "demo"):
    db = database(mode)
    with db.connect() as conn:
        conn.execute("DELETE FROM sources WHERE id=?", (source_id,))
    return {"ok": True}


@app.post("/api/collect")
async def collect_now(mode: str = "live", background: bool = False):
    if mode == "demo":
        return {"message": "샘플 데이터입니다. 실제 수집 모드에서 채널을 등록해 주세요.", "results": []}
    database(mode)
    if OFFLINE_TEST:
        return await collect()
    if background:
        if not database(mode).get("settings")["running"]:
            return {"message": "모니터링이 일시정지되어 있습니다.", "results": []}
        eligible, waiting, active = collection_candidates(database(mode))
        if not eligible:
            return {"message": waiting_message(waiting, active), "results": [], "started": False}
        started = start_collection()
        return {"message": "수집을 시작했습니다." if started else "이미 수집 중입니다.", "results": [], "started": started}
    return await collect()


class TestScenario(BaseModel):
    scenario: Literal["normal", "warning", "danger", "duplicate", "expired"]


@app.post("/api/test/scenario")
def test_scenario(data: TestScenario):
    if not OFFLINE_TEST:
        raise HTTPException(403, "수동 테스트 모드에서만 사용할 수 있습니다. run.py --test-mode로 실행하세요.")
    import json
    db = database("live")
    at = now()
    with db.lock:
        with db.connect() as conn:
            for table in ("posts", "events", "sources", "site_cooldowns"):
                conn.execute(f"DELETE FROM {table}")
            for key, value in {"settings": DEFAULT_SETTINGS, "keywords": DEFAULT_KEYWORDS, "last_level": "normal", "started_at": stamp(at)}.items():
                conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value, ensure_ascii=False)))
        db.save_source({"id": "manual-qa", "name": "수동 테스트 채널", "kind": "rss", "url": "https://example.com/manual-qa",
                        "enabled": True, "config": {}})
        count = {"normal": 10, "warning": 3, "danger": 5, "duplicate": 3, "expired": 5}[data.scenario]
        db.begin_session(at - timedelta(minutes=2))
        posts = [{"title": "오늘 레이드 파티 구해요" if data.scenario == "normal" else "레이드 입장 후 팅김",
                  "body": "사용자가 실행한 격리 수동 테스트용 게시글입니다.",
                  "url": f"https://example.com/manual-qa/{i}",
                  "published_at": stamp(at - timedelta(minutes=31 if data.scenario == "expired" else 1))} for i in range(count)]
        stats = db.ingest("manual-qa", posts, at)
        duplicates = db.ingest("manual-qa", posts * 2, at)["duplicates"] if data.scenario == "duplicate" else 0
        observed = db.detect(at)
        expected_level = {"normal": "normal", "warning": "warning", "danger": "danger", "duplicate": "warning", "expired": "normal"}[data.scenario]
        expected_count = 0 if data.scenario in {"normal", "expired"} else count
        return {"scenario": data.scenario, **stats, "duplicates": duplicates, "level": observed["level"], "count": observed["count"],
                "expected": {"level": expected_level, "count": expected_count},
                "passed": observed["level"] == expected_level and observed["count"] == expected_count}


class Post(BaseModel):
    title: str = Field(min_length=1, max_length=1000)
    body: str = Field(default="", max_length=100000)
    published_at: str
    url: str = Field(max_length=2000)


class Import(BaseModel):
    source_id: str
    posts: list[Post] = Field(min_length=1, max_length=1000)


@app.post("/api/import")
def import_posts(data: Import, mode: str = "demo"):
    db = database(mode)
    if data.source_id not in {s["id"] for s in db.sources()}:
        raise HTTPException(400, "먼저 수집 채널을 등록해 주세요.")
    try:
        result = db.ingest(data.source_id, [p.model_dump() for p in data.posts])
    except (ValueError, KeyError) as exc:
        raise HTTPException(400, str(exc)) from exc
    db.detect(parse_date(db.get("demo_at")) if mode == "demo" else None)
    return result


@app.get("/api/export")
def export(mode: str = "demo"):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["작성시간", "채널", "제목", "키워드", "URL", "처리상태"])
    def safe_cell(value):
        value = str(value)
        return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value
    for p in database(mode).posts():
        if p["keywords"]:
            label = {"active": "활성", "non_issue": "논이슈", "in_progress": "진행중", "resolved": "처리완료"}[p["feedback_status"]]
            writer.writerow([safe_cell(v) for v in [p["published_at"], p["source_name"], p["title"], ", ".join(p["keywords"]), p["url"], label]])
    return Response("\ufeff" + output.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="qa-sentinel-feedback.csv"'})
