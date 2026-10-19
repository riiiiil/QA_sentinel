from __future__ import annotations

import json
import sqlite3
import threading
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

UTC = timezone.utc
DEFAULT_KEYWORDS = [
    {"name": "결제", "terms": ["결제", "과금", "구매 오류"], "enabled": True},
    {"name": "증발", "terms": ["증발", "아이템 사라짐", "재화 소실"], "enabled": True},
    {"name": "무한 로딩", "terms": ["무한 로딩", "무한로딩"], "enabled": True},
    {"name": "접속 불가", "terms": ["접속 불가", "접속불가", "로그인 안됨"], "enabled": True},
    {"name": "팅김", "terms": ["팅김", "튕김", "강제 종료", "crash"], "enabled": True},
]
DEFAULT_SETTINGS = {"lookback_minutes": 0, "warning": 3, "danger": 5, "running": True}
FEEDBACK_STATUSES = {"active", "non_issue", "in_progress", "resolved"}


def now():
    return datetime.now(UTC)


def stamp(value):
    return value.astimezone(UTC).isoformat(timespec="seconds")


def parse_date(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(UTC)
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("작성 시각은 시간대가 포함된 ISO 8601 형식이어야 합니다.") from exc


def valid_url(value):
    try:
        u = urlsplit(value)
        if u.scheme != "https" or not u.hostname or u.username or u.password or u.port not in (None, 443):
            raise ValueError()
        return value
    except (ValueError, TypeError) as exc:
        raise ValueError("인증정보가 없는 HTTPS URL을 입력해 주세요.") from exc


def canonical_url(value):
    u = urlsplit(valid_url(value))
    query = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}]
    return urlunsplit((u.scheme, u.netloc.lower(), u.path, urlencode(sorted(query)), ""))


def normalize(text):
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


def match_keywords(title, body, dictionary):
    # Keep the argument for callers/old databases, but classify titles only.
    haystack = normalize(title)
    return [k["name"] for k in dictionary if k["enabled"] and any(normalize(t) in haystack for t in k["terms"])]


def level(count, settings):
    return "danger" if count >= settings["danger"] else "warning" if count >= settings["warning"] else "normal"


class Database:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sources (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, url TEXT NOT NULL,
                    enabled INTEGER NOT NULL, config TEXT NOT NULL, last_attempt TEXT, last_success TEXT,
                    error TEXT, warning TEXT, collected INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL,
                    title TEXT NOT NULL, body TEXT NOT NULL, published_at TEXT NOT NULL,
                    url TEXT NOT NULL, collected_at TEXT NOT NULL, UNIQUE(source_id, url));
                CREATE INDEX IF NOT EXISTS posts_published ON posts(published_at);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
                    level TEXT NOT NULL, count INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS site_cooldowns (
                    domain TEXT PRIMARY KEY, until_at TEXT NOT NULL, reason TEXT NOT NULL,
                    failures INTEGER NOT NULL, updated_at TEXT NOT NULL);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(sources)")}
            if "warning" not in columns:
                db.execute("ALTER TABLE sources ADD COLUMN warning TEXT")
            post_columns = {row[1] for row in db.execute("PRAGMA table_info(posts)")}
            if "feedback_status" not in post_columns:
                db.execute("ALTER TABLE posts ADD COLUMN feedback_status TEXT NOT NULL DEFAULT 'active'")
            if "status_updated_at" not in post_columns:
                db.execute("ALTER TABLE posts ADD COLUMN status_updated_at TEXT")
            for key, value in {"settings": DEFAULT_SETTINGS, "keywords": DEFAULT_KEYWORDS,
                               "started_at": stamp(now()), "session_started_at": stamp(now()), "last_level": "normal"}.items():
                db.execute("INSERT OR IGNORE INTO meta VALUES (?,?)", (key, json.dumps(value, ensure_ascii=False)))
            old_settings = json.loads(db.execute("SELECT value FROM meta WHERE key='settings'").fetchone()[0])
            settings = {key: old_settings.get(key, value) for key, value in DEFAULT_SETTINGS.items()}
            db.execute("UPDATE meta SET value=? WHERE key='settings'", (json.dumps(settings),))

    @contextmanager
    def connect(self):
        with self.lock:
            db = sqlite3.connect(self.path, timeout=20)
            db.row_factory = sqlite3.Row
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()

    def get(self, key):
        with self.connect() as db:
            row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None

    def put(self, key, value):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value, ensure_ascii=False)))

    def begin_session(self, at=None):
        """Start one server run, retaining records, triage and site cooldowns."""
        with self.lock:
            self.put("session_started_at", stamp(at or now()))
            self.put("settings", {**self.get("settings"), "lookback_minutes": 0})
            self.put("last_level", "normal")

    def scope(self):
        with self.lock:
            started = parse_date(self.get("session_started_at"))
            lookback = self.get("settings").get("lookback_minutes", 0)
            return {"session_started_at": stamp(started), "start_at": stamp(started - timedelta(minutes=lookback)),
                    "lookback_minutes": lookback}

    def set_feedback_status(self, post_id, status):
        if status not in FEEDBACK_STATUSES:
            raise ValueError("피드백 처리 상태를 확인해 주세요.")
        with self.connect() as db:
            changed = db.execute("UPDATE posts SET feedback_status=?,status_updated_at=? WHERE id=?",
                                 (status, stamp(now()), post_id)).rowcount
        return bool(changed)

    def sources(self):
        with self.connect() as db:
            return [{**dict(r), "enabled": bool(r["enabled"]), "config": json.loads(r["config"])}
                    for r in db.execute("SELECT * FROM sources ORDER BY rowid")]

    def save_source(self, data):
        with self.connect() as db:
            db.execute("""INSERT INTO sources(id,name,kind,url,enabled,config) VALUES (?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name,kind=excluded.kind,url=excluded.url,
                enabled=excluded.enabled,config=excluded.config,
                last_attempt=CASE WHEN sources.url<>excluded.url OR sources.kind<>excluded.kind OR sources.config<>excluded.config
                    THEN NULL ELSE sources.last_attempt END""",
                (data["id"], data["name"], data["kind"], data["url"], int(data["enabled"]), json.dumps(data["config"])))

    def site_cooldown(self, domain, at=None):
        with self.connect() as db:
            row = db.execute("SELECT * FROM site_cooldowns WHERE domain=?", (domain,)).fetchone()
        if row and parse_date(row["until_at"]) > (at or now()):
            return dict(row)
        return None

    def defer_site(self, domain, reason, retry_after=1800, at=None):
        at = at or now()
        with self.connect() as db:
            previous = db.execute("SELECT * FROM site_cooldowns WHERE domain=?", (domain,)).fetchone()
            failures = previous["failures"] + 1 if previous else 1
            delay = max(retry_after, min(1800 * 2 ** min(failures - 1, 4), 21600))
            until = at + timedelta(seconds=delay)
            if previous and parse_date(previous["until_at"]) > until:
                until = parse_date(previous["until_at"])
            data = {"domain": domain, "until_at": stamp(until), "reason": reason[:500], "failures": failures, "updated_at": stamp(at)}
            db.execute("INSERT OR REPLACE INTO site_cooldowns VALUES (?,?,?,?,?)", tuple(data.values()))
        return data

    def site_recovered(self, domain):
        # An older successful request cannot clear a newer, still-active block.
        with self.connect() as db:
            db.execute("DELETE FROM site_cooldowns WHERE domain=? AND until_at<=?", (domain, stamp(now())))

    def ingest(self, source_id, posts, at=None):
        with self.lock:
            return self._ingest(source_id, posts, at)

    def _ingest(self, source_id, posts, at=None):
        at = at or now()
        start = parse_date(self.scope()["start_at"])
        validated, outside = [], 0
        for p in posts:
            published = parse_date(p["published_at"])
            if published > at + timedelta(minutes=1):
                raise ValueError("미래 시각의 게시글은 저장할 수 없습니다.")
            if not p["title"].strip():
                raise ValueError("게시글 제목이 필요합니다.")
            url = canonical_url(p["url"])
            if published < start:
                outside += 1
                continue
            validated.append((source_id, p["title"][:1000], "",
                              stamp(published), url, stamp(at)))
        with self.connect() as db:
            inserted = 0
            for row in validated:
                inserted += db.execute("""INSERT OR IGNORE INTO posts
                    (source_id,title,body,published_at,url,collected_at) VALUES (?,?,?,?,?,?)""", row).rowcount
        result = {"inserted": inserted, "duplicates": len(validated) - inserted}
        if outside:
            result["outside_scope"] = outside
        return result

    def posts(self, start=None, end=None):
        dictionary = self.get("keywords")
        names = {s["id"]: s["name"] for s in self.sources()}
        clauses, params = [], []
        if start is not None:
            clauses.append("published_at>=?")
            params.append(stamp(start))
        if end is not None:
            clauses.append("published_at<=?")
            params.append(stamp(end))
        query = "SELECT * FROM posts" + (" WHERE " + " AND ".join(clauses) if clauses else "")
        with self.connect() as db:
            rows = [dict(r) for r in db.execute(query + " ORDER BY published_at DESC,id DESC", params)]
        for p in rows:
            p["keywords"] = match_keywords(p["title"], p["body"], dictionary)
            p["source_name"] = names.get(p["source_id"], "삭제된 채널")
        return rows

    def detect(self, at=None):
        # Serialize the snapshot and transition across API/scheduler threads.
        with self.lock:
            return self._detect(at)

    def _detect(self, at=None):
        at = at or now()
        settings = self.get("settings")
        scope = self.scope()
        start = parse_date(scope["start_at"])
        active = {s["id"] for s in self.sources() if s["enabled"]}
        recent = [p for p in self.posts(start, at) if p["keywords"] and p["source_id"] in active
                  and p["feedback_status"] == "active" and start <= parse_date(p["published_at"]) <= at]
        current = level(len(recent), settings)
        counts = {}
        for p in recent:
            for k in p["keywords"]:
                counts[k] = counts.get(k, 0) + 1
        ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
        payload = {"keywords": [{"name": k, "count": n} for k, n in ranked],
                   "channels": sorted({p["source_name"] for p in recent}),
                   "links": [{"title": p["title"], "url": p["url"]} for p in sorted(recent, key=lambda x: (len(x["keywords"]), x["published_at"]), reverse=True)[:3]],
                   **scope, "delivery": "not_configured"}
        # Serialize transitions so repeated refreshes never generate duplicate events.
        with self.connect() as db:
            previous = json.loads(db.execute("SELECT value FROM meta WHERE key='last_level'").fetchone()[0])
            if current != previous and current != "normal":
                db.execute("INSERT INTO events(created_at,level,count,payload) VALUES (?,?,?,?)",
                           (stamp(at), current, len(recent), json.dumps(payload, ensure_ascii=False)))
            db.execute("UPDATE meta SET value=? WHERE key='last_level'", (json.dumps(current),))
        return {"level": current, "count": len(recent), **payload}

    def events(self):
        with self.connect() as db:
            return [{**dict(r), "payload": json.loads(r["payload"])} for r in db.execute("SELECT * FROM events ORDER BY id DESC LIMIT 100")]

    def seed_demo(self):
        if self.get("demo_seeded"):
            return
        channels = [("dc", "디시인사이드", "dcinside"), ("cafe", "공식 카페", "html"), ("forum", "공식 커뮤니티", "rss")]
        for ident, name, kind in channels:
            self.save_source({"id": ident, "name": name, "kind": kind, "url": "https://example.com/" + ident,
                              "enabled": True, "config": {}})
        titles = ["패치 이후 접속 불가 현상 있습니다", "레이드 입장하면 계속 팅김", "결제는 됐는데 상품이 안 들어와요",
                  "캐릭터 선택 후 무한 로딩", "창고 아이템 증발한 분 계신가요?", "보스전 도중 강제 종료가 됩니다",
                  "로그인 안됨 저만 그런가요", "업데이트 후 프레임이 조금 좋아졌네요", "오늘 레이드 파티 구해요",
                  "결제 오류 재현 순서 공유합니다", "지역 이동 중 무한로딩 발생", "사전 보상 잘 받았습니다"]
        at = now()
        self.begin_session(at - timedelta(days=3))
        for i in range(132):
            # Representative fixture, never presented as actual community data.
            age = (i * 1.6 + 1) if i < 24 else (45 + (i - 24) * 12)
            title = titles[i % len(titles)]
            self.ingest(channels[i % 3][0], [{"title": title,
                "body": "[샘플 피드백] 1. 게임 실행 → 2. 캐릭터 선택 → 3. 해당 기능 이용 시 문제가 발생합니다.\n패치 이후 반복되고 있고, 재접속해도 같은 현상이 나타납니다. 확인 부탁드립니다.",
                "published_at": stamp(at - timedelta(minutes=age)), "url": f"https://example.com/sample/{i}"}], at)
        self.put("demo_seeded", True)
        self.put("demo_at", stamp(at))
        self.begin_session(at - timedelta(hours=1))
        self.detect(at)
