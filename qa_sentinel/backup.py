"""Consistent SQLite snapshots and stopped-server restore, using only stdlib."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import uuid
from zipfile import ZipFile, ZIP_DEFLATED

from . import __version__
from .runtime import ProcessLock

DATABASES = ("live.sqlite3", "demo.sqlite3")
ARCHIVE_PATTERN = "qa-sentinel-backup-*.zip"


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def connection(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=20)


def validate_database(path):
    with closing(connection(path)) as database:
        if database.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise ValueError(f"SQLite 무결성 검사 실패: {Path(path).name}")


def archives(directory):
    return sorted(Path(directory).glob(ARCHIVE_PATTERN), key=lambda p: p.name, reverse=True)


def create_backup(data_dir, backup_dir, keep=14):
    if keep < 1:
        raise ValueError("백업 보관 개수는 1 이상이어야 합니다.")
    data_dir, backup_dir = Path(data_dir), Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    if not (data_dir / "live.sqlite3").is_file():
        raise ValueError("실제 수집 DB가 없습니다. 서버 시작 후 다시 시도하세요.")
    # Separate from the server lock: snapshots are allowed while the server runs.
    with ProcessLock(data_dir / ".maintenance"), tempfile.TemporaryDirectory(dir=backup_dir, prefix=".snapshot-") as temporary:
        temporary = Path(temporary)
        hashes = {}
        for name in DATABASES:
            path = data_dir / name
            if not path.is_file():
                continue
            snapshot = temporary / name
            deadline = time.monotonic() + 120

            def progress(*_):
                if time.monotonic() > deadline:
                    raise TimeoutError("SQLite 백업 제한 시간(120초)을 초과했습니다.")

            with closing(connection(path)) as source, closing(sqlite3.connect(snapshot)) as target:
                source.backup(target, pages=256, progress=progress, sleep=0.1)
                target.execute("PRAGMA journal_mode=DELETE")
            validate_database(snapshot)
            hashes[name] = digest(snapshot)
        timestamp = datetime.now(timezone.utc)
        manifest = {"format": 1, "version": __version__, "created_at": timestamp.isoformat(), "files": hashes}
        filename = f"qa-sentinel-backup-{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}.zip"
        staging = temporary / "backup.zip"
        with ZipFile(staging, "w", ZIP_DEFLATED) as archive:
            for name in hashes:
                archive.write(temporary / name, name)
            archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False))
        staging.chmod(0o600)
        result = backup_dir / filename
        os.replace(staging, result)
        for old in archives(backup_dir)[keep:]:
            old.unlink()
        return result


def unpack_verified(archive_path, destination):
    """Never use extractall: only verified, allowlisted DB basenames are written."""
    with ZipFile(archive_path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or "manifest.json" not in names:
            raise ValueError("중복 파일 또는 매니페스트 누락")
        if archive.getinfo("manifest.json").file_size > 65536:
            raise ValueError("잘못된 백업 매니페스트")
        manifest = json.loads(archive.read("manifest.json"))
        files = manifest.get("files", {})
        if manifest.get("format") != 1 or not isinstance(files, dict) or "live.sqlite3" not in files:
            raise ValueError("지원하지 않는 백업 형식")
        if not set(files).issubset(DATABASES) or set(names) != set(files) | {"manifest.json"}:
            raise ValueError("허용되지 않은 백업 파일")
        for name, expected in files.items():
            path = destination / name
            with archive.open(name) as source, path.open("wb") as target:
                shutil.copyfileobj(source, target)
            path.chmod(0o600)
            if digest(path) != expected:
                raise ValueError(f"백업 체크섬 불일치: {name}")
            validate_database(path)
        return manifest


def restore_backup(archive_path, data_dir, confirmed=False):
    if not confirmed:
        raise ValueError("서버와 자동 백업을 중지한 뒤 --confirm-restore가 필요합니다.")
    data_dir = Path(data_dir)
    # The same lock used by run.py prevents overwriting a running server's DB.
    with ProcessLock(data_dir / ".maintenance"), ProcessLock(data_dir), tempfile.TemporaryDirectory(dir=data_dir, prefix=".restore-") as temporary:
        staging = Path(temporary)
        manifest = unpack_verified(archive_path, staging)
        previous = Path(tempfile.mkdtemp(dir=data_dir, prefix="pre-restore-"))
        moved, installed = [], []
        try:
            for name in DATABASES:
                # A restored snapshot is self-contained; keep old WAL with the old DB.
                for suffix in ("", "-wal", "-shm"):
                    current = data_dir / (name + suffix)
                    if current.exists():
                        os.replace(current, previous / current.name)
                        moved.append(current.name)
            for name in manifest["files"]:
                os.replace(staging / name, data_dir / name)
                installed.append(name)
        except BaseException:
            for name in installed:
                (data_dir / name).unlink(missing_ok=True)
            for name in reversed(moved):
                os.replace(previous / name, data_dir / name)
            raise
        return previous


def check_backup(backup_dir, max_age=26 * 3600):
    latest = archives(backup_dir)
    if not latest:
        return False
    age = time.time() - latest[0].stat().st_mtime
    # Keep health probes cheap: full DB checks happen on snapshot creation/restore.
    with ZipFile(latest[0]) as archive:
        if not -60 <= age <= max_age or archive.getinfo("manifest.json").file_size > 65536:
            return False
        manifest = json.loads(archive.read("manifest.json"))
        files = manifest.get("files", {})
        return manifest.get("format") == 1 and isinstance(files, dict) and "live.sqlite3" in files and set(files).issubset(DATABASES) and set(archive.namelist()) == set(files) | {"manifest.json"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="QA Sentinel SQLite 백업/복구")
    parser.add_argument("action", choices=["create", "daemon", "list", "check", "restore"])
    parser.add_argument("--data-dir", type=Path, default=Path(os.environ.get("QA_SENTINEL_DATA", ".sentinel-data")))
    parser.add_argument("--backup-dir", type=Path, default=Path("backups"))
    parser.add_argument("--keep", type=int, default=14)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--confirm-restore", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.keep < 1:
        parser.error("--keep는 1 이상이어야 합니다.")
    try:
        if args.action == "list":
            for path in archives(args.backup_dir):
                print(path.name)
        elif args.action == "check":
            return 0 if check_backup(args.backup_dir) else 1
        elif args.action == "restore":
            if not args.archive:
                parser.error("--archive가 필요합니다.")
            print("복구 완료. 이전 DB 보관 위치:", restore_backup(args.archive, args.data_dir, args.confirm_restore))
        elif args.action == "create":
            print(create_backup(args.data_dir, args.backup_dir, args.keep))
        else:
            while True:
                try:
                    logging.info("Backup complete: %s", create_backup(args.data_dir, args.backup_dir, args.keep))
                except Exception:
                    logging.exception("Backup failed; retrying in 5 minutes")
                    time.sleep(300)
                else:
                    time.sleep(24 * 3600)
    except Exception as exc:
        logging.error("%s: %s", type(exc).__name__, exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
