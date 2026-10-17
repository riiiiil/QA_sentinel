"""Deployment checks exercise real SQLite WAL snapshots and recovery failure paths."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from qa_sentinel.backup import archives, check_backup, create_backup, restore_backup
from qa_sentinel.runtime import ProcessLock


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.data = self.root / "data"
        self.backups = self.root / "backups"
        self.data.mkdir()
        self.db = sqlite3.connect(self.data / "live.sqlite3")
        self.addCleanup(self.db.close)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE feedback (body TEXT)")
        self.db.execute("INSERT INTO feedback VALUES ('결제 오류')")
        self.db.commit()

    def backup(self, keep=14):
        return create_backup(self.data, self.backups, keep)

    def rows(self):
        with sqlite3.connect(self.data / "live.sqlite3") as db:
            return db.execute("SELECT body FROM feedback").fetchall()

    def rewrite(self, archive, change):
        with ZipFile(archive) as source:
            files = {name: source.read(name) for name in source.namelist()}
        change(files)
        with ZipFile(archive, "w") as target:
            for name, value in files.items():
                target.writestr(name, value)

    def test_online_wal_snapshot_and_restore_preserve_previous_data(self):
        self.assertTrue((self.data / "live.sqlite3-wal").exists())
        archive = self.backup()
        self.assertTrue(check_backup(self.backups))
        self.db.execute("INSERT INTO feedback VALUES ('나중 글')")
        self.db.commit()
        self.db.close()
        previous = restore_backup(archive, self.data, confirmed=True)
        self.assertEqual(self.rows(), [('결제 오류',)])
        with sqlite3.connect(previous / "live.sqlite3") as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM feedback").fetchone()[0], 2)

    def test_snapshot_works_while_server_lock_is_held_but_restore_refuses(self):
        with ProcessLock(self.data):
            archive = self.backup()
            with self.assertRaisesRegex(RuntimeError, '이미 실행'):
                restore_backup(archive, self.data, confirmed=True)

    def test_retention_keeps_only_our_newest_snapshots(self):
        self.backups.mkdir()
        unrelated = self.backups / 'personal.zip'
        unrelated.write_bytes(b'keep')
        first = self.backup(keep=2)
        self.backup(keep=2)
        latest = self.backup(keep=2)
        self.assertEqual(len(archives(self.backups)), 2)
        self.assertFalse(first.exists())
        self.assertEqual(archives(self.backups)[0], latest)
        self.assertTrue(unrelated.exists())

    def test_restore_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, 'confirm-restore'):
            restore_backup(self.backup(), self.data)

    def test_modified_checksum_is_rejected_without_replacing_data(self):
        archive = self.backup()
        self.rewrite(archive, lambda files: files.__setitem__('live.sqlite3', files['live.sqlite3'] + b'tampered'))
        with self.assertRaisesRegex(ValueError, '체크섬'):
            restore_backup(archive, self.data, confirmed=True)
        self.assertEqual(self.rows(), [('결제 오류',)])
        self.assertFalse(list(self.data.glob('pre-restore-*')))

    def test_path_traversal_is_rejected(self):
        archive = self.backup()
        self.rewrite(archive, lambda files: files.__setitem__('../escaped', b'bad'))
        with self.assertRaisesRegex(ValueError, '허용되지 않은'):
            restore_backup(archive, self.data, confirmed=True)
        self.assertFalse((self.root / 'escaped').exists())

    def test_invalid_database_with_valid_checksum_is_rejected(self):
        archive = self.backup()

        def corrupt(files):
            files['live.sqlite3'] = b'not a sqlite database'
            manifest = json.loads(files['manifest.json'])
            manifest['files']['live.sqlite3'] = hashlib.sha256(files['live.sqlite3']).hexdigest()
            files['manifest.json'] = json.dumps(manifest).encode()

        self.rewrite(archive, corrupt)
        with self.assertRaises(sqlite3.DatabaseError):
            restore_backup(archive, self.data, confirmed=True)
        self.assertEqual(self.rows(), [('결제 오류',)])

    def test_failed_install_rolls_back_original_database_and_wal(self):
        archive = self.backup()
        self.db.execute("INSERT INTO feedback VALUES ('보존 대상')")
        self.db.commit()
        self.db.close()
        replace = os.replace

        def fail_install(source, target):
            if Path(source).parent.name.startswith('.restore-'):
                raise OSError('simulated disk failure')
            return replace(source, target)

        with patch('qa_sentinel.backup.os.replace', side_effect=fail_install):
            with self.assertRaisesRegex(OSError, 'disk failure'):
                restore_backup(archive, self.data, confirmed=True)
        self.assertEqual(self.rows(), [('결제 오류',), ('보존 대상',)])

    def test_stale_or_missing_backups_are_unhealthy(self):
        self.assertFalse(check_backup(self.backups))
        archive = self.backup()
        os.utime(archive, (time.time() - 27 * 3600,) * 2)
        self.assertFalse(check_backup(self.backups))

    def test_backup_does_not_create_a_missing_live_database(self):
        missing = self.root / 'missing'
        missing.mkdir()
        with self.assertRaisesRegex(ValueError, 'DB가 없습니다'):
            create_backup(missing, self.backups)
        self.assertFalse((missing / 'live.sqlite3').exists())


if __name__ == '__main__':
    unittest.main()
