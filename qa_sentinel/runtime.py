"""Local process diagnostics; uses only the Python standard library."""
import importlib
import os
from pathlib import Path
import socket
import sys
import tempfile


def check_dependencies():
    errors = []
    if sys.version_info < (3, 11):
        errors.append("Python 3.11 이상이 필요합니다.")
    for name in ("fastapi", "uvicorn", "httpx", "bs4"):
        try:
            importlib.import_module(name)
        except Exception as exc:
            errors.append(f"{name} 로드 실패 ({type(exc).__name__}). start.bat 또는 sh start.sh로 의존성을 설치하세요.")
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo("Asia/Seoul")
    except Exception:
        errors.append("한국 시간대 정보가 없습니다. requirements.txt의 tzdata를 설치하세요.")
    return errors


def check_data_directory(directory):
    directory = Path(directory)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".write-check-", dir=directory):
            pass
    except OSError as exc:
        return [f"데이터 폴더에 쓸 수 없습니다 ({type(exc).__name__}): {directory}"]
    return []


def check_port(port, host="127.0.0.1"):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((host, port))
    except OSError:
        return [f"{host}:{port} 포트를 사용할 수 없습니다. 실행 중인 앱을 종료하거나 --port {port + 1 if port < 65535 else 8000}을 사용하세요."]
    return []


class ProcessLock:
    """One crawler process per data directory, including on Windows."""
    def __init__(self, directory):
        self.path = Path(directory) / ".server.lock"
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                if self.path.stat().st_size == 0:
                    self.file.write(b"0")
                    self.file.flush()
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.file.close()
            self.file = None
            raise RuntimeError("같은 데이터 폴더를 사용하는 QA Sentinel이 이미 실행 중입니다.") from exc
        return self

    def __exit__(self, *args):
        if self.file:
            if os.name == "nt":
                import msvcrt
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            self.file.close()
            self.file = None
