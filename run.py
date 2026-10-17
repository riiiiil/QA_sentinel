"""QA Sentinel local launcher and offline diagnostics."""
import argparse
import os
from pathlib import Path
import sys

from qa_sentinel import __version__
from qa_sentinel.runtime import ProcessLock, check_data_directory, check_dependencies, check_port

ROOT = Path(__file__).resolve().parent


def open_when_ready(port):
    import socket
    import time
    import webbrowser
    for _ in range(60):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                webbrowser.open(f"http://127.0.0.1:{port}")
                return
        except OSError:
            time.sleep(0.5)


def main(argv=None):
    parser = argparse.ArgumentParser(description="QA Sentinel 로컬 실행 / 진단")
    parser.add_argument("--port", type=int, default=8000, help="접속 포트 (기본 8000)")
    parser.add_argument("--host", choices=["127.0.0.1", "0.0.0.0"], default="127.0.0.1", help="수신 주소. 컨테이너 내부에서만 0.0.0.0 사용")
    parser.add_argument("--data-dir", help="데이터와 로그 저장 폴더")
    parser.add_argument("--open-browser", action="store_true", help="서버 시작 후 브라우저 열기")
    parser.add_argument("--check", action="store_true", help="의존성·폴더·포트 진단 후 종료")
    parser.add_argument("--self-test", action="store_true", help="외부 접속 없이 자동 테스트 실행")
    parser.add_argument("--test-mode", action="store_true", help="실제 데이터와 분리된 수동 테스트 모드. 외부 수집 차단")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("포트는 1~65535 사이여야 합니다.")
    if args.test_mode and args.data_dir:
        parser.error("--test-mode는 전용 데이터 폴더를 사용하므로 --data-dir와 함께 사용할 수 없습니다.")
    errors = check_dependencies()
    if errors:
        for error in errors:
            print("[ERROR] " + error)
        return 2
    if args.self_test:
        import unittest
        suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_sentinel*.py")
        if not suite.countTestCases():
            print("[ERROR] tests/test_sentinel*.py 파일이 없습니다.")
            return 2
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if args.test_mode:
        directory = ROOT / ".sentinel-test-data"
        os.environ["QA_SENTINEL_OFFLINE_TEST"] = "1"
    else:
        directory = Path(args.data_dir or os.environ.get("QA_SENTINEL_DATA", ROOT / ".sentinel-data")).expanduser().resolve()
    errors = check_data_directory(directory) + check_port(args.port, args.host)
    if errors:
        for error in errors:
            print("[ERROR] " + error)
        return 2
    os.environ["QA_SENTINEL_DATA"] = str(directory)
    print(f"QA Sentinel {__version__}")
    print(f"Listening: http://{args.host}:{args.port}")
    print(f"Local URL: http://127.0.0.1:{args.port}")
    print(f"Data: {directory}")
    if args.check:
        print("[OK] Python, dependencies, data directory and port are ready.")
        return 0
    logs = directory / "logs"
    logs.mkdir(exist_ok=True)
    print(f"Log: {logs / 'qa-sentinel.log'}")
    if args.test_mode:
        print("[TEST MODE] External collection is disabled. Production data is not used.")
    log_config = {
        "version": 1, "disable_existing_loggers": False,
        "formatters": {"default": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"}},
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "default", "stream": "ext://sys.stderr"},
            "file": {"class": "logging.handlers.RotatingFileHandler", "formatter": "default", "filename": str(logs / "qa-sentinel.log"),
                     "maxBytes": 2000000, "backupCount": 3, "encoding": "utf-8"}},
        "root": {"handlers": ["console", "file"], "level": "INFO"},
        "loggers": {name: {"handlers": [], "propagate": True, "level": "INFO"} for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "qa_sentinel")},
    }
    import uvicorn
    try:
        with ProcessLock(directory):
            if args.open_browser:
                from threading import Thread
                Thread(target=open_when_ready, args=(args.port,), daemon=True).start()
            uvicorn.run("qa_sentinel.app:app", host=args.host, port=args.port, log_config=log_config)
    except RuntimeError as exc:
        print("[ERROR] " + str(exc))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
