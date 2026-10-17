"""Create an isolated environment; install only on first use or requirements change."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import venv

ROOT = Path(__file__).resolve().parent


def main():
    if sys.version_info < (3, 11):
        print("Python 3.11 or later is required. Please install it and try again.")
        return 2
    directory = ROOT / ".venv"
    python = directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not python.exists():
        print("Creating a local Python environment...", flush=True)
        try:
            venv.EnvBuilder(with_pip=True).create(directory)
        except Exception as exc:
            print(f"Environment setup failed ({type(exc).__name__}). Check Python venv support and folder permissions.")
            return 2
    requirements = ROOT / "requirements.txt"
    digest = hashlib.sha256(requirements.read_bytes()).hexdigest()
    marker = directory / ".qa-sentinel-requirements"
    check = [str(python), "-c", "import fastapi,uvicorn,httpx,bs4; from zoneinfo import ZoneInfo; ZoneInfo('Asia/Seoul')"]
    try:
        ready = marker.exists() and marker.read_text().strip() == digest
        if ready:
            ready = subprocess.run(check, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
        if not ready:
            print("Installing QA Sentinel dependencies (Internet is required on first use)...", flush=True)
            subprocess.run([str(python), "-m", "pip", "install", "--disable-pip-version-check", "--retries", "2", "--timeout", "20", "-r", str(requirements)], cwd=ROOT, check=True)
            subprocess.run(check, cwd=ROOT, check=True)
            marker.write_text(digest + "\n")
        flags = sys.argv[1:]
        if not any(flag in flags for flag in ("--check", "--self-test", "--open-browser")):
            flags = ["--open-browser", *flags]
        return subprocess.run([str(python), str(ROOT / "run.py"), *flags], cwd=ROOT).returncode
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"Setup or launch failed ({type(exc).__name__}). See the message above. No data was deleted.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
