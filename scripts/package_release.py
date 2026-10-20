#!/usr/bin/env python3
"""Stage a source release without local data, secrets, environments or legacy QA."""
import argparse
import hashlib
import json
from pathlib import Path
import runpy
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parents[1]
VERSION = runpy.run_path(str(ROOT / "qa_sentinel/__init__.py"))["__version__"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "work/release" / f"QA-Sentinel-{VERSION}.zip")
    args = parser.parse_args()
    files = [ROOT / name for name in ["README.md", "START-HERE.md", "MANUAL-TEST.md", "RELEASE-NOTES.md", "requirements.txt", "bootstrap.py", "run.py",
        "start.bat", "start.sh", "test.bat", "test.sh", "manual-test.bat", "manual-test.sh",
        "tests/test_ui.cjs", "tests/test_refresh.cjs", "tests/test_chart.cjs", "scripts/package_release.py", "DEPLOY-PRIVATE.md", "Dockerfile", ".dockerignore", "compose.yaml", "scripts/container_health.py", "requirements-deploy.txt"]]
    files += sorted((ROOT / "tests").glob("test_sentinel*.py"))
    files += sorted(path for path in (ROOT / "qa_sentinel").rglob("*") if path.is_file() and "__pycache__" not in path.parts)
    entries = {str(path.relative_to(ROOT)).replace("\\", "/"): path.read_bytes() for path in files}
    manifest = {"version": VERSION, "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(entries.items())}}
    entries["PACKAGE-MANIFEST.json"] = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(args.output, "w", ZIP_DEFLATED) as archive:
        for name, data in sorted(entries.items()):
            info = ZipInfo("QA-Sentinel/" + name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = (0o100755 if name.endswith(".sh") else 0o100644) << 16
            archive.writestr(info, data)
    with ZipFile(args.output) as archive:
        assert archive.testzip() is None
        for name, digest in manifest["files"].items():
            assert hashlib.sha256(archive.read("QA-Sentinel/" + name)).hexdigest() == digest
    print(json.dumps({"archive": str(args.output.resolve()), "version": VERSION, "files": len(entries),
        "bytes": args.output.stat().st_size, "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()}))


if __name__ == "__main__":
    main()
