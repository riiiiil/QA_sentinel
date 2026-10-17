#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
if command -v python3 >/dev/null 2>&1; then
    exec python3 bootstrap.py "$@"
fi
if command -v python >/dev/null 2>&1; then
    exec python bootstrap.py "$@"
fi
printf 'Python 3.11+ is required. Install Python, then run this script again.\n' >&2
exit 2
