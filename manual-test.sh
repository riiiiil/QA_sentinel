#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
exec sh start.sh --test-mode --port 8001
