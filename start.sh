#!/usr/bin/env bash
set -e
python -m playwright install-deps || true
python -m playwright install chromium
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
