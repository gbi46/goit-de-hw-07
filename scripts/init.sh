#!/usr/bin/env bash
set -euo pipefail
airflow db migrate
if ! airflow users list --output json | python -c 'import json,sys; sys.exit(0 if any(u["username"] == "admin" for u in json.load(sys.stdin)) else 1)'; then
  airflow users create --username admin --password admin --firstname Local --lastname Admin --role Admin --email admin@example.com
fi
