#!/usr/bin/env bash
# Simple smoke test for local reproducibility
set -euo pipefail

echo "Running profiler to generate staging artifacts..."
python3 src/generate_staging_ddl.py

# check files exist
ART1=data/schemas/staging_create_tables.sql
ART2=data/schemas/staging_schema_profiles.json

if [[ -f "$ART1" && -f "$ART2" ]]; then
  echo "Profiler artifacts found:" "$ART1" "$ART2"
else
  echo "ERROR: profiler artifacts missing" >&2
  ls -la data/schemas || true
  exit 2
fi

# check airflow user exists
if docker compose ps --services --filter "status=running" | grep -q airflow; then
  echo "Checking Airflow users..."
  docker compose exec -T airflow airflow users list || true
else
  echo "Airflow is not running; skipping user check"
fi

echo "Smoke check complete."
