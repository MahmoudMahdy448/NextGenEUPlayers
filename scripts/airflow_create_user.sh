#!/usr/bin/env bash
set -euo pipefail

# Simple idempotent startup wrapper for Airflow standalone that ensures a
# specified admin user exists. Environment variables expected:
#  - AIRFLOW_USER (username)
#  - AIRFLOW_PASSWORD
#  - AIRFLOW_EMAIL
#  - AIRFLOW_FIRSTNAME
#  - AIRFLOW_LASTNAME

: "${AIRFLOW_USER:=devadmin}"
: "${AIRFLOW_PASSWORD:=DevAdmin!23}"
: "${AIRFLOW_EMAIL:=devadmin@example.com}"
: "${AIRFLOW_FIRSTNAME:=Dev}"
: "${AIRFLOW_LASTNAME:=Admin}"

# initialize/migrate DB (safe to run repeatedly)
airflow db upgrade || true

# check if user exists
if airflow users list | awk -F'|' '{gsub(/ /, "", $2); if(NR>2) print $2}' | grep -wq "${AIRFLOW_USER}"; then
  echo "User ${AIRFLOW_USER} already exists"
else
  echo "Creating user ${AIRFLOW_USER}"
  airflow users create \
    --username "${AIRFLOW_USER}" \
    --firstname "${AIRFLOW_FIRSTNAME}" \
    --lastname "${AIRFLOW_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_EMAIL}" \
    --password "${AIRFLOW_PASSWORD}"
fi

# exec the original entrypoint (start airflow standalone)
exec airflow standalone
