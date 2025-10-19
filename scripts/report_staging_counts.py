#!/usr/bin/env python3
"""Report row counts for all tables in the staging schema.

Usage: python3 scripts/report_staging_counts.py
Runs inside the project (docker compose run --rm app ...) so it can reach the DB service.
"""
import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    # fallback to individual env vars if DATABASE_URL not present
    user = os.environ.get('POSTGRES_USER', 'football_user')
    pw = os.environ.get('POSTGRES_PASSWORD', 'football_pass')
    host = os.environ.get('POSTGRES_HOST', 'db')
    port = os.environ.get('POSTGRES_PORT', '5432')
    db = os.environ.get('POSTGRES_DB', 'football_db')
    DATABASE_URL = f'postgresql://{user}:{pw}@{host}:{port}/{db}'

engine = create_engine(DATABASE_URL)

def main():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='staging' ORDER BY table_name;"))
        tables = [r[0] for r in rows]
        if not tables:
            print('No tables found in schema "staging"')
            return
        total = 0
        print('Row counts for staging.*')
        print('-------------------------')
        for t in tables:
            q = text(f'SELECT COUNT(*) FROM staging."{t}";')
            try:
                cnt = conn.execute(q).scalar()
            except Exception as e:
                print(f'{t}: ERROR - {e}')
                continue
            print(f'{t}: {cnt}')
            total += int(cnt or 0)
        print('-------------------------')
        print(f'Total rows across {len(tables)} tables: {total}')

if __name__ == '__main__':
    main()
