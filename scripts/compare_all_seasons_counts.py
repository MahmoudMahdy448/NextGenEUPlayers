#!/usr/bin/env python3
"""Compare expected rows from all seasons' staging CSVs to DB counts in staging schema.

Run inside the compose network:
  docker compose run --rm app python3 scripts/compare_all_seasons_counts.py
"""
import os
import glob
import re
from collections import defaultdict
from sqlalchemy import create_engine, text

DATA_ROOT = os.path.join('data', 'staging_data')

def filename_to_table(fname):
    bn = os.path.basename(fname)
    m = re.match(r'^(?P<prefix>stg_[\w_\-]+)_(?P<season>\d{4}-\d{4})\.csv$', bn)
    if m:
        return m.group('prefix'), m.group('season')
    parts = bn.rsplit('_', 1)
    if len(parts) == 2:
        return parts[0], None
    return os.path.splitext(bn)[0], None

def count_file_rows(path):
    rows = 0
    with open(path, 'rb') as fh:
        for i, line in enumerate(fh):
            if i == 0:
                continue
            if line.strip():
                rows += 1
    return rows

def build_db_engine():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if DATABASE_URL:
        return create_engine(DATABASE_URL)
    user = os.environ.get('POSTGRES_USER', 'football_user')
    pw = os.environ.get('POSTGRES_PASSWORD', 'football_pass')
    host = os.environ.get('POSTGRES_HOST', 'db')
    port = os.environ.get('POSTGRES_PORT', '5432')
    db = os.environ.get('POSTGRES_DB', 'football_db')
    url = f'postgresql://{user}:{pw}@{host}:{port}/{db}'
    return create_engine(url)

def main():
    pattern = os.path.join(DATA_ROOT, '**', 'stg_*.csv')
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        print(f'No staging CSVs found under {DATA_ROOT}')
        return

    expected_per_table = defaultdict(int)
    expected_per_table_by_season = defaultdict(lambda: defaultdict(int))
    files_by_table = defaultdict(list)
    for f in files:
        tbl, season = filename_to_table(f)
        cnt = count_file_rows(f)
        expected_per_table[tbl] += cnt
        expected_per_table_by_season[season][tbl] += cnt
        files_by_table[tbl].append((f, season, cnt))

    engine = build_db_engine()
    with engine.connect() as conn:
        print('Table | expected_total_rows (all seasons) | db_rows | diff')
        print('-----')
        grand_expected = 0
        grand_db = 0
        for tbl in sorted(expected_per_table.keys()):
            exp = expected_per_table[tbl]
            grand_expected += exp
            try:
                res = conn.execute(text(f'SELECT COUNT(*) FROM staging."{tbl}";'))
                dbcnt = int(res.scalar() or 0)
            except Exception as e:
                dbcnt = None
            grand_db += (dbcnt or 0)
            diff = (dbcnt - exp) if dbcnt is not None else 'N/A'
            print(f'{tbl} | {exp} | {dbcnt} | {diff}')

        print('-----')
        print(f'TOTAL | {grand_expected} | {grand_db} | {grand_db - grand_expected}')

        # show per-season breakdown for tables where db rows > 0
        print('\nPer-season breakdown for tables with non-zero DB rows:')
        for tbl, files in sorted(files_by_table.items()):
            try:
                res = conn.execute(text(f'SELECT COUNT(*) FROM staging."{tbl}";'))
                dbcnt = int(res.scalar() or 0)
            except Exception:
                dbcnt = 0
            if dbcnt == 0:
                continue
            print(f'\n{tbl}: DB rows = {dbcnt}')
            seasons = defaultdict(int)
            for f, season, cnt in files:
                seasons[season] += cnt
            for season, cnt in sorted(seasons.items()):
                print(f'  {season}: {cnt} rows (CSV)')

if __name__ == '__main__':
    main()
