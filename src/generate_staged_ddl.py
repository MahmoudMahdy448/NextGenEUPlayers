#!/usr/bin/env python3
"""Generate SQL DDL for staged tables from the staged manifest.

Reads data/schemas/staged_schemas_by_season.json and writes
sql/create_staging_tables.sql containing DROP TABLE IF EXISTS + CREATE TABLE
for each staged table. Columns across seasons are unioned; type conflicts
are resolved to a safe common type where possible.

Usage: python3 src/generate_staged_ddl.py
"""
import json
from pathlib import Path
import re

ROOT = Path.cwd()
STAGED_MANIFEST = ROOT / 'data' / 'schemas' / 'staged_schemas_by_season.json'
OUT_DIR = ROOT / 'sql'
OUT_FILE = OUT_DIR / 'create_staging_tables.sql'


def parse_varchar(t):
    m = re.match(r'VARCHAR\((\d+)\)', t, flags=re.I)
    if m:
        return int(m.group(1))
    return None


def is_decimal(t):
    return bool(re.match(r'DECIMAL\(', t, flags=re.I) or (t and t.upper().startswith('NUMERIC')))


def normalize_type(t):
    if not t:
        return 'TEXT'
    t_up = t.strip().upper()
    if t_up.startswith('VARCHAR'):
        n = parse_varchar(t_up)
        return f'VARCHAR({n if n else 255})'
    if t_up.startswith('DECIMAL') or t_up.startswith('NUMERIC'):
        # keep DECIMAL as-is if present
        return t_up
    if 'TIMESTAMP' in t_up:
        return 'TIMESTAMP'
    if t_up in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        return 'INTEGER'
    if t_up in ('DOUBLE PRECISION', 'FLOAT', 'REAL', 'DOUBLE'):
        return 'DECIMAL(38,8)'
    # fallback
    return 'TEXT'


def pick_common_type(types):
    # Normalize and pick a safe type that can hold all values
    norm = [normalize_type(t) for t in types if t]
    norm = list(dict.fromkeys(norm))  # preserve order, uniq
    if not norm:
        return 'TEXT'
    # if any TIMESTAMP -> TIMESTAMP
    if any('TIMESTAMP' == t for t in norm):
        return 'TIMESTAMP'
    # if any VARCHAR -> choose largest VARCHAR
    varchars = [parse_varchar(t) for t in norm if t.startswith('VARCHAR')]
    varchars = [v for v in varchars if v]
    if varchars:
        maxlen = max(varchars) if varchars else 255
        return f'VARCHAR({maxlen})'
    # if any DECIMAL/NUMERIC -> choose DECIMAL with wide precision
    if any(is_decimal(t) or (t and t.startswith('DECIMAL')) for t in norm):
        # keep one with parentheses if present, else use a safe default
        for t in norm:
            if t.startswith('DECIMAL'):
                return t
        return 'DECIMAL(38,8)'
    # if all INTEGER -> INTEGER
    if all(t == 'INTEGER' for t in norm):
        return 'INTEGER'
    # fallback to TEXT
    return 'TEXT'


def main():
    if not STAGED_MANIFEST.exists():
        print(f"Staged manifest not found: {STAGED_MANIFEST}")
        return 1
    j = json.loads(STAGED_MANIFEST.read_text(encoding='utf-8'))
    seasons = j.get('seasons', {})

    # collect per-table column types across seasons
    table_cols = {}
    for season, sdata in seasons.items():
        tables = sdata.get('tables', {})
        for tname, tdata in tables.items():
            cols = tdata.get('columns', {})
            table_cols.setdefault(tname, {})
            for col, t in cols.items():
                table_cols[tname].setdefault(col, set()).add(t)

    # produce DDL: use staging table name as stg_<table>
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_FILE.open('w', encoding='utf-8') as fh:
        fh.write('-- Generated staging DDL\n')
        fh.write('-- source: data/schemas/staged_schemas_by_season.json\n')
        fh.write('-- generated_at: \n\n')
        for tname, cols in sorted(table_cols.items()):
            stg_name = f'stg_{tname}'
            fh.write(f'-- DDL for table: {tname}\n')
            fh.write(f'DROP TABLE IF EXISTS "{stg_name}" CASCADE;\n')
            fh.write(f'CREATE TABLE "{stg_name}" (\n')
            col_lines = []
            for col, types in cols.items():
                chosen = pick_common_type(list(types))
                col_lines.append(f'  "{col}" {chosen}')
            fh.write(',\n'.join(col_lines))
            fh.write('\n);\n\n')

    print(f'Wrote staging DDL to: {OUT_FILE}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
