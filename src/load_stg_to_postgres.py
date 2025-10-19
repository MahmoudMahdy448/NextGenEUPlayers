import os
import glob
import re
import time
import math
import uuid
import io
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")
DDL_FILE = os.path.join('sql', 'create_staging_tables.sql')
DATA_DIR = os.path.join('data', 'staging_data')


def apply_staging_ddl(conn):
    """Apply the generated staging DDL into the configured staging schema.
    The DDL file contains unqualified table names. We set search_path so
    CREATE TABLE / DROP TABLE operate inside the staging schema.
    """
    if not os.path.exists(DDL_FILE):
        print(f"⚠️ DDL file not found: {DDL_FILE}")
        return
    with open(DDL_FILE, 'r', encoding='utf-8') as fh:
        sql = fh.read()

    # Ensure schema exists
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};"))
    # Set search_path so unqualified CREATE TABLE statements create tables in staging
    conn.execute(text(f"SET search_path TO {STAGING_SCHEMA}, public;"))

    # Execute the DDL script. Use simple split by semicolon to avoid relying on DBAPI multi-statement support.
    # This assumes the generated SQL uses standard statement separation.
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in statements:
        try:
            conn.execute(text(stmt + ';'))
        except Exception as e:
            print(f"⚠️ Error executing DDL statement: {e}\nStatement: {stmt[:200]}...")


def discover_staging_csvs():
    pattern = os.path.join(DATA_DIR, '**', 'stg_*.csv')
    return glob.glob(pattern, recursive=True)


def filename_to_table(fname):
    # Expect filenames like stg_stats_defense_2024-2025.csv -> table stg_stats_defense
    bn = os.path.basename(fname)
    m = re.match(r'^(?P<prefix>stg_[\w_\-]+)_(?P<season>\d{4}-\d{4})\.csv$', bn)
    if m:
        return m.group('prefix')
    # fallback: remove last underscore segment
    parts = bn.rsplit('_', 1)
    if len(parts) == 2:
        return parts[0]
    return os.path.splitext(bn)[0]


def count_file_rows(path):
    # Count non-empty lines excluding header
    rows = 0
    with open(path, 'rb') as fh:
        for i, line in enumerate(fh):
            if i == 0:
                continue
            if line.strip():
                rows += 1
    return rows


def load_csv_safe(engine, csv_path, table_name):
    """Load one CSV into a temporary load table, validate counts, then insert into final table.
    Returns a dict with stats and any error message.
    """
    stats = {"file": csv_path, "table": table_name, "start": time.time()}
    expected = count_file_rows(csv_path)
    stats['expected_rows'] = expected

    # read CSV into pandas (strings to be safe)
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    except Exception as e:
        stats['error'] = f"Could not read CSV: {e}"
        return stats

    loaded_rows = len(df)
    stats['read_rows'] = loaded_rows

    # Create a transient load table with a stable name
    load_tbl = f"load_{table_name}_{uuid.uuid4().hex[:8]}"
    full_load_tbl = f"{STAGING_SCHEMA}." + load_tbl
    final_tbl = f"{STAGING_SCHEMA}." + table_name

    with engine.begin() as conn:
        # Always create the transient load table from the CSV columns as TEXT.
        # This avoids COPY type conversion errors when final tables have stricter types.
        cols = df.columns.tolist()
        cols_defs = ', '.join([f'"{c}" TEXT' for c in cols])
        try:
            conn.execute(text(f'CREATE TABLE {full_load_tbl} ({cols_defs});'))
        except Exception as e:
            stats['error'] = f"Could not create load table from CSV headers: {e}"
            return stats

    # write to load table using COPY FROM STDIN for robust bulk loading
    try:
        # normalize NaN to empty strings for CSV
        df = df.where(pd.notna(df), '')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # We'll COPY only the columns present in the CSV; this lets the load table be LIKE final
        csv_cols = df.columns.tolist()
        col_list = ', '.join([f'"{c}"' for c in csv_cols])

        # Use a raw DBAPI connection for COPY
        raw_conn = engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            copy_sql = f'COPY {full_load_tbl} ({col_list}) FROM STDIN WITH CSV HEADER'
            cur.copy_expert(copy_sql, csv_buffer)
            raw_conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                raw_conn.close()
            except Exception:
                pass
    except Exception as e:
        stats['error'] = f"Could not write to load table using COPY: {e}"
        # attempt to drop the load table
        with engine.begin() as conn:
            try:
                conn.execute(text(f'DROP TABLE IF EXISTS {full_load_tbl}'))
            except Exception:
                pass
        return stats

    # validate counts in DB
    with engine.connect() as conn:
        res = conn.execute(text(f'SELECT COUNT(*) FROM {full_load_tbl}')).fetchone()
        db_loaded = int(res[0])
    stats['db_loaded_rows'] = db_loaded

    if db_loaded != expected and db_loaded != loaded_rows:
        stats['error'] = f"Row count mismatch: file expected {expected}, pd read {loaded_rows}, db loaded {db_loaded}"
        stats['keep_table'] = full_load_tbl
        return stats

    # insert into final table aligning columns
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table(table_name, schema=STAGING_SCHEMA):
            # final table doesn't exist: promote load table to final (rename)
            try:
                conn.execute(text(f'ALTER TABLE {full_load_tbl} RENAME TO {table_name};'))
                stats['inserted_rows'] = db_loaded
                return stats
            except Exception as e:
                stats['error'] = f"Could not promote load table to final: {e}"
                return stats

        # get final table columns and types
        cols_res = conn.execute(text(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
        ), {'s': STAGING_SCHEMA, 't': table_name}).fetchall()
        final_cols = [r[0] for r in cols_res]
        final_types = {r[0]: r[1] for r in cols_res}

        # get load table columns
        cols_res = conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
        ), {'s': STAGING_SCHEMA, 't': load_tbl}).fetchall()
        load_cols = [r[0] for r in cols_res]

        # build column list for insert: cast load text columns to final types where possible
        select_list = []
        for c in final_cols:
            if c in load_cols:
                ftype = final_types.get(c, '').lower()
                if ftype in ('integer', 'smallint', 'bigint'):
                    # Accept values like '23.0' by casting to numeric first, then integer
                    expr = f"NULLIF(\"{c}\", '')::numeric::integer"
                elif ftype in ('numeric', 'double precision', 'real', 'decimal'):
                    expr = f"NULLIF(\"{c}\", '')::numeric"
                elif 'timestamp' in ftype or ftype in ('date', 'time without time zone', 'time with time zone'):
                    expr = f"NULLIF(\"{c}\", '')::timestamp"
                elif ftype == 'boolean':
                    expr = f"NULLIF(\"{c}\", '')::boolean"
                else:
                    expr = f'"{c}"'
                select_list.append(expr + f' AS "{c}"')
            else:
                select_list.append('NULL AS "' + c + '"')

        quoted_final_cols = ', '.join([f'"{c}"' for c in final_cols])
        select_clause = ', '.join(select_list)
        insert_sql = f'INSERT INTO {final_tbl} ({quoted_final_cols}) SELECT {select_clause} FROM {full_load_tbl};'
        try:
            with engine.begin() as conn2:
                res = conn2.execute(text(insert_sql))
                stats['inserted_rows'] = db_loaded
        except Exception as e:
            stats['error'] = f"Could not insert into final table: {e}"
            stats['keep_table'] = full_load_tbl
            return stats

    # Cleanup load table
    with engine.begin() as conn:
        try:
            conn.execute(text(f'DROP TABLE IF EXISTS {full_load_tbl}'))
        except Exception:
            pass

    stats['duration_s'] = time.time() - stats['start']
    return stats


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Load staging CSVs into Postgres staging schema with validation')
    parser.add_argument('--apply-ddl', action='store_true', help='Apply generated staging DDL before loading')
    parser.add_argument('--limit', type=int, default=None, help='Only process N files')
    parser.add_argument('--offset', type=int, default=0, help='Start processing at this file index (for batching)')
    parser.add_argument('--batches', type=int, default=None, help='Split unique tables into N batches and process one batch')
    parser.add_argument('--batch-index', type=int, default=0, help='Zero-based index of the table-batch to process when --batches is used')
    parser.add_argument('--preview', action='store_true', help='Do not modify DB; just print plan')
    parser.add_argument('--yes', action='store_true', help='Confirm and perform DB writes (required to actually apply DDL or load files)')
    args = parser.parse_args()

    csvs = discover_staging_csvs()
    if not csvs:
        print('⚠️ No staging CSVs found under data/staging_data')
        return

    print(f'Found {len(csvs)} staging CSV(s)')

    # Build plan
    files_to_process = []

    # If user requested batching by unique table names, compute selected tables and pick files that belong to them.
    if args.batches is not None:
        if args.batches <= 0:
            print('⚠️ --batches must be >= 1')
            return
        # discover unique table names in stable order
        tables = []
        seen = set()
        for f in csvs:
            t = filename_to_table(f)
            if t not in seen:
                seen.add(t)
                tables.append(t)

        total_tables = len(tables)
        batch_count = args.batches
        if args.batch_index < 0 or args.batch_index >= batch_count:
            print(f'⚠️ --batch-index must be between 0 and {batch_count - 1}')
            return

        batch_size = math.ceil(total_tables / batch_count)
        start_table = args.batch_index * batch_size
        selected_tables = tables[start_table:start_table + batch_size]

        # select files whose table is in selected_tables (preserve original csv order)
        files_to_process = [f for f in csvs if filename_to_table(f) in selected_tables]
        plan_note = f'Processing table-batch {args.batch_index+1}/{batch_count} (tables {start_table}..{start_table+len(selected_tables)-1})'
    else:
        # apply offset and limit to produce the planned subset
        start = max(0, args.offset or 0)
        if args.limit == 0:
            files_to_process = []
        elif args.limit is None:
            files_to_process = csvs[start:]
        else:
            files_to_process = csvs[start:start + args.limit]
        plan_note = None

    plan = []
    if args.apply_ddl:
        plan.append('Apply staging DDL (will create/modify objects in schema "' + STAGING_SCHEMA + '")')
    if files_to_process:
        if plan_note:
            plan.append(plan_note)
        plan.append(f'Load {len(files_to_process)} file(s) into schema "{STAGING_SCHEMA}"')
    else:
        plan.append('No files will be processed (limit==0 or no files selected)')

    if args.preview:
        print('-- Preview mode: planned actions --')
        for p in plan:
            print('  -', p)
        if files_to_process:
            print('\nFiles that would be processed:')
            for f in files_to_process:
                print('  -', f, '->', filename_to_table(f))
        return

    # Safety: require explicit confirmation flag for any DB writes
    if not args.yes:
        print('⚠️ No --yes flag provided. The following actions are planned:')
        for p in plan:
            print('  -', p)
        print('\nTo proceed and perform DB writes, re-run with the --yes flag.')
        return

    # Apply DDL if requested
    if args.apply_ddl:
        with engine.begin() as conn:
            apply_staging_ddl(conn)
        print('Applied staging DDL')

        # If user passed --limit 0 we take that to mean "apply DDL only, do not load files".
        # This is handy for CI/ops where you want to create the schema but not ingest data.
        if args.limit == 0:
            print('Limit==0 detected: exiting after applying DDL (no files will be processed).')
            return

    processed = []
    # Use the planned subset (files_to_process) so --limit and --limit 0 are respected
    for i, csv in enumerate(files_to_process):
        table = filename_to_table(csv)
        print(f"Processing {csv} -> {STAGING_SCHEMA}.{table}")
        stats = load_csv_safe(engine, csv, table)
        processed.append(stats)
        if 'error' in stats:
            print(f"⚠️ Error loading {csv}: {stats['error']}")
            if 'keep_table' in stats:
                print(f"  Load table kept for inspection: {stats['keep_table']}")
        else:
            print(f"✅ Loaded {stats.get('inserted_rows',0)} rows into {STAGING_SCHEMA}.{table} in {stats.get('duration_s',0):.1f}s")

    # summary
    print('\nLoad summary:')
    for s in processed:
        print(s)


if __name__ == '__main__':
    main()
