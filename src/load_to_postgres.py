import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import json
import math
import time
from sql_ident import make_sql_ident, make_table_name, normalize_name

# 1️⃣ Load environment variables
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "db")  # "db" is the service name inside Docker
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

# 2️⃣ Create database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# 3️⃣ Define data path
DATA_DIR = "data/raw"

def detect_schema_and_load():
    """
    Iterate through all CSV files inside data/raw and load them to the database
    """
    # Only load the canonical players data files (skip stats_*, players_data_light, etc.)
    csv_files = glob.glob(f"{DATA_DIR}/**/players_data-*.csv", recursive=True)
    
    if not csv_files:
        print("⚠️ No CSV files found in data/raw/")
        return

    inspector = inspect(engine)
    # load detected schema (optional)
    schema_path = os.path.join('data', 'schemas', 'players_schema.json')
    players_schema = {}
    if os.path.exists(schema_path):
        try:
            with open(schema_path, 'r', encoding='utf-8') as fh:
                players_schema = json.load(fh)
        except Exception:
            players_schema = {}
    import argparse

    parser = argparse.ArgumentParser(description='Load players CSVs into Postgres with optional DDL preview/apply')
    parser.add_argument('--preview', action='store_true', help='Print sanitized column mapping and CREATE TABLE DDL but do not modify DB')
    parser.add_argument('--ddl-only', action='store_true', help='Apply CREATE TABLE / ALTER DDL but do not load data')
    parser.add_argument('--apply-ddl', action='store_true', help='Apply DDL then load data (default behavior if none specified)')
    parser.add_argument('--raw', action='store_true', help='Load data as-is: create table with TEXT columns and skip staging/coercion')
    parser.add_argument('--limit', type=int, default=None, help='Only process N files (for preview/testing)')
    args = parser.parse_args()

    for idx, file_path in enumerate(csv_files):
        if args.limit and idx >= args.limit:
            break
        file_path = file_path
        try:
            # Extract table name from filename
            # Use the CSV filename (without extension) as the table name.
            # Replace hyphens with underscores to be SQL-safe.
            base_name = os.path.splitext(os.path.basename(file_path))[0].lower()
            table_name = base_name.replace('-', '_')

            # determine season from filename when possible (players_data-YYYY-YYYY)
            season = None
            if base_name.startswith('players_data-'):
                season = base_name[len('players_data-'):]

            print(f"📂 Processing {file_path} → table: {table_name}")

            # Read file as strings to avoid accidental dtype inference
            # keep_default_na=False keeps empty fields as empty strings instead of NaN
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

            # Normalize and sanitize column names first (preserve index order)
            raw_cols = [str(c) if c is not None else '' for c in df.columns]
            raw_cols = [c.strip() for c in raw_cols]

            # use shared make_sql_ident from src.sql_ident

            # build mapping original -> safe unique column name
            mapping_file = os.path.join('data', 'schemas', 'players_column_map.json')
            canonical_map = {}
            try:
                if os.path.exists(mapping_file):
                    with open(mapping_file, 'r', encoding='utf-8') as mf:
                        canonical_map = json.load(mf) or {}
            except Exception:
                canonical_map = {}

            seen = {}
            col_map = {}
            sql_cols = []
            changed_mapping = False
            for i, raw in enumerate(raw_cols):
                base = raw if raw != '' else f'col_{i}'
                # prefer canonical mapping when available
                if base in canonical_map:
                    candidate = canonical_map[base]
                else:
                    candidate = make_sql_ident(base)
                # ensure uniqueness for this file (suffix if duplicate)
                if candidate in seen:
                    seen[candidate] += 1
                    suffix = f"_{seen[candidate]}"
                    candidate = (candidate[: 63 - len(suffix)]) + suffix
                else:
                    seen[candidate] = 0

                # if not in canonical_map, add it for future runs
                if base not in canonical_map or canonical_map.get(base) != candidate:
                    canonical_map[base] = candidate
                    changed_mapping = True

                col_map[raw] = candidate
                sql_cols.append(candidate)

            # rename df columns to safe SQL identifiers
            df.columns = sql_cols
            # inverse map sanitized -> original raw header
            inv_col_map = {v: k for k, v in col_map.items()}

            # persist mapping file if new headers were added
            if changed_mapping:
                try:
                    os.makedirs(os.path.dirname(mapping_file), exist_ok=True)
                    with open(mapping_file, 'w', encoding='utf-8') as mf:
                        json.dump(canonical_map, mf, ensure_ascii=False, indent=2)
                    print(f"ℹ️ Updated column mapping: {mapping_file}")
                except Exception:
                    pass

            # Drop obvious repeated header rows (some CSVs include header rows interleaved)
            cols = list(df.columns)
            def row_looks_like_header(row):
                # count how many cells match their column name (case-insensitive)
                matches = 0
                for i, col in enumerate(cols):
                    try:
                        cell = str(row.iloc[i]).strip().lower()
                    except Exception:
                        cell = ''
                    if cell == str(col).strip().lower():
                        matches += 1
                # consider it a header row if >=60% columns match
                return matches >= max(1, int(len(cols) * 0.6))

            if not df.empty:
                mask = df.apply(row_looks_like_header, axis=1)
                if mask.any():
                    df = df[~mask]

            # Also drop rows where player == 'Player' or matches == 'Matches' (legacy guard)
            lower_cols = [c.lower() for c in df.columns]
            if 'player' in lower_cols:
                pcol = df.columns[lower_cols.index('player')]
                df = df[df[pcol].astype(str).str.lower() != 'player']
            if 'matches' in lower_cols:
                mcol = df.columns[lower_cols.index('matches')]
                df = df[df[mcol].astype(str).str.lower() != 'matches']

            # At this point columns are sanitized and unique (safe for Postgres)

            # sanitize table name as SQL identifier
            table_name = make_table_name(table_name)

            # If this is a players_data-<season> file, map to players_<season>
            if base_name.startswith('players_data-') and season:
                table_name = make_table_name(f"players_{season.replace('-', '_')}")

            # Check if target table exists early so we can map columns to DB names
            table_exists = inspector.has_table(table_name)
            existing_cols = []
            if table_exists:
                try:
                    with engine.connect() as conn:
                        res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = :t"), {'t': table_name}).fetchall()
                        existing_cols = [r[0] for r in res]
                except Exception:
                    existing_cols = []

                # try to match sanitized columns to existing DB columns by a normalized name
                norm_existing = {normalize_name(c): c for c in existing_cols}
                rename_map = {}
                for c in df.columns:
                    raw_header = inv_col_map.get(c)
                    candidates = [raw_header, c]
                    matched = None
                    for cand in candidates:
                        if not cand:
                            continue
                        nk = normalize_name(cand)
                        if nk in norm_existing:
                            matched = norm_existing[nk]
                            break
                    if matched and matched != c:
                        rename_map[c] = matched

                if rename_map:
                    # rename DataFrame columns so they match DB column names
                    df = df.rename(columns=rename_map)

            # determine season from filename when possible (players_data-YYYY-YYYY)
            season = None
            if base_name.startswith('players_data-'):
                season = base_name[len('players_data-'):]

            # remove duplicated columns just in case (keep first occurrence)
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()]


            # fix age column like '24-358' -> 24 (we read as strings earlier)
            if 'age' in df.columns:
                df['age'] = df['age'].astype(str).str.split('-').str[0]
                # keep as string for raw ingest; but coerce malformed to empty
                df['age'] = df['age'].where(df['age'].str.match(r"^\d+$"), '')

            # Ensure no column name is empty (final guard)
            final_cols = []
            for i, c in enumerate(df.columns):
                if c is None or str(c).strip() == '':
                    final_cols.append(f'col_{i}')
                else:
                    final_cols.append(str(c))
            df.columns = final_cols

            # We'll first compute desired typed columns based on players_schema
            # and sample coercion. Do not force everything to string yet.
            df = df.fillna("")

            desired_types = {}  # col -> 'INTEGER'|'FLOAT'
            # Only compute desired types when we have a players_schema available
            if players_schema:
                inv_col_map = {v: k for k, v in col_map.items()} if 'col_map' in locals() else {}
                for col in df.columns:
                    orig = inv_col_map.get(col, col)
                    norm_orig = normalize_name(orig)
                    matched_key = None
                    for key, meta in players_schema.items():
                        if season and 'seasons' in meta and season not in meta['seasons']:
                            continue
                        if normalize_name(key) == norm_orig or normalize_name(key) == normalize_name(col):
                            matched_key = key
                            break
                    if not matched_key:
                        continue
                    if players_schema[matched_key].get('type','').upper() != 'NUMERIC':
                        continue
                    coerced = pd.to_numeric(df[col].astype(str).str.replace(',', '' ).replace('', pd.NA), errors='coerce')
                    total = len(coerced)
                    non_null = int(coerced.notna().sum())
                    success_rate = 0 if total == 0 else non_null / total
                    if total > 0 and success_rate >= 0.98:
                        # decide int vs float
                        if (coerced.dropna() % 1 == 0).all():
                            desired_types[col] = 'INTEGER'
                        else:
                            desired_types[col] = 'FLOAT'

            # Coerce df columns for numeric desired types so to_sql will send proper Python numbers
            for col, t in desired_types.items():
                coerced = pd.to_numeric(df[col].astype(str).str.replace(',', '' ).replace('', pd.NA), errors='coerce')
                if t == 'INTEGER':
                    # convert to Python int or None
                    df[col] = coerced.where(coerced.notna(), None).apply(lambda x: int(x) if x is not None and float(x).is_integer() else (None if x is None else float(x)))
                else:
                    df[col] = coerced.where(coerced.notna(), None).astype(float)

            # Force remaining columns to string/object so to_sql creates TEXT columns for them
            other_cols = [c for c in df.columns if c not in desired_types]
            if other_cols:
                df[other_cols] = df[other_cols].astype(str)

            # Load data to PostgreSQL
            # Build CREATE TABLE statement using detected schema when available
            cols_defs = []
            final_col_types = {}
            for col in df.columns:
                # prefer desired_types, then players_schema guidance, else TEXT
                if col in desired_types:
                    sql_type = desired_types[col]
                else:
                    sql_type = 'TEXT'
                    orig = col_map.get(col, col) if 'col_map' in locals() else col
                    norm_orig = ''.join([c.lower() for c in str(orig) if c.isalnum()])
                    matched = None
                    for key, meta in players_schema.items():
                        if season and 'seasons' in meta and season not in meta['seasons']:
                            continue
                        if ''.join([c.lower() for c in str(key) if c.isalnum()]) == norm_orig or ''.join([c.lower() for c in str(key) if c.isalnum()]) == ''.join([c.lower() for c in str(col) if c.isalnum()]):
                            matched = meta
                            break
                    if matched and matched.get('type','').upper() == 'NUMERIC' and col not in desired_types:
                        sql_type = 'NUMERIC'
                cols_defs.append(f'"{col}" {sql_type}')
                final_col_types[col] = sql_type
            create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_defs)});'

            # If preview mode: print mapping and DDL and skip DB changes
            if args.preview:
                print('\n-- Preview: column mapping (original -> sanitized)')
                for orig_raw, safe in col_map.items():
                    print(f'{orig_raw!r} -> {safe}')
                print('\n-- Preview: CREATE TABLE DDL')
                print(create_sql)
                print('\n')
                # don't apply DDL or load when previewing
                if not args.apply_ddl:
                    # continue to next file
                    continue

            # Ensure table exists. Prefer creating table with types from players_schema
            created_table = False
            table_exists = inspector.has_table(table_name)
            # If raw mode requested, we want to create the table with all TEXT columns
            raw_create = False
            if getattr(args, 'raw', False):
                # build a simple TEXT-only create statement
                cols_text = [f'"{col}" TEXT' for col in df.columns]
                create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_text)});'
                raw_create = True
                # if table exists and user asked to apply ddl, recreate it as TEXT
                if table_exists and (args.ddl_only or args.apply_ddl):
                    try:
                        with engine.begin() as conn:
                            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                        table_exists = False
                    except Exception:
                        pass
            if not table_exists:
                # build CREATE TABLE statement using detected schema when available
                cols_defs = []
                final_col_types = {}
                # If we're in raw mode and already prepared a TEXT-only create_sql, skip
                if raw_create:
                    # create_sql was already set above to a TEXT-only table
                    pass
                else:
                    for col in df.columns:
                        # prefer desired_types, then players_schema guidance, else TEXT
                        if col in desired_types:
                            sql_type = desired_types[col]
                        else:
                            sql_type = 'TEXT'
                            orig = col_map.get(col, col) if 'col_map' in locals() else col
                            norm_orig = ''.join([c.lower() for c in str(orig) if c.isalnum()])
                            matched = None
                            for key, meta in players_schema.items():
                                if season and 'seasons' in meta and season not in meta['seasons']:
                                    continue
                                if ''.join([c.lower() for c in str(key) if c.isalnum()]) == norm_orig or ''.join([c.lower() for c in str(key) if c.isalnum()]) == ''.join([c.lower() for c in str(col) if c.isalnum()]):
                                    matched = meta
                                    break
                            if matched and matched.get('type','').upper() == 'NUMERIC' and col not in desired_types:
                                sql_type = 'NUMERIC'
                        cols_defs.append(f'"{col}" {sql_type}')
                        final_col_types[col] = sql_type
                    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_defs)});'
                try:
                    if args.ddl_only or args.apply_ddl:
                        # use a transaction context so DDL is committed and visible
                        with engine.begin() as conn:
                            conn.execute(text(create_sql))
                            if raw_create:
                                print(f"Created table {table_name} as TEXT-only (raw mode)")
                            else:
                                print(f"Created table {table_name} with schema-driven types")
                            created_table = True
                            table_exists = True
                    else:
                        # Do NOT create/replace tables by default. Skip file and warn the user.
                        print(f"⚠️ Table {table_name} does not exist. Skipping file (pass --apply-ddl to create tables)")
                        # continue to next file
                        continue
                except Exception as err:
                    print(f"⚠️ Could not create table with DDL: {err}; skipping file")
                    continue
                # refresh inspector
                inspector = inspect(engine)

            # ------------------
            # Apply desired_types (computed earlier) to existing tables.
            # Run each ALTER in its own connection so failures don't abort the
            # whole DDL sequence. Only run ALTERs when applying DDL explicitly.
            if (table_name.startswith('players_data') and desired_types) and (args.ddl_only or args.apply_ddl):
                # fetch existing columns once
                with engine.connect() as conn:
                    res = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name = :t"), {'t': table_name}).fetchall()
                    existing_cols = {r[0]: r[1] for r in res}

                    # If we created the table with CREATE TABLE and that CREATE already
                    # encoded desired_types, skip ALTERs to avoid race/visibility issues.
                    if created_table:
                        # refresh existing_cols now that table was created
                        with engine.connect() as conn:
                            res = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name = :t"), {'t': table_name}).fetchall()
                            existing_cols = {r[0]: r[1] for r in res}
                        print(f"Table {table_name} was just created with schema-driven types; skipping ALTERs.")
                    else:
                        for col, sql_type in desired_types.items():
                            current_type = existing_cols.get(col)
                            # map our types to information_schema equivalents
                            target_type_lower = 'integer' if sql_type == 'INTEGER' else 'double precision'
                            if current_type and target_type_lower in current_type:
                                continue
                            try:
                                # If we're only previewing DDL, print ALTER statements instead of executing
                                # Cast the existing column to text before replacing commas so regexp/replace
                                # functions don't fail when the current column is numeric.
                                using_expr = f"NULLIF(replace(CAST(\"{col}\" AS text), ',', ''), '')::numeric"
                                alter_sql = f'ALTER TABLE "{table_name}" ALTER COLUMN "{col}" TYPE {sql_type} USING {using_expr};'
                                if args.preview:
                                    print('-- Preview ALTER:')
                                    print(alter_sql)
                                    continue
                                if args.ddl_only or args.apply_ddl:
                                    # ensure each DDL runs in its own transaction so it's committed
                                    with engine.begin() as conn2:
                                        conn2.execute(text(alter_sql))
                                        print(f"Altered column {col} to {sql_type} in {table_name}")
                            except Exception as err:
                                print(f"⚠️ Could not alter column {col} in {table_name}: {err}")

            # Add any missing columns to existing table as TEXT (only if table exists and applying DDL)
            if table_exists and (args.ddl_only or args.apply_ddl) and not getattr(args, 'raw', False):
                res = engine.connect().execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = :t"), {'t': table_name}).fetchall()
                existing_cols = [r[0] for r in res]
                missing = [c for c in df.columns if c not in existing_cols]
                if missing:
                    for col in missing:
                        try:
                            sql = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT')
                            with engine.begin() as conn:
                                conn.execute(sql)
                            print(f"Added missing column {col} to {table_name}")
                        except Exception:
                            # if adding fails, continue; to_sql append may still work if column exists differently
                            pass

            # If ddl-only was requested, skip appending data and staging
            if args.ddl_only and not args.apply_ddl:
                print(f"-- DDL-only mode: skipping data load for {table_name}\n")
                continue

            # Append data
            # If raw mode is requested, skip coercion and insert columns as TEXT as-is
            if getattr(args, 'raw', False):
                # ensure df columns are strings (TEXT); do not replace empty strings
                df = df.astype(str)
            else:
                # Before inserting, coerce DataFrame columns to match final SQL types
                # so empty strings become NULL and numeric inserts do not fail.
                for col, sql_type in (final_col_types.items() if 'final_col_types' in locals() else []):
                    st = str(sql_type).upper()
                    if st in ('INTEGER', 'INT'):
                        coerced = pd.to_numeric(df[col].astype(str).str.replace(',', '' ).replace('', pd.NA), errors='coerce')
                        # convert to Python ints where possible, else None
                        df[col] = coerced.where(coerced.notna(), None).apply(lambda x: int(x) if x is not None and float(x).is_integer() else (None if x is None else float(x)))
                    elif st in ('FLOAT', 'DOUBLE PRECISION', 'NUMERIC', 'REAL', 'DECIMAL'):
                        coerced = pd.to_numeric(df[col].astype(str).str.replace(',', '' ).replace('', pd.NA), errors='coerce')
                        df[col] = coerced.where(coerced.notna(), None).astype(float)
                    else:
                        # leave as string (TEXT)
                        df[col] = df[col].astype(str)

            # insert with progress in chunks so we can monitor
            def insert_with_progress(df_obj, table, engine_obj, if_exists='append', index=False, chunksize=1000):
                total = len(df_obj)
                if total == 0:
                    print('    (no rows to insert)')
                    return
                inserted = 0
                start_time = time.time()
                for start in range(0, total, chunksize):
                    end = min(start + chunksize, total)
                    chunk = df_obj.iloc[start:end]
                    # for first chunk with if_exists='append' is fine; others append as well
                    chunk.to_sql(table, engine_obj, if_exists=if_exists, index=index)
                    inserted = end
                    pct = int((inserted / total) * 100)
                    elapsed = time.time() - start_time
                    # simple ETA estimate
                    eta = (elapsed / inserted) * (total - inserted) if inserted and total > inserted else 0
                    print(f"    {pct}% ({inserted}/{total}) - ETA {eta:.1f}s", end='\r', flush=True)
                # final newline after loop
                print()

            print(f"⏳ Inserting into {table_name} ({len(df)} rows) ...")
            # Replace empty strings with NULL so numeric columns receive NULL instead of '' unless raw mode
            if not getattr(args, 'raw', False):
                df = df.replace('', None)
            insert_with_progress(df, table_name, engine, if_exists='append', index=False, chunksize=1000)

            # If raw mode, skip creating staging and any further coercion/ALTERs
            if getattr(args, 'raw', False):
                print(f"✅ Successfully loaded {len(df)} rows into table {table_name} (raw mode)\n")
                continue

            # --- create a typed staging table (safer than altering landing tables) ---
            stg_table = f"stg_{table_name}"
            # build CREATE TABLE for staging using desired_types and players_schema
            stg_cols = []
            for col in df.columns:
                if col in desired_types:
                    stype = desired_types[col]
                else:
                    # consult players_schema
                    stype = 'TEXT'
                    orig = col_map.get(col, col) if 'col_map' in locals() else col
                    norm_orig = ''.join([c.lower() for c in str(orig) if c.isalnum()])
                    for key, meta in players_schema.items():
                        if season and 'seasons' in meta and season not in meta['seasons']:
                            continue
                        if ''.join([c.lower() for c in str(key) if c.isalnum()]) == norm_orig or ''.join([c.lower() for c in str(key) if c.isalnum()]) == ''.join([c.lower() for c in str(col) if c.isalnum()]):
                            if meta.get('type','').upper() == 'NUMERIC':
                                stype = 'NUMERIC'
                            break
                stg_cols.append(f'"{col}" {stype}')
            stg_create = f'DROP TABLE IF EXISTS "{stg_table}"; CREATE TABLE "{stg_table}" ({", ".join(stg_cols)});'
            try:
                with engine.begin() as conn:
                    conn.execute(text(stg_create))
                    # insert into staging
                    # insert staging table with progress
                    print(f"⏳ Creating and populating staging table {stg_table} ({len(df)} rows) ...")
                    insert_with_progress(df, stg_table, engine, if_exists='append', index=False, chunksize=1000)
                    print(f"Created and populated staging table {stg_table}")
            except Exception as err:
                print(f"⚠️ Could not create/populate staging table {stg_table}: {err}")

            print(f"✅ Successfully loaded {len(df)} rows into table {table_name}\n")

        except Exception as e:
            import traceback
            print(f"❌ Error while loading {file_path}: {e}\n")
            print(traceback.format_exc())

if __name__ == "__main__":
    print("🚀 Starting data loading process to PostgreSQL...")
    detect_schema_and_load()
    print("🎉 Data loading process completed successfully!")
