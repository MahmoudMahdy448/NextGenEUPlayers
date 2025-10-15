import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import json

# 1️⃣ تحميل متغيرات البيئة
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "db")  # "db" هو اسم الخدمة داخل Docker
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

# 2️⃣ إنشاء الاتصال بالداتابيز
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# 3️⃣ تحديد مسار البيانات
DATA_DIR = "data/raw"

def detect_schema_and_load():
    """
    تمر على كل ملفات CSV داخل data/raw وتحملها للداتابيز
    """
    # Only load the canonical players data files (skip stats_*, players_data_light, etc.)
    csv_files = glob.glob(f"{DATA_DIR}/**/players_data-*.csv", recursive=True)
    
    if not csv_files:
        print("⚠️ لم يتم العثور على أي ملفات CSV في data/raw/")
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
    parser.add_argument('--limit', type=int, default=None, help='Only process N files (for preview/testing)')
    args = parser.parse_args()

    for idx, file_path in enumerate(csv_files):
        if args.limit and idx >= args.limit:
            break
        file_path = file_path
        try:
            # استخراج اسم الجدول من اسم الملف
            # Use the CSV filename (without extension) as the table name.
            # Replace hyphens with underscores to be SQL-safe.
            base_name = os.path.splitext(os.path.basename(file_path))[0].lower()
            table_name = base_name.replace('-', '_')

            print(f"📂 Processing {file_path} → table: {table_name}")

            # قراءة الملف as strings to avoid accidental dtype inference
            # keep_default_na=False keeps empty fields as empty strings instead of NaN
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

            # Normalize and sanitize column names first (preserve index order)
            raw_cols = [str(c) if c is not None else '' for c in df.columns]
            raw_cols = [c.strip() for c in raw_cols]

            # function to make a safe SQL identifier (Postgres max 63 chars)
            def make_sql_ident(name, max_len=63):
                # lowercase, replace spaces with underscore, remove bad chars
                nm = name.lower().replace(' ', '_')
                nm = pd.Series([nm]).str.replace(r'[^a-z0-9_]', '', regex=True).iloc[0]
                if nm == '' or nm[0].isdigit():
                    nm = f'col_{nm}' if nm != '' else 'col'
                # truncate and reserve room for suffixes
                if len(nm) > max_len:
                    nm = nm[: max_len - 4]
                return nm

            # build mapping original -> safe unique column name
            seen = {}
            col_map = {}
            sql_cols = []
            for i, raw in enumerate(raw_cols):
                base = raw if raw != '' else f'col_{i}'
                candidate = make_sql_ident(base)
                if candidate in seen:
                    seen[candidate] += 1
                    suffix = f"_{seen[candidate]}"
                    # ensure length limit
                    candidate = (candidate[: 63 - len(suffix)]) + suffix
                else:
                    seen[candidate] = 0
                col_map[raw] = candidate
                sql_cols.append(candidate)

            # rename df columns to safe SQL identifiers
            df.columns = sql_cols

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
            def make_table_name(orig):
                t = orig.lower()
                t = t.replace('-', '_')
                t = pd.Series([t]).str.replace(r'[^a-z0-9_]', '', regex=True).iloc[0]
                if t == '' or t[0].isdigit():
                    t = f't_{t}' if t != '' else 't'
                if len(t) > 63:
                    t = t[:63]
                return t

            table_name = make_table_name(table_name)

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
            if table_name.startswith('players_data') and players_schema:
                # reuse normalize_name from above
                def normalize_name(s: str) -> str:
                    return ''.join([c.lower() for c in str(s) if c.isalnum()])
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

            # تحميل البيانات إلى PostgreSQL
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
            if not inspector.has_table(table_name):
                # build CREATE TABLE statement using detected schema when available
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
                try:
                    if args.ddl_only or args.apply_ddl:
                        # use a transaction context so DDL is committed and visible
                        with engine.begin() as conn:
                            conn.execute(text(create_sql))
                            print(f"Created table {table_name} with schema-driven types")
                            created_table = True
                    else:
                        # default path: create fallback via pandas to_sql to ensure table exists
                        df_head = df.head(0).astype(str)
                        df_head.to_sql(table_name, engine, if_exists='replace', index=False)
                        print(f"Created table {table_name} as TEXT fallback (no --apply-ddl)")
                except Exception as err:
                    print(f"⚠️ Could not create table with DDL: {err}; falling back to TEXT table via pandas")
                    df_head = df.head(0).astype(str)
                    df_head.to_sql(table_name, engine, if_exists='replace', index=False)
                # refresh inspector
                inspector = inspect(engine)

            # ------------------
            # Apply desired_types (computed earlier) to existing tables.
            # Run each ALTER in its own connection so failures don't abort the
            # whole DDL sequence.
            if table_name.startswith('players_data') and desired_types:
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

            # Add any missing columns to existing table as TEXT
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

            df.to_sql(
                table_name,
                engine,
                if_exists="append",
                index=False,
                chunksize=1000
            )

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
                    df.to_sql(stg_table, engine, if_exists='append', index=False, chunksize=1000)
                    print(f"Created and populated staging table {stg_table}")
            except Exception as err:
                print(f"⚠️ Could not create/populate staging table {stg_table}: {err}")

            print(f"✅ تم تحميل {len(df)} صف إلى الجدول {table_name}\n")

        except Exception as e:
            import traceback
            print(f"❌ خطأ أثناء تحميل {file_path}: {e}\n")
            print(traceback.format_exc())

if __name__ == "__main__":
    print("🚀 بدء عملية تحميل البيانات إلى PostgreSQL ...")
    detect_schema_and_load()
    print("🎉 انتهت عملية التحميل بنجاح!")
