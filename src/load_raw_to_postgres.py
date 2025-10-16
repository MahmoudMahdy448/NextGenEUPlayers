import os
import glob
import argparse
import pandas as pd
import io
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from sql_ident import make_table_name, make_sql_ident
import re

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

DEFAULT_DATA_DIR = "data/raw"


def valid_identifier(name: str) -> bool:
    # simple guard: only a-z0-9_ and not empty
    return bool(name) and all(c.isalnum() or c == "_" for c in name)


def load_raw_csvs(data_dir: str, schema: str, use_copy: bool, drop: bool, chunksize: int, preview: bool):
    csv_files = glob.glob(f"{data_dir}/**/*.csv", recursive=True)
    if not csv_files:
        print("⚠️ No CSV files found")
        return

    # ensure schema
    if not preview:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    for file_path in csv_files:
        base = os.path.splitext(os.path.basename(file_path))[0].lower()
        # detect season folder like 2025-2026 in the path and append to table name
        season_match = re.search(r"(\d{4}-\d{4})", file_path)
        if season_match:
            season = season_match.group(1)
            season_token = season.replace('-', '_')
            # if the base already ends with the same season token, strip it to avoid duplication
            if base.endswith(f"_{season_token}"):
                base = base[: - (len(season_token) + 1)]
            # also strip if base already contains the dash-style season (players_data-2023-2024)
            if base.endswith(f"-{season}"):
                base = base[: - (len(season) + 1)]
            base_with_season = f"{base}_{season_token}"
        else:
            base_with_season = base
        table_name = make_table_name(base_with_season)
        full_table = f"{schema}.{table_name}"

        if not valid_identifier(schema) or not valid_identifier(table_name):
            print(f"⚠️ Skipping {file_path}: invalid schema/table name {full_table}")
            continue

        print(f"📂 {file_path} -> {full_table}")

        if preview:
            print("-- preview mode: showing first 3 rows and column mapping")
            df_preview = pd.read_csv(file_path, dtype=str, nrows=3, keep_default_na=False)
            print(df_preview.head(3))
            continue

        # stream the CSV in chunks to avoid OOM
        reader = pd.read_csv(file_path, dtype=str, keep_default_na=False, chunksize=chunksize)

        first = True
        total = 0
        for chunk in reader:
            # sanitize columns
            raw_cols = [make_sql_ident(str(c).strip()) for c in chunk.columns]
            # make sanitized column names unique to avoid DuplicateColumnError
            seen = {}
            unique_cols = []
            for col in raw_cols:
                if col in seen:
                    seen[col] += 1
                    unique_name = f"{col}_{seen[col]-1}"
                else:
                    seen[col] = 1
                    unique_name = col
                unique_cols.append(unique_name)
            chunk.columns = unique_cols
            chunk = chunk.fillna("")

            if first:
                inspector = inspect(engine)
                table_exists = inspector.has_table(table_name, schema=schema)
                # create or replace if drop requested
                if drop and table_exists:
                    with engine.begin() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE;"))
                    table_exists = False

                # create empty table using the chunk header if it doesn't exist
                if not table_exists:
                    empty = chunk.head(0)
                    # create table with TEXT columns
                    empty.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
                first = False

            # insert chunk
            if use_copy:
                # attempt to use COPY via raw connection
                csv_buffer = chunk.to_csv(index=False, header=False)
                with engine.raw_connection() as raw_conn:
                    cur = raw_conn.cursor()
                    try:
                        cur.copy_expert(f"COPY {full_table} ({', '.join([f'"{c}"' for c in chunk.columns])}) FROM STDIN WITH (FORMAT csv)",
                                        io.StringIO(csv_buffer))
                        raw_conn.commit()
                    except Exception:
                        raw_conn.rollback()
                        # fallback to to_sql
                        chunk.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
            else:
                chunk.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)

            total += len(chunk)
            print(f"    inserted {total} rows...", end='\r')

        print(f"\n✅ Finished {file_path} -> {full_table} ({total} rows)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Improved raw CSV loader')
    parser.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    parser.add_argument('--schema', default='raw')
    parser.add_argument('--use-copy', action='store_true')
    parser.add_argument('--drop', action='store_true', help='Drop and recreate tables')
    parser.add_argument('--chunksize', type=int, default=10000)
    parser.add_argument('--preview', action='store_true')
    args = parser.parse_args()


    load_raw_csvs(args.data_dir, args.schema, args.use_copy, args.drop, args.chunksize, args.preview)
