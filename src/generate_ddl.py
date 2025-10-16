import json
import os
import glob
import re
from sql_ident import make_sql_ident, make_table_name

SCHEMA_FILE = "data/schemas/players_schema.json"
DDL_OUTPUT_FILE = "data/schemas/create_tables.sql"

# Mapping for a few common type names to SQL types. We include both
# pandas dtype names and some canonical type names that may already
# be present in the generated schema (e.g. NUMERIC, TEXT).
TYPE_MAPPING = {
    "int64": "INTEGER",
    "float64": "FLOAT",
    "object": "TEXT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    # canonical names that may already be present in the schema
    "NUMERIC": "NUMERIC",
    "TEXT": "TEXT",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
}

def generate_ddl():
    if not os.path.exists(SCHEMA_FILE):
        raise FileNotFoundError(f"Schema file not found at {SCHEMA_FILE}")

    with open(SCHEMA_FILE, "r") as f:
        schema = json.load(f)

    os.makedirs(os.path.dirname(DDL_OUTPUT_FILE), exist_ok=True)

    # Normalize schema shapes. There are two supported shapes:
    # 1) Single-table schema where top-level keys are column names and
    #    values are objects containing a "type" field. Example:
    #    { "col1": {"type": "NUMERIC", ...}, ... }
    # 2) Multi-table schema where top-level keys are table names and
    #    values are dicts mapping column -> type (or column -> {"type":...}).
    if isinstance(schema, dict) and all(
        isinstance(v, dict) and "type" in v for v in schema.values()
    ):
        # single table
        base = os.path.splitext(os.path.basename(SCHEMA_FILE))[0]
        if base.endswith("_schema"):
            table_name = base[: -len("_schema")]
        else:
            table_name = base
        tables = {table_name: {col: info["type"] for col, info in schema.items()}}
    else:
        # assume schema maps table -> columns. Normalize inner shapes so that
        # each column maps to a primitive type string when possible.
        tables = {}
        for tname, cols in schema.items():
            if not isinstance(cols, dict):
                continue
            normalized = {}
            for col, info in cols.items():
                if isinstance(info, dict) and "type" in info:
                    normalized[col] = info["type"]
                else:
                    normalized[col] = info
            tables[tname] = normalized

    def _normalize_col_type(col_type):
        # Accept strings, dicts and lists. Return an uppercase type-name string.
        if isinstance(col_type, str):
            return col_type.upper()
        if isinstance(col_type, dict):
            return _normalize_col_type(col_type.get("type"))
        if isinstance(col_type, list):
            if len(col_type) == 0:
                return "TEXT"
            # pick first element and normalize it
            return _normalize_col_type(col_type[0])
        return "TEXT"

    # Discover seasons by scanning data/raw/**/players_data-<season>.csv
    csv_paths = glob.glob(os.path.join("data", "raw", "**", "players_data-*.csv"), recursive=True)
    seasons = []
    season_re = re.compile(r"players_data-(?P<season>\d{4}-\d{4})\.csv$")
    for p in csv_paths:
        m = season_re.search(os.path.basename(p))
        if m:
            seasons.append(m.group('season'))
    seasons = sorted(set(seasons))

    ddl_statements = []

    column_map = {}  # original -> sanitized (global across seasons)
    if seasons:
        # Generate a per-season players table
        for season in seasons:
            safe_season = season.replace('-', '_')
            table_name = make_table_name(f"players_{safe_season}")
            column_defs = []
            # When schema is the single-table form (columns -> {type,seasons,...})
            if isinstance(schema, dict) and all(isinstance(v, dict) and 'type' in v for v in schema.values()):
                for col_name, meta in schema.items():
                    # If the meta has seasons, only use the type if this season is listed
                    if 'seasons' in meta:
                        if season not in meta['seasons']:
                            # include column but default to TEXT when season not listed
                            chosen = 'TEXT'
                        else:
                            chosen = meta.get('type', 'TEXT')
                    else:
                        chosen = meta.get('type', 'TEXT')
                    norm = _normalize_col_type(chosen)
                    sql_type = TYPE_MAPPING.get(norm, norm if isinstance(norm, str) else 'TEXT')
                    sanitized = make_sql_ident(col_name)
                    # ensure uniqueness of sanitized names within this table
                    if sanitized in [c.split()[-2].strip('"') for c in column_defs]:
                        # count existing occurrences and append suffix
                        suffix = 1
                        base = sanitized
                        while f"{base}_{suffix}" in [c.split()[-2].strip('"') for c in column_defs]:
                            suffix += 1
                        sanitized = f"{base}_{suffix}"
                    column_defs.append(f'    "{sanitized}" {sql_type}')
                    column_map[col_name] = sanitized
            else:
                # Fallback: use normalized tables mapping (table -> cols)
                cols = tables.get('players') or next(iter(tables.values()))
                for col_name, col_type in cols.items():
                    norm = _normalize_col_type(col_type)
                    sql_type = TYPE_MAPPING.get(norm, norm if isinstance(norm, str) else 'TEXT')
                    sanitized = make_sql_ident(col_name)
                    # ensure uniqueness within this table
                    if sanitized in [c.split()[-2].strip('"') for c in column_defs]:
                        suffix = 1
                        base = sanitized
                        while f"{base}_{suffix}" in [c.split()[-2].strip('"') for c in column_defs]:
                            suffix += 1
                        sanitized = f"{base}_{suffix}"
                    column_defs.append(f'    "{sanitized}" {sql_type}')
                    column_map[col_name] = sanitized

            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(column_defs) + "\n);\n"
            ddl_statements.append(ddl)
    else:
        # No season CSVs found — fallback to previous behavior (tables dict)
        for table_name, columns in tables.items():
            column_defs = []
            for col_name, col_type in columns.items():
                norm = _normalize_col_type(col_type)
                sql_type = TYPE_MAPPING.get(norm, norm if isinstance(norm, str) else "TEXT")
                sanitized = make_sql_ident(col_name)
                column_defs.append(f'    "{sanitized}" {sql_type}')
                column_map[col_name] = sanitized
            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(column_defs) + "\n);\n"
            ddl_statements.append(ddl)

    full_ddl = "\n".join(ddl_statements)

    with open(DDL_OUTPUT_FILE, "w") as f:
        f.write(full_ddl)

    # write mapping file for auditability: original -> sanitized
    mapping_file = os.path.join(os.path.dirname(DDL_OUTPUT_FILE), 'players_column_map.json')
    try:
        with open(mapping_file, 'w', encoding='utf-8') as mf:
            json.dump(column_map, mf, ensure_ascii=False, indent=2)
        print(f"ℹ️ Column mapping written to: {mapping_file}")
    except Exception:
        pass

    print(f"✅ DDL written to: {DDL_OUTPUT_FILE}")

if __name__ == "__main__":
    generate_ddl()
