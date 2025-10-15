import json
import os

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

    ddl_statements = []
    for table_name, columns in tables.items():
        column_defs = []
        for col_name, col_type in columns.items():
            norm = _normalize_col_type(col_type)
            sql_type = TYPE_MAPPING.get(norm, norm if isinstance(norm, str) else "TEXT")
            column_defs.append(f'    "{col_name}" {sql_type}')
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(column_defs) + "\n);\n"
        ddl_statements.append(ddl)

    full_ddl = "\n".join(ddl_statements)

    with open(DDL_OUTPUT_FILE, "w") as f:
        f.write(full_ddl)

    print(f"✅ DDL written to: {DDL_OUTPUT_FILE}")

if __name__ == "__main__":
    generate_ddl()
