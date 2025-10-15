import os
import pandas as pd
import json
from glob import glob

RAW_FOLDER = "/workspaces/NextGenEUPlayers/data/raw"
SCHEMA_OUT = "/workspaces/NextGenEUPlayers/data/schemas"
os.makedirs(SCHEMA_OUT, exist_ok=True)

def infer_type(series: pd.Series) -> str:
    dtype = pd.api.types.infer_dtype(series, skipna=True)
    if dtype in ["integer", "floating"]:
        return "NUMERIC"
    elif dtype in ["datetime", "date"]:
        return "TIMESTAMP"
    elif dtype in ["boolean"]:
        return "BOOLEAN"
    else:
        return "TEXT"

def detect_schema():
    schema = {}
    csv_files = glob(os.path.join(RAW_FOLDER, "*", "players_data-*.csv"))
    print(f"Found {len(csv_files)} season CSVs")

    for file in csv_files:
        season = os.path.basename(os.path.dirname(file))
        print(f"Analyzing {file} (season {season})")
        df = pd.read_csv(file, low_memory=False)

        for col in df.columns:
            inferred = infer_type(df[col])
            if col not in schema:
                schema[col] = {"type": inferred, "seasons": [season]}
            else:
                if season not in schema[col]["seasons"]:
                    schema[col]["seasons"].append(season)

    schema_path = os.path.join(SCHEMA_OUT, "players_schema.json")
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=4, ensure_ascii=False)

    print(f"\n✅ Schema saved to: {schema_path}")
    return schema

if __name__ == "__main__":
    detect_schema()
