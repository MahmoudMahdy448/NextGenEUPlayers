import os
import json
from glob import glob
from collections import defaultdict

import pandas as pd

RAW_FOLDER = "/workspaces/NextGenEUPlayers/data/raw"
SCHEMA_OUT = "/workspaces/NextGenEUPlayers/data/schemas"
os.makedirs(SCHEMA_OUT, exist_ok=True)
PROFILES_OUT = os.path.join(SCHEMA_OUT, "profiles")
os.makedirs(PROFILES_OUT, exist_ok=True)


def infer_type(series: pd.Series) -> str:
    """Return a coarse SQL-friendly type for a pandas Series."""
    # Use pandas type inference but fall back to safe TEXT
    try:
        dtype = pd.api.types.infer_dtype(series, skipna=True)
    except Exception:
        return "TEXT"

    if dtype in ["integer", "integer-na", "mixed-integer"]:
        return "BIGINT"
    if dtype in ["floating", "mixed-integer-float"]:
        return "DOUBLE PRECISION"
    if dtype in ["datetime", "date"]:
        return "TIMESTAMP"
    if dtype in ["boolean"]:
        return "BOOLEAN"
    # fallback
    return "TEXT"


def normalize_logical_name(path: str) -> str:
    """Map a raw file path to a logical dataset name (e.g., players_data, stats_shooting).

    We remove the season directory and any season suffix in the filename so multiple
    seasons aggregate to the same logical dataset.
    """
    base = os.path.basename(path)
    # remove season suffix like -2023-2024
    name = base
    if name.count("-") >= 2:
        # players_data-2023-2024.csv -> players_data
        name = "-".join(name.split("-")[:-2]) + ".csv"
    return os.path.splitext(name)[0]


def detect_schema():
    """Scan CSVs under data/raw and produce aggregated schema hints.

    Output written to data/schemas/detected_schemas.json
    Returns the detected schema dict.
    """
    csv_files = glob(os.path.join(RAW_FOLDER, "**", "*.csv"), recursive=True)
    print(f"Found {len(csv_files)} CSV files under {RAW_FOLDER}")

    # structure: {logical_name: {column_name: {types: {type: count}, seasons: set()}}}
    agg = defaultdict(lambda: defaultdict(lambda: {"types": defaultdict(int), "seasons": set()}))

    for file in sorted(csv_files):
        season = os.path.basename(os.path.dirname(file))
        logical = normalize_logical_name(file)
        print(f"Scanning: {file} -> logical dataset: {logical} (season {season})")
        try:
            df = pd.read_csv(file, low_memory=False)
        except Exception as e:
            print(f"  ⚠️  Failed to read {file}: {e}")
            continue

        for col in df.columns:
            typ = infer_type(df[col])
            agg[logical][col]["types"][typ] += 1
            agg[logical][col]["seasons"].add(season)

    # collapse to deterministic structure
    out = {}
    for logical, cols in agg.items():
        out[logical] = {}
        for col, meta in cols.items():
            # pick the most common inferred type
            types = meta["types"]
            most_common_type = max(types.items(), key=lambda kv: kv[1])[0]
            out[logical][col] = {
                "inferred_type": most_common_type,
                "type_distribution": dict(types),
                "seasons": sorted(list(meta["seasons"]))
            }

    out_path = os.path.join(SCHEMA_OUT, "detected_schemas.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    # brief summary
    total_tables = len(out)
    sample = next(iter(out.items())) if total_tables else (None, None)
    print(f"\n✅ Detected {total_tables} logical datasets. Example: {sample[0] if sample[0] else 'n/a'}")
    if sample[1]:
        print(f"Example has {len(sample[1])} columns. Sample columns: {list(sample[1].keys())[:5]}")

    print(f"Written detected schemas to: {out_path}")
    return out


def detect_profiles():
    """Per-season profiler: scan CSVs under data/raw, produce per-season profile JSONs

    Outputs written to data/schemas/profiles/
    Returns mapping of season -> list of files scanned.
    """
    csv_files = glob(os.path.join(RAW_FOLDER, "**", "*.csv"), recursive=True)
    print(f"Found {len(csv_files)} CSV files under {RAW_FOLDER}")

    # organize files by season
    files_by_season = defaultdict(list)
    for fpath in sorted(csv_files):
        season = os.path.basename(os.path.dirname(fpath))
        files_by_season[season].append(fpath)

    glossary_counts = defaultdict(int)
    profiles_summary = []

    for season, files in files_by_season.items():
        season_profile = {"season": season, "tables": {}}
        print(f"Profiling season {season} ({len(files)} files)")

        for file in files:
            logical = normalize_logical_name(file)
            print(f"  - Profiling {file} -> {logical}")
            try:
                df = pd.read_csv(file, low_memory=False)
            except Exception as e:
                print(f"    ⚠️  Failed to read {file}: {e}")
                season_profile["tables"][logical] = {"error": str(e)}
                continue

            table_info = {
                "file": file,
                "rows": int(len(df)),
                "columns": int(len(df.columns)),
                "column_info": {}
            }

            for col in df.columns:
                s = df[col]
                col_name = str(col)
                inferred = infer_type(s)
                nulls = int(s.isna().sum())
                uniques = int(s.dropna().nunique())
                sample_vals = s.dropna().astype(str).head(5).tolist()

                table_info["column_info"][col_name] = {
                    "inferred_type": inferred,
                    "null_count": nulls,
                    "unique_count": uniques,
                    "sample_values": sample_vals
                }

                glossary_counts[col_name] += 1

            season_profile["tables"][logical] = table_info
            profiles_summary.append({
                "season": season,
                "table": logical,
                "rows": table_info["rows"],
                "columns": table_info["columns"]
            })

        # write per-season profile
        out_path = os.path.join(PROFILES_OUT, f"per_season_{season}.initial_raw_data_profile.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(season_profile, f, indent=2, ensure_ascii=False)
        print(f"  -> Wrote season profile: {out_path}")

    # write glossary summary (columns across all seasons)
    glossary = {col: {"occurrences": cnt} for col, cnt in sorted(glossary_counts.items(), key=lambda kv: -kv[1])}
    glossary_path = os.path.join(PROFILES_OUT, "glossary.json")
    with open(glossary_path, "w", encoding="utf-8") as f:
        json.dump(glossary, f, indent=2, ensure_ascii=False)
    print(f"Wrote glossary summary to: {glossary_path}")

    # write CSV summary
    try:
        import csv
        summary_path = os.path.join(PROFILES_OUT, "profiles_summary.csv")
        with open(summary_path, "w", newline='', encoding='utf-8') as sf:
            writer = csv.DictWriter(sf, fieldnames=["season", "table", "rows", "columns"])
            writer.writeheader()
            for r in profiles_summary:
                writer.writerow(r)
        print(f"Wrote profiles summary CSV to: {summary_path}")
    except Exception:
        pass

    return files_by_season


if __name__ == "__main__":
    detect_profiles()
