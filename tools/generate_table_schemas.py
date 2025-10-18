#!/usr/bin/env python3
"""Generate per-logical-table JSON schema files for raw and staged data.

Produces:
- data/schemas/raw_schemas/<table>.json  (merged from per-season raw profiles)
- data/schemas/staged_schemas/<table>.json (merged from staging schema jsons under data/staging_data/<season>/)

This is intentionally conservative: it records observed inferred types, null counts and sample values from raw profiles,
and records declared types from staged schema JSONs.
"""
import json
import os
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path.cwd()
PROFILES_DIR = ROOT / "data" / "schemas" / "profiles" / "raw"
RAW_OUT = ROOT / "data" / "schemas" / "raw_schemas"
STAGED_DIR = ROOT / "data" / "staging_data"
STAGED_OUT = ROOT / "data" / "schemas" / "staged_schemas"

RAW_OUT.mkdir(parents=True, exist_ok=True)
STAGED_OUT.mkdir(parents=True, exist_ok=True)


def normalize_type(t: str) -> str:
    if t is None:
        return "string"
    t = t.upper()
    if "INT" in t or t in ("BIGINT", "INTEGER"):
        return "integer"
    if "DOUBLE" in t or "DECIMAL" in t or "NUM" in t or t in ("FLOAT", "REAL"):
        return "number"
    return "string"


def build_raw_schemas():
    # gather per-logical table column stats across seasons
    agg = defaultdict(lambda: defaultdict(lambda: {"types": Counter(), "null_count": 0, "samples": Counter(), "occurrences": 0}))

    json_files = sorted(PROFILES_DIR.glob("per_season_*.initial_raw_data_profile.json"))
    for jf in json_files:
        with jf.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        season = data.get("season")
        tables = data.get("tables", {})
        for logical, tbl in tables.items():
            colinfo = tbl.get("column_info") or {}
            for col, meta in colinfo.items():
                inf = meta.get("inferred_type")
                nulls = int(meta.get("null_count", 0) or 0)
                samples = meta.get("sample_values", [])
                entry = agg[logical][col]
                entry["types"][inf] += 1
                entry["null_count"] += nulls
                for s in samples:
                    entry["samples"][str(s)] += 1
                entry["occurrences"] += 1

    # write per-table JSON
    for logical, cols in agg.items():
        out = {"table": logical, "columns": {}}
        for col, meta in sorted(cols.items()):
            # pick most common inferred_type
            most = meta["types"].most_common(1)
            chosen = most[0][0] if most else None
            out["columns"][col] = {
                "preferred_type": normalize_type(chosen),
                "raw_inferred_types": dict(meta["types"]),
                "total_nulls": int(meta["null_count"]),
                "occurrences_in_seasons": int(meta["occurrences"]),
                "top_sample_values": [v for v, _ in meta["samples"].most_common(5)]
            }

        out_path = RAW_OUT / f"{logical}.json"
        with out_path.open("w", encoding="utf-8") as of:
            json.dump(out, of, indent=2, ensure_ascii=False)
        print(f"Wrote raw schema: {out_path}")


def build_staged_schemas():
    # look for *_schema.json files in staging directories
    seasons = [p for p in STAGED_DIR.iterdir() if p.is_dir()]
    agg = defaultdict(lambda: defaultdict(lambda: {"types": Counter(), "seasons": []}))

    for season_dir in seasons:
        for schema_file in season_dir.glob("*_schema.json"):
            table = schema_file.name.replace("_schema.json", "")
            try:
                with schema_file.open("r", encoding="utf-8") as fh:
                    sdata = json.load(fh)
            except Exception as e:
                print(f"  ⚠️ Skipping {schema_file}: {e}")
                continue
            for col, typ in sdata.items():
                agg[table][col]["types"][typ] += 1
                if season_dir.name not in agg[table][col]["seasons"]:
                    agg[table][col]["seasons"].append(season_dir.name)

    for table, cols in agg.items():
        out = {"table": table, "columns": {}}
        for col, meta in sorted(cols.items()):
            out["columns"][col] = {
                "declared_types": dict(meta["types"]),
                "seasons_present": sorted(meta["seasons"]) 
            }
        out_path = STAGED_OUT / f"{table}.json"
        with out_path.open("w", encoding="utf-8") as of:
            json.dump(out, of, indent=2, ensure_ascii=False)
        print(f"Wrote staged schema: {out_path}")


def main():
    build_raw_schemas()
    build_staged_schemas()


if __name__ == "__main__":
    main()
