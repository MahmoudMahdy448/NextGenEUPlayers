#!/usr/bin/env python3
"""Merge per-season raw profiles and staged schema JSONs into two combined manifests.

Outputs:
- data/schemas/raw_profiles_by_season.json
- data/schemas/staged_schemas_by_season.json

Each file has structure:
{
  "generated_at": "...",
  "seasons": {
    "2023-2024": {
      "tables": {
        "players_data": { ... original per-season table object ... }
      }
    },
    ...
  }
}

For staged manifest, tables map to declared column types loaded from data/staging_data/<season>/*_schema.json
"""
from pathlib import Path
import json
import datetime

ROOT = Path.cwd()
PROFILES_RAW = ROOT / "data" / "schemas" / "profiles" / "raw"
OUT_DIR = ROOT / "data" / "schemas"
STAGED_DIR = ROOT / "data" / "staging_data"

OUT_DIR.mkdir(parents=True, exist_ok=True)


def build_raw_manifest():
    manifest = {"generated_at": datetime.datetime.utcnow().isoformat() + "Z", "seasons": {}}
    for p in sorted(PROFILES_RAW.glob("per_season_*.initial_raw_data_profile.json")):
        try:
            with p.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as e:
            print(f"Skipping {p}: {e}")
            continue
        season = data.get("season") or p.stem
        # keep original per-season structure under tables
        manifest["seasons"][season] = {"tables": data.get("tables", {}), "summary": {"rows": None, "columns": None}}
    out_path = OUT_DIR / "raw_profiles_by_season.json"
    with out_path.open("w", encoding="utf-8") as of:
        json.dump(manifest, of, indent=2, ensure_ascii=False)
    print(f"Wrote raw manifest: {out_path}")


def build_staged_manifest():
    manifest = {"generated_at": datetime.datetime.utcnow().isoformat() + "Z", "seasons": {}}
    for season_dir in sorted([d for d in STAGED_DIR.iterdir() if d.is_dir()]):
        season = season_dir.name
        tables = {}
        for schema_file in sorted(season_dir.glob("*_schema.json")):
            table = schema_file.name.replace("_schema.json", "")
            try:
                with schema_file.open("r", encoding="utf-8") as fh:
                    sdata = json.load(fh)
            except Exception as e:
                print(f"Skipping {schema_file}: {e}")
                continue
            tables[table] = {"columns": sdata}
        manifest["seasons"][season] = {"tables": tables}
    out_path = OUT_DIR / "staged_schemas_by_season.json"
    with out_path.open("w", encoding="utf-8") as of:
        json.dump(manifest, of, indent=2, ensure_ascii=False)
    print(f"Wrote staged manifest: {out_path}")


def main():
    build_raw_manifest()
    build_staged_manifest()


if __name__ == "__main__":
    main()
