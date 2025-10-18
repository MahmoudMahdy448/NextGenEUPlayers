#!/usr/bin/env python3
import pandas as pd
import hashlib
from pathlib import Path
import json
import sys

season = '2023-2024'
raw_dir = Path('data') / 'raw' / season
staged_dir = Path('data') / 'staging_data' / season
reports_dir = Path('data') / 'staging_data' / 'reports'
reports_dir.mkdir(parents=True, exist_ok=True)

results = {}

def file_md5(path: Path):
    h = hashlib.md5()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

for raw_file in sorted(raw_dir.glob('*.csv')):
    name = raw_file.stem
    table_key = name.replace(f"-{season}", '').replace(season, '').strip()
    staged_name = f"stg_{table_key}_{season}.csv"
    staged_file = staged_dir / staged_name

    info = {
        'raw_file': str(raw_file),
        'staged_file': str(staged_file) if staged_file.exists() else None,
        'raw_rows': None,
        'raw_cols': None,
        'staged_rows': None,
        'staged_cols': None,
        'row_delta': None,
        'row_delta_pct': None,
        'cols_in_raw_not_staged': [],
        'cols_in_staged_not_raw': [],
        'raw_md5': None,
        'staged_md5': None,
        'raw_player_names': None,
        'staged_player_names': None,
        'missing_in_staged': None,
        'extra_in_staged': None,
        'null_player_name_raw': None,
        'null_player_name_staged': None
    }

    try:
        df_raw = pd.read_csv(raw_file, encoding='utf-8-sig')
    except Exception:
        df_raw = pd.read_csv(raw_file, encoding='latin1')

    info['raw_rows'] = int(len(df_raw))
    info['raw_cols'] = int(len(df_raw.columns))
    try:
        info['raw_md5'] = file_md5(raw_file)
    except Exception:
        info['raw_md5'] = None

    # staged may not exist if processing failed
    if staged_file.exists():
        df_st = pd.read_csv(staged_file, encoding='utf-8-sig')
        info['staged_rows'] = int(len(df_st))
        info['staged_cols'] = int(len(df_st.columns))
        try:
            info['staged_md5'] = file_md5(staged_file)
        except Exception:
            info['staged_md5'] = None

        info['row_delta'] = info['staged_rows'] - info['raw_rows']
        if info['raw_rows']:
            info['row_delta_pct'] = round(100.0 * info['row_delta'] / float(info['raw_rows']), 3)

        # column differences (compare normalized column lists)
        raw_cols = [str(c).strip() for c in df_raw.columns]
        st_cols = [str(c).strip() for c in df_st.columns]
        info['cols_in_raw_not_staged'] = [c for c in raw_cols if c not in st_cols]
        info['cols_in_staged_not_raw'] = [c for c in st_cols if c not in raw_cols]

        # compare player_name sets if present
        if 'player_name' in df_raw.columns:
            raw_players = set(df_raw['player_name'].dropna().astype(str).str.strip())
            info['raw_player_names'] = len(raw_players)
        else:
            raw_players = None

        if 'player_name' in df_st.columns:
            st_players = set(df_st['player_name'].dropna().astype(str).str.strip())
            info['staged_player_names'] = len(st_players)
        else:
            st_players = None

        if raw_players is not None and st_players is not None:
            info['missing_in_staged'] = list(sorted(raw_players - st_players))[:20]
            info['extra_in_staged'] = list(sorted(st_players - raw_players))[:20]

        # null counts for player_name
        if 'player_name' in df_raw.columns:
            info['null_player_name_raw'] = int(df_raw['player_name'].isna().sum())
        if 'player_name' in df_st.columns:
            info['null_player_name_staged'] = int(df_st['player_name'].isna().sum())

    else:
        info['notes'] = 'staged file missing (processing likely failed)'

    results[name] = info

# save report
report_file = reports_dir / f'compare_{season}.json'
with report_file.open('w') as f:
    json.dump(results, f, indent=2)

# print compact summary
for k, v in results.items():
    print(f"{k}: raw_rows={v['raw_rows']} raw_cols={v['raw_cols']} staged_rows={v.get('staged_rows')} staged_cols={v.get('staged_cols')} row_delta_pct={v.get('row_delta_pct')}")

print('\nDetailed JSON report saved to', report_file)

# exit with success
sys.exit(0)
