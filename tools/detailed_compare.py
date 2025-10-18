#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import json
import sys

season = '2023-2024'
raw_dir = Path('data') / 'raw' / season
staged_dir = Path('data') / 'staging_data' / season
reports_dir = Path('data') / 'staging_data' / 'reports'
reports_dir.mkdir(parents=True, exist_ok=True)

OUT_FILE = reports_dir / f'detailed_compare_{season}.json'

N = 5  # head/tail rows to include

results = {}

raw_files = sorted(raw_dir.glob('*.csv')) if raw_dir.exists() else []

for raw_file in raw_files:
    table_key = raw_file.stem
    # normalized table name used for staged filenames (strip season or trailing dash-season)
    table_base = table_key.replace(f'-{season}', '').replace(season, '').strip()
    staged_name = f'stg_{table_base}_{season}.csv'
    staged_file = staged_dir / staged_name
    metadata_file = staged_dir / f'{table_base}_metadata.json'

    rec = {
        'raw_file': str(raw_file),
        'staged_file': str(staged_file) if staged_file.exists() else None,
        'metadata_file': str(metadata_file) if metadata_file.exists() else None,
        'table_description': {},
        'raw_head': [],
        'raw_tail': [],
        'staged_head': [],
        'staged_tail': [],
        'columns_raw': [],
        'columns_staged': [],
        'cols_in_raw_not_staged': [],
        'cols_in_staged_not_raw': [],
        'metadata_missing_descriptions': [],
        'sample_mismatches_head': {},
        'sample_mismatches_tail': {}
    }

    # read raw
    try:
        try:
            df_raw = pd.read_csv(raw_file, encoding='utf-8-sig')
        except Exception:
            df_raw = pd.read_csv(raw_file, encoding='latin1')
    except Exception as e:
        rec['error'] = f'Failed to read raw file: {e}'
        results[table_key] = rec
        continue

    rec['raw_head'] = df_raw.head(N).fillna('').astype(str).to_dict(orient='records')
    rec['raw_tail'] = df_raw.tail(N).fillna('').astype(str).to_dict(orient='records')
    raw_cols = [str(c).strip() for c in df_raw.columns]
    rec['columns_raw'] = raw_cols

    # read staged if present
    if staged_file.exists():
        try:
            df_st = pd.read_csv(staged_file, encoding='utf-8-sig')
        except Exception:
            df_st = pd.read_csv(staged_file, encoding='latin1')
        rec['staged_head'] = df_st.head(N).fillna('').astype(str).to_dict(orient='records')
        rec['staged_tail'] = df_st.tail(N).fillna('').astype(str).to_dict(orient='records')
        st_cols = [str(c).strip() for c in df_st.columns]
        rec['columns_staged'] = st_cols

        # column diffs
        rec['cols_in_raw_not_staged'] = [c for c in raw_cols if c not in st_cols]
        rec['cols_in_staged_not_raw'] = [c for c in st_cols if c not in raw_cols]

        # sample mismatches: for columns in intersection, compare stringified head rows
        common = [c for c in raw_cols if c in st_cols]
        # compare head
        mism_head = {}
        for c in common:
            raw_vals = df_raw[c].head(N).fillna('').astype(str).tolist()
            st_vals = df_st[c].head(N).fillna('').astype(str).tolist()
            diffs = []
            for i in range(min(len(raw_vals), len(st_vals))):
                if raw_vals[i] != st_vals[i]:
                    diffs.append({'row': i, 'raw': raw_vals[i], 'staged': st_vals[i]})
            if diffs:
                mism_head[c] = diffs
        rec['sample_mismatches_head'] = mism_head

        # compare tail
        mism_tail = {}
        for c in common:
            raw_vals = df_raw[c].tail(N).fillna('').astype(str).tolist()
            st_vals = df_st[c].tail(N).fillna('').astype(str).tolist()
            diffs = []
            for i in range(min(len(raw_vals), len(st_vals))):
                if raw_vals[i] != st_vals[i]:
                    diffs.append({'row_from_end': i, 'raw': raw_vals[-(i+1)], 'staged': st_vals[-(i+1)]})
            if diffs:
                mism_tail[c] = diffs
        rec['sample_mismatches_tail'] = mism_tail

    else:
        rec['notes'] = 'staged file missing (processing failed)'

    # metadata / descriptions
    if metadata_file.exists():
        try:
            with metadata_file.open() as f:
                md = json.load(f)
            rec['table_description'] = md
            # list columns present but with missing description
            missing = [c for c, d in md.items() if isinstance(d, str) and d.strip().lower().startswith('no description')]
            rec['metadata_missing_descriptions'] = missing
        except Exception as e:
            rec['metadata_error'] = str(e)
    else:
        # no metadata: attempt a best-effort description mapping from column names (empty)
        rec['table_description'] = {}

    results[table_key] = rec

# write report
with OUT_FILE.open('w') as f:
    json.dump(results, f, indent=2)

print('Wrote detailed comparison report to', OUT_FILE)

# also print a compact summary for user
for k, v in results.items():
    s = f"{k}: raw_cols={len(v.get('columns_raw',[]))} staged_cols={len(v.get('columns_staged',[])) if v.get('columns_staged') else 'MISSING'}"
    s += f" cols_missing_in_staged={len(v.get('cols_in_raw_not_staged',[]))} metadata_gaps={len(v.get('metadata_missing_descriptions',[]))}"
    print(s)

sys.exit(0)
