#!/usr/bin/env python3
"""Profile CSVs under data/raw and generate staging DDL + JSON profiles.

Produces:
 - data/schemas/staging_create_tables.sql
 - data/schemas/staging_schema_profiles.json

Heuristics: infers INTEGER vs REAL vs TEXT by sampling values and using simple cleaning
rules (strip +,%, commas). Uses folder name as season suffix to create table names like
<filename>_<YYYY_YYYY>.
"""
import csv
import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, Any

ROOT = Path('data/raw')
OUT_SQL = Path('data/schemas/staging_create_tables.sql')
OUT_JSON = Path('data/schemas/staging_schema_profiles.json')
# glossary paths
GLOSSARY_IN = Path('data/schemas/fbref_glossary_user.json')
GLOSSARY_ENRICHED_OUT = Path('data/schemas/fbref_glossary_enriched.json')

# in-memory glossary mapping: raw_header -> {canonical, type_hint, desc}
GLOSSARY = {}
CANONICAL_MAPPING_PATH = Path('data/schemas/canonical_mapping.json')
CANONICAL_REGEX = {}


def load_canonical_mapping(path: Path):
    """Load regex mapping dict: canonical -> [pattern strings]."""
    if not path.exists():
        return {}
    j = json.loads(path.read_text())
    # compile regexes
    out = {}
    for canon, patterns in j.items():
        out[canon] = [re.compile(p) for p in patterns]
    return out

# load mapping at import time if available
CANONICAL_REGEX = load_canonical_mapping(CANONICAL_MAPPING_PATH)

def clean_value(v: str):
    """Normalize a raw CSV cell string for numeric checks.

    - Strips surrounding whitespace.
    - Treats empty string or None as None.
    - Removes thousands separators (commas) and leading plus signs.

    This prepares values so the numeric detection helpers can parse
    them reliably (e.g. '+1,234' -> '1234'). Returns None for blank
    values to indicate missing data.
    """
    if v is None:
        return None
    v = v.strip()
    if v == '':
        return None
    # remove thousands separators and plus signs
    v = v.replace(',', '')
    if v.startswith('+'):
        v = v[1:]
    return v


def guess_type_hint_from_name(name: str, desc: str = ''):
    """Heuristic to suggest a type_hint for a glossary entry.

    Returns one of: 'INTEGER', 'REAL', 'TEXT' (VARCHAR not used here).
    This is intentionally conservative — if unsure, returns 'TEXT'.
    """
    n = name.lower()
    # textual identifiers
    if any(tok in n for tok in ('player', 'nation', 'pos', 'squad', 'comp', 'team', 'club', 'season', 'country', 'opp', 'opponent')):
        return 'TEXT'
    # percentages and ratios
    if '%' in name or 'pct' in n or 'percent' in n or '/90' in n or 'per 90' in n or 'rate' in n:
        return 'REAL'
    # names commonly integer
    int_like_tokens = ('rk', 'age', 'born', 'mp', 'starts', 'mins', 'min', 'matches', 'goals', 'gla', 'gls', 'ast', 'saves', 'ga', 'sh', 'shots', 'touches')
    if any(tok in n for tok in int_like_tokens):
        return 'INTEGER'
    # fallback to checking description for hints
    d = desc.lower()
    if 'percent' in d or '%' in d or 'per 90' in d or 'ratio' in d:
        return 'REAL'
    if any(tok in d for tok in ('minutes', 'goals', 'assists', 'matches', 'shots', 'saves', 'touches')):
        return 'INTEGER'
    return 'TEXT'


def load_and_enrich_glossary(path: Path):
    """Load the user glossary and produce a flattened enriched mapping.

    The original `fbref_glossary_user.json` is organized by sections.
    This function flattens all header keys and assigns a suggested
    `canonical` (the header itself) and a `type_hint` using a small
    heuristic. It writes `fbref_glossary_enriched.json` for inspection
    and returns the mapping: { header_name: {canonical, type_hint, desc} }
    """
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    enriched = {}
    for section, mapping in raw.items():
        if not isinstance(mapping, dict):
            continue
        for header, desc in mapping.items():
            # if header already seen, keep the first description but merge
            if header in enriched:
                # don't overwrite; but if description empty, fill it
                if not enriched[header].get('desc') and desc:
                    enriched[header]['desc'] = desc
                continue
            t = guess_type_hint_from_name(header, desc if isinstance(desc, str) else '')
            enriched[header] = {
                'canonical': header,
                'type_hint': t,
                'desc': desc if isinstance(desc, str) else str(desc)
            }
    # write enriched file for visibility
    GLOSSARY_ENRICHED_OUT.parent.mkdir(parents=True, exist_ok=True)
    GLOSSARY_ENRICHED_OUT.write_text(json.dumps(enriched, indent=2))
    return enriched

def is_int_string(s: str):
    """Return True if the cleaned string represents an integer literal.

    Uses a regex that accepts an optional sign followed by digits. This
    is intentionally strict (no decimal point) because some numeric
    values may be integers even if they appear in text form.
    """
    try:
        if s is None:
            return False
        if re.fullmatch(r"[+-]?\d+", s):
            return True
        return False
    except Exception:
        return False

def is_float_string(s: str):
    """Return True if the cleaned string can be parsed as a float.

    This accepts values that are numeric and also handles trailing
    percent signs (e.g. '12.3%') by stripping '%' before attempting
    float conversion.
    """
    try:
        if s is None:
            return False
        # handle percentages like 12.3%
        s2 = s.rstrip('%')
        float(s2)
        return True
    except Exception:
        return False

def infer_type(samples, colname):
    """Infer a SQL-compatible type (INTEGER / REAL / TEXT) for a column.

    Heuristics used:
    - Column name hints: some names are strongly integer-like (rk, age,
      born, mp, starts, matches, etc.) and will be treated as INTEGER if
      all sampled non-null values are integers.
    - Percent-like name or values result in REAL.
    - Sample inspection: values ending with '%' are treated as REAL. If
      all non-null samples look like integers -> INTEGER. If any sample
      looks like a float -> REAL. If any sample cannot be parsed as a
      number -> TEXT.

    This function is conservative: when both integers and floats appear
    it returns REAL, and if parsing is ambiguous it falls back to TEXT.
    """
    # name hints
    lname = colname.lower()
    int_like_names = {'rk','age','born','mp','starts','mins','min','matches','ppg'}
    if any(x in lname for x in ('%','pct','percent','rate')):
        return 'REAL'
    if lname in int_like_names or re.fullmatch(r'.*(_id|_count|count|num)$', lname):
        # prefer int but still check samples
        all_int = True
        for v in samples:
            if v is None:
                continue
            cv = clean_value(v)
            if cv is None:
                continue
            if not is_int_string(cv):
                all_int = False
                break
        if all_int:
            return 'INTEGER'

    # examine samples
    saw_float = False
    saw_int = False
    for v in samples:
        if v is None:
            continue
        cv = clean_value(v)
        if cv is None:
            continue
        # percent handling
        if cv.endswith('%'):
            # treat as real
            saw_float = True
            continue
        if is_int_string(cv):
            saw_int = True
            continue
        if is_float_string(cv):
            saw_float = True
            continue
        # otherwise text
        return 'TEXT'

    if saw_float and not saw_int:
        return 'REAL'
    if saw_int and not saw_float:
        return 'INTEGER'
    if saw_int and saw_float:
        return 'REAL'
    # default
    return 'TEXT'


def lookup_glossary_override(colname: str):
    """Return glossary override dict for colname if available.

    Performs exact match lookup in the flattened GLOSSARY mapping.
    """
    # 1) exact match
    if colname in GLOSSARY:
        return GLOSSARY[colname]

    # 2) consult explicit canonical regex mapping (highest priority)
    for canon, regexes in CANONICAL_REGEX.items():
        for rx in regexes:
            if rx.match(colname):
                # return glossary canonical if available, else synthesize
                if canon in GLOSSARY:
                    return GLOSSARY[canon]
                return {'canonical': canon, 'type_hint': guess_type_hint_from_name(canon, '')}

    # 3) heuristic: strip common section suffixes like '_stats_shooting', '_stats_passing', etc.
    if '_stats_' in colname:
        base = colname.split('_stats_')[0]
        if base in GLOSSARY:
            return GLOSSARY[base]

    # 4) other heuristic: if header contains multiple '_' try first token as base
    if '_' in colname:
        base = colname.split('_')[0]
        if base in GLOSSARY:
            return GLOSSARY[base]

    # 5) final heuristic: try removing trailing section after last space (e.g., 'Def 3rd')
    if ' ' in colname:
        base = colname.split(' ')[0]
        if base in GLOSSARY:
            return GLOSSARY[base]

    return None

def profile_csv(path: Path, max_rows=10000):
    """Read a CSV file and produce a column profile.

    The profile samples up to `max_rows` rows and collects values per
    column. For each column it records an inferred type (INTEGER/REAL/TEXT),
    the number of non-null sample values, and the number of distinct
    sampled values. The returned structure looks like:

      {
        'columns': [ {'name': col, 'type': t, 'sample_non_null': n, 'sample_distinct': d}, ... ],
        'sample_rows': <rows_seen>
      }

    Returning a compact profile enables downstream generation of a
    staging CREATE TABLE statement and quick schema-reporting.
    """
    with path.open(newline='') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return None
        # prepare samples list per column
        cols = [h.strip() for h in header]
        samples = [[] for _ in cols]
        count = 0
        for row in reader:
            count += 1
            for i in range(len(cols)):
                v = row[i] if i < len(row) else None
                samples[i].append(v)
            if count >= max_rows:
                break

    # infer types
    col_profiles = []
    for i, col in enumerate(cols):
        s = samples[i]
        # consult glossary override first
        override = lookup_glossary_override(col)
        if override and 'type_hint' in override:
            t = override['type_hint']
            matched = True
        else:
            t = infer_type(s, col)
            matched = False
        non_null = sum(1 for v in s if v is not None and v.strip()!='')
        distinct = len(set([v for v in s if v not in (None,'')]))
        col_profiles.append({
            'name': col,
            'type': t,
            'sample_non_null': non_null,
            'sample_distinct': distinct,
            'glossary_matched': matched,
            'glossary_canonical': override['canonical'] if override else None,
        })

    return {'columns': col_profiles, 'sample_rows': min(count, max_rows)}

def csv_to_table_name(path: Path):
    """Convert a CSV Path into a staging table name.

    Rules:
      - The season folder (e.g. '2023-2024') becomes a suffix with underscore: '2023_2024'.
      - If the filename stem already contains the season (either '2023-2024' or '2023_2024')
        we do NOT append the season again.
      - For 'players_data' and 'players_data_light' filenames we append the season once
        so they become 'players_data_2023_2024' and 'players_data_light_2023_2024'.
    """
    season = path.parent.name.replace('-', '_')
    stem = path.stem
    normalized = stem.replace('-', '_')

    # if stem already ends with the season (handles both '-' and '_' cases) -> keep it
    if normalized.endswith(season):
        return normalized

    # explicit rule for players_data files: add season once
    if normalized in ('players_data', 'players_data_light'):
        return f"{normalized}_{season}"

    # default: append season
    return f"{normalized}_{season}"


def sanitize_identifier(name: str) -> str:
    """Turn an arbitrary header or name into a safe SQL identifier.

    Replaces non-alphanumeric characters with underscores and collapses
    multiple underscores. Leaves numeric-leading names intact (caller
    can further prefix if desired).
    """
    if name is None:
        return ''
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    s = re.sub(r"__+", "_", s)
    s = s.strip('_')
    if s == '':
        return 'col'
    # prefix if identifier starts with a digit to make it valid in SQL
    if s[0].isdigit():
        s = f"c_{s}"
    return s

def generate_ddl(profiles: Dict[str, Any], schema_name: str = "staging") -> str:
    """Generate CREATE TABLE statements for each profiled CSV.

    This function uses canonical column names when available and adds a
    `_provenance` JSONB column along with `_loaded_at` timestamp to
    preserve raw values and load metadata.
    """
    statements = []

    # ensure staging schema exists
    statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # small set of tokens to avoid as bare identifiers
    reserved_prefix = set(('in','to','on','by','as','user','order','group','type','select','where','from'))

    def normalize_ident(n: str) -> str:
        # sanitized already removes bad chars; ensure lowercase and prefix reserved/leading-digit
        s = sanitize_identifier(n).lower()
        if s == '':
            s = 'col'
        if s[0].isdigit() or s in reserved_prefix or len(s) <= 2:
            s = f"c_{s}"
        return s

    def promote_type(t1: str, t2: str) -> str:
        # pick a type that can hold both t1 and t2: TEXT > REAL > INTEGER
        order = {'INTEGER': 1, 'REAL': 2, 'TEXT': 3}
        if t1 == t2:
            return t1
        if order.get(t1, 3) >= order.get(t2, 3):
            return t1
        return t2

    for table, profile in profiles.items():
        col_map = {}  # ident -> (type, original_names[])
        order_seen = []
        for col in profile["columns"]:
            orig_name = col["name"]
            canonical = col.get("glossary_canonical")
            chosen = canonical if canonical else orig_name
            ident = normalize_ident(chosen)
            col_type = col.get("type", "TEXT")
            if ident in col_map:
                existing_type, origs = col_map[ident]
                new_type = promote_type(existing_type, col_type)
                origs.append(orig_name)
                col_map[ident] = (new_type, origs)
            else:
                col_map[ident] = (col_type, [orig_name])
                order_seen.append(ident)

        # add provenance and load metadata at the end
        col_map['_provenance'] = ('JSONB', [])
        order_seen.append('_provenance')
        col_map['_loaded_at'] = ('TIMESTAMP', [])
        order_seen.append('_loaded_at')

        # build CREATE TABLE
        table_ident = normalize_ident(table)
        stmt_lines = [f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_ident} ("]
        col_lines = []
        for ident in order_seen:
            col_type, origs = col_map[ident]
            col_lines.append(f"    {ident} {col_type}")
        stmt_lines.append(',\n'.join(col_lines))
        stmt_lines.append(');')
        statements.append('\n'.join(stmt_lines))

    return '\n\n'.join(statements)

def main():
    # load glossary/enriched mapping now that loader functions are defined
    global GLOSSARY
    GLOSSARY = load_and_enrich_glossary(GLOSSARY_IN)

    profiles = {}
    csvs = sorted([p for p in ROOT.rglob('*.csv')])
    for p in csvs:
        # skip players data summary files? include all for staging
        prof = profile_csv(p)
        if prof is None:
            continue
        table = csv_to_table_name(p)
        profiles[table] = prof

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(profiles, indent=2))

    ddl = generate_ddl(profiles)
    OUT_SQL.write_text(ddl)
    print('WROTE', OUT_SQL, 'and', OUT_JSON)

if __name__ == '__main__':
    main()
