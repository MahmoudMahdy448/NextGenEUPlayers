import os
import json
import pandas as pd
import time
import random

# Local output folder for CSVs
OUT_FOLDER = r"/workspaces/NextGenEUPlayers/data/raw"

# URL-e for scraping
URLS = {
    'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats': 'stats_standard',
    'https://fbref.com/en/comps/Big5/shooting/players/Big-5-European-Leagues-Stats': 'stats_shooting',
    'https://fbref.com/en/comps/Big5/passing/players/Big-5-European-Leagues-Stats': 'stats_passing',
    'https://fbref.com/en/comps/Big5/passing_types/players/Big-5-European-Leagues-Stats': 'stats_passing_types',
    'https://fbref.com/en/comps/Big5/gca/players/Big-5-European-Leagues-Stats': 'stats_gca',
    'https://fbref.com/en/comps/Big5/defense/players/Big-5-European-Leagues-Stats': 'stats_defense',
    'https://fbref.com/en/comps/Big5/possession/players/Big-5-European-Leagues-Stats': 'stats_possession',
    'https://fbref.com/en/comps/Big5/playingtime/players/Big-5-European-Leagues-Stats': 'stats_playing_time',
    'https://fbref.com/en/comps/Big5/misc/players/Big-5-European-Leagues-Stats': 'stats_misc',
    'https://fbref.com/en/comps/Big5/keepers/players/Big-5-European-Leagues-Stats': 'stats_keeper',
    'https://fbref.com/en/comps/Big5/keepersadv/players/Big-5-European-Leagues-Stats': 'stats_keeper_adv'
}

# Explicit season URL maps (user-provided)
URLS_2023_2024 = {
    'https://fbref.com/en/comps/Big5/2023-2024/stats/players/Big-5-European-Leagues-Stats': 'stats_standard',
    'https://fbref.com/en/comps/Big5/2023-2024/shooting/players/Big-5-European-Leagues-Stats': 'stats_shooting',
    'https://fbref.com/en/comps/Big5/2023-2024/passing/players/Big-5-European-Leagues-Stats': 'stats_passing',
    'https://fbref.com/en/comps/Big5/2023-2024/passing_types/players/Big-5-European-Leagues-Stats': 'stats_passing_types',
    'https://fbref.com/en/comps/Big5/2023-2024/gca/players/Big-5-European-Leagues-Stats': 'stats_gca',
    'https://fbref.com/en/comps/Big5/2023-2024/defense/players/Big-5-European-Leagues-Stats': 'stats_defense',
    'https://fbref.com/en/comps/Big5/2023-2024/possession/players/Big-5-European-Leagues-Stats': 'stats_possession',
    'https://fbref.com/en/comps/Big5/2023-2024/playingtime/players/Big-5-European-Leagues-Stats': 'stats_playing_time',
    'https://fbref.com/en/comps/Big5/2023-2024/misc/players/Big-5-European-Leagues-Stats': 'stats_misc',
    'https://fbref.com/en/comps/Big5/2023-2024/keepers/players/Big-5-European-Leagues-Stats': 'stats_keeper',
    'https://fbref.com/en/comps/Big5/2023-2024/keepersadv/players/Big-5-European-Leagues-Stats': 'stats_keeper_adv'
}

URLS_2024_2025 = {
    'https://fbref.com/en/comps/Big5/2024-2025/stats/players/Big-5-European-Leagues-Stats': 'stats_standard',
    'https://fbref.com/en/comps/Big5/2024-2025/shooting/players/Big-5-European-Leagues-Stats': 'stats_shooting',
    'https://fbref.com/en/comps/Big5/2024-2025/passing/players/Big-5-European-Leagues-Stats': 'stats_passing',
    'https://fbref.com/en/comps/Big5/2024-2025/passing_types/players/Big-5-European-Leagues-Stats': 'stats_passing_types',
    'https://fbref.com/en/comps/Big5/2024-2025/gca/players/Big-5-European-Leagues-Stats': 'stats_gca',
    'https://fbref.com/en/comps/Big5/2024-2025/defense/players/Big-5-European-Leagues-Stats': 'stats_defense',
    'https://fbref.com/en/comps/Big5/2024-2025/possession/players/Big-5-European-Leagues-Stats': 'stats_possession',
    'https://fbref.com/en/comps/Big5/2024-2025/playingtime/players/Big-5-European-Leagues-Stats': 'stats_playing_time',
    'https://fbref.com/en/comps/Big5/2024-2025/misc/players/Big-5-European-Leagues-Stats': 'stats_misc',
    'https://fbref.com/en/comps/Big5/2024-2025/keepers/players/Big-5-European-Leagues-Stats': 'stats_keeper',
    'https://fbref.com/en/comps/Big5/2024-2025/keepersadv/players/Big-5-European-Leagues-Stats': 'stats_keeper_adv'
}

# Mapping seasons to explicit URL dictionaries
URLS_BY_SEASON = {
    "2023-2024": URLS_2023_2024,
    "2024-2025": URLS_2024_2025,
}

# Default seasons list
DEFAULT_SEASONS = ["2025-2026", "2024-2025", "2023-2024"]


# Kaggle upload/auth removed per user request — script will save CSVs locally


def scrape_table(url, table_id):
    """ Retrieves a table from the given URL """
    try:
        df = pd.read_html(url, attrs={"id": table_id})[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
        df = df.loc[:, ~df.columns.duplicated()]
        if 'Player' in df.columns:
            df = df[df['Player'] != 'Player']
        print(f"Retrieved: {table_id}")
        return df
    except Exception as e:
        print(f"Error retrieving {table_id}: {e}")
        return None


def scrape_all_tables():
    """ Retrieves all tables """
    dfs = {}
    for url, table_id in URLS.items():
        df = scrape_table(url, table_id)
        if df is not None:
            dfs[table_id] = df
        time.sleep(random.uniform(1, 2))
    return dfs


def scrape_all_tables_for_season(season):
    """Scrape all configured tables for a given season.

    Prefers an explicit per-season URL map if available, otherwise falls back to the generic URLS map.
    Returns a dict of table_id -> DataFrame (None if failed).
    """
    urls_map = URLS_BY_SEASON.get(season, URLS)
    dfs = {}
    for url, table_id in urls_map.items():
        df = scrape_table(url, table_id)
        dfs[table_id] = df
        time.sleep(random.uniform(1, 2))
    return dfs


def merge_dataframes(dfs):
    """ Merges retrieved tables """
    if 'stats_standard' not in dfs:
        raise ValueError("Missing main table 'stats_standard'!")
    merged_df = dfs['stats_standard']
    for name, df in dfs.items():
        if name != 'stats_standard':
            merged_df = merged_df.merge(df, on=['Player', 'Squad'], how='left', suffixes=('', f'_{name}'))
    return merged_df


def remove_unwanted_columns(df):
    """ Removes columns containing 'matches' """
    return df.drop(columns=[col for col in df.columns if "matches" in col.lower()], errors='ignore')


def fix_age_format(df):
    """
    Converts the 'Age' column from 'yy-ddd' format to 'yy' (years only).
    
    Example:
    - '22-150' -> '22'
    - '19-032' -> '19'
    """
    if 'Age' in df.columns:
        df['Age'] = df['Age'].astype(str).str.split('-').str[0]
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    return df


def save_outputs_locally(df_full, df_light, season):
    """Save the full and light dataframes locally under OUT_FOLDER/<season>/"""
    season_folder = os.path.join(OUT_FOLDER, season)
    os.makedirs(season_folder, exist_ok=True)
    safe_season = season.replace('/', '_')
    full_fname = f"players_data-{safe_season}.csv"
    light_fname = f"players_data_light-{safe_season}.csv"
    full_path = os.path.join(season_folder, full_fname)
    light_path = os.path.join(season_folder, light_fname)
    df_full.to_csv(full_path, index=False)
    df_light.to_csv(light_path, index=False)
    print(f"Saved files to {season_folder}: {full_fname} and {light_fname}")


def save_individual_tables(dfs, season):
    """Save each raw table DataFrame under raw_data/<season>/<table_id>.csv"""
    season_dir = os.path.join(OUT_FOLDER, season)
    os.makedirs(season_dir, exist_ok=True)
    saved = []
    for tid, df in dfs.items():
        if df is None:
            continue
        fname = os.path.join(season_dir, f"{tid}.csv")
        df.to_csv(fname, index=False)
        saved.append(fname)
    print(f"Saved {len(saved)} individual tables to {season_dir}")
    return saved


def run_pipeline(seasons=None):
    """ Runs the pipeline for one or more seasons. If seasons is None, use DEFAULT_SEASONS."""
    seasons = seasons or DEFAULT_SEASONS
    print(f"Starting data scraping for seasons: {seasons}")
    for season in seasons:
        print(f"\n=== Season: {season} ===")
        dfs = scrape_all_tables_for_season(season)
        # save raw tables individually before merging
        save_individual_tables(dfs, season)
        try:
            merged_df = merge_dataframes(dfs)
        except Exception as e:
            print(f"Could not merge tables for season {season}: {e}")
            continue
        df_cleaned = remove_unwanted_columns(merged_df)
        df_cleaned_fixed_age = fix_age_format(df_cleaned)

        keep_columns = [
            'Rk', 'Player', 'Nation', 'Pos', 'Squad', 'Comp', 'Age', 'Born', 'MP', 'Starts', 'Min', '90s', 'Gls', 'Ast',
            'G+A', 'G-PK', 'PK', 'PKatt', 'CrdY', 'CrdR', 'xG', 'npxG', 'xAG', 'npxG+xAG', 'G+A-PK', 'xG+xAG', 'PrgC',
            'PrgP', 'PrgR', 'Sh', 'SoT', 'SoT%', 'Sh/90', 'SoT/90', 'G/Sh', 'G/SoT', 'Dist', 'FK',
            'PK_stats_shooting', 'PKatt_stats_shooting', 'xG_stats_shooting', 'npxG_stats_shooting', 'npxG/Sh',
            'G-xG', 'np:G-xG', 'Cmp', 'Att', 'Cmp%', 'TotDist', 'PrgDist', 'Ast_stats_passing', 'xAG_stats_passing',
            'xA', 'A-xAG', 'KP', '1/3', 'PPA', 'CrsPA', 'PrgP_stats_passing', 'Live', 'Dead', 'FK_stats_passing_types',
            'TB', 'Sw', 'Crs', 'TI', 'CK', 'In', 'Out', 'Str', 'Cmp_stats_passing_types', 'Tkl', 'TklW', 'Def 3rd',
            'Mid 3rd', 'Att 3rd', 'Att_stats_defense', 'Tkl%', 'Lost', 'Blocks_stats_defense', 'Sh_stats_defense',
            'Pass', 'Int', 'Tkl+Int', 'Clr', 'Err', 'SCA', 'SCA90', 'PassLive', 'PassDead', 'TO', 'Sh_stats_gca', 'Fld',
            'Def', 'GCA', 'GCA90', 'Touches', 'Def Pen', 'Def 3rd_stats_possession', 'Mid 3rd_stats_possession',
            'Att 3rd_stats_possession', 'Att Pen', 'Live_stats_possession', 'Att_stats_possession', 'Succ', 'Succ%',
            'Tkld', 'Tkld%', 'Carries', 'TotDist_stats_possession', 'PrgDist_stats_possession',
            'PrgC_stats_possession', '1/3_stats_possession', 'CPA', 'Mis', 'Dis', 'Rec', 'PrgR_stats_possession',
            'CrdY_stats_misc', 'CrdR_stats_misc', '2CrdY', 'Fls', 'Fld_stats_misc', 'Off_stats_misc', 'Crs_stats_misc',
            'Int_stats_misc', 'TklW_stats_misc', 'PKwon', 'PKcon', 'OG', 'Recov', 'Won', 'Lost_stats_misc', 'Won%',
            'GA', 'GA90', 'SoTA', 'Saves', 'Save%', 'W', 'D', 'L', 'CS', 'CS%', 'PKatt_stats_keeper', 'PKA', 'PKsv',
            'PKm', 'PSxG', 'PSxG/SoT', 'PSxG+/-', '/90', 'Cmp_stats_keeper_adv', 'Att_stats_keeper_adv',
            'Cmp%_stats_keeper_adv', 'Att (GK)', 'Thr', 'Launch%', 'AvgLen', 'Opp', 'Stp', 'Stp%', '#OPA', '#OPA/90',
            'AvgDist'
        ]
        # Some columns may be missing; select only those present
        existing_keep = [c for c in keep_columns if c in df_cleaned_fixed_age.columns]
        df_light = df_cleaned_fixed_age[existing_keep]
        save_outputs_locally(df_cleaned_fixed_age, df_light, season)


run_pipeline()
