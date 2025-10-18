import pandas as pd
import os
import json
import re
from pathlib import Path
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FBRef Statistics Glossary
FBREF_GLOSSARY = {
    "Player Standard Stats": {
        "Player": "Player name.",
        "Rk": "Rank - This is a count of the rows from top to bottom. It is recalculated following the sorting of a column.",
        "Squad": "Player's squad / club.",
        "Nation": "Nationality of the player. First, we check our records in international play at senior level. Then youth level. Then citizenship presented on wikipedia. Finally, we use their birthplace when available.",
        "Pos": "Position most commonly played by the player. Examples: GK - Goalkeepers, DF - Defenders, MF - Midfielders, FW - Forwards, FB - Fullbacks, LB - Left Backs, RB - Right Backs, CB - Center Backs, DM - Defensive Midfielders, CM - Central Midfielders, LM - Left Midfielders, RM - Right Midfielders, WM - Wide Midfielders, LW - Left Wingers, RW - Right Wingers, AM - Attacking Midfielders.",
        "Comp": "Competition. Number next to competition states which level in the country's league pyramid this league occupies.",
        "Age": "Age at season start. Given on August 1 for winter leagues and February 1 for summer leagues.",
        "Born": "Year of birth.",
        "MP": "Matches Played by the player or squad.",
        "Starts": "Game or games started by player.",
        "Min": "Minutes played.",
        "90s": "90s played - Minutes played divided by 90.",
        "Gls": "Goals scored or allowed.",
        "Ast": "Assists.",
        "G+A": "Goals + Assists.",
        "G-PK": "Non-Penalty Goals.",
        "PK": "Penalty Kicks Made.",
        "PKatt": "Penalty Kicks Attempted.",
        "CrdY": "Yellow Cards.",
        "CrdR": "Red Cards.",
        "xG": "xG: Expected Goals. xG totals include penalty kicks, but do not include penalty shootouts (unless otherwise noted). Provided by Opta.",
        "npxG": "npxG: Non-Penalty xG. Non-Penalty Expected Goals. Provided by Opta.",
        "xAG": "xAG: Expected Assisted Goals - xG which follows a pass that assists a shot. Provided by Opta.",
        "npxG+xAG": "Non-Penalty Expected Goals plus Assisted Goals. xG totals include penalty kicks, but do not include penalty shootouts (unless otherwise noted). Provided by Opta.",
        "PrgC": "Progressive Carries - Carries that move the ball towards the opponent's goal line at least 10 yards from its furthest point in the last six passes, or any carry into the penalty area. Excludes carries which end in the defending 50% of the pitch.",
        "PrgP": "Progressive Passes - Completed passes that move the ball towards the opponent's goal line at least 10 yards from its furthest point in the last six passes, or any completed pass into the penalty area. Excludes passes from the defending 40% of the pitch.",
        "PrgR": "Progressive Passes Received - Completed passes that move the ball towards the opponent's goal line at least 10 yards from its furthest point in the last six passes, or any completed pass into the penalty area. Excludes passes from the defending 40% of the pitch.",
        "G+A-PK": "Goals + Assists minus Penalty Kicks made (non-penalty contribution).",
        "xG+xAG": "Expected Goals plus Expected Assisted Goals (xG + xAG).",
        "A-xAG": "Assists minus xAG (actual assists minus expected assisted goals).",
        "Matches": "Matches played (alternate label sometimes used instead of MP)."
    },
    "Player Shooting": {
        "Sh": "Shots Total - Does not include penalty kicks.",
        "SoT": "Shots on Target - Note: Shots on target do not include penalty kicks.",
        "SoT%": "Shots on Target %.",
        "Sh/90": "Shots Total per 90 minutes.",
        "SoT/90": "Shots on target per 90 minutes.",
        "G/Sh": "Goals per shot.",
        "G/SoT": "Goals per shot on target.",
        "Dist": "Average Shot Distance (yards).",
        "FK": "Shots from Free Kicks.",
        "npxG/Sh": "Non-Penalty xG per shot.",
        "G-xG": "Goals minus Expected Goals.",
        "np:G-xG": "Non-Penalty Goals minus Non-Penalty Expected Goals."
    },
    "Player Passing": {
        "Cmp": "Passes Completed - Includes live ball passes (including crosses) as well as corner kicks, throw-ins, free kicks and goal kicks.",
        "Att": "Passes Attempted - Includes live ball passes (including crosses) as well as corner kicks, throw-ins, free kicks and goal kicks.",
        "Cmp%": "Pass Completion %.",
        "TotDist": "Total Passing Distance (yards) - total distance that completed passes have traveled in any direction.",
        "PrgDist": "Progressive Passing Distance - total distance that completed passes have traveled towards the opponent's goal.",
        "xA": "xA: Expected Assists - Provided by Opta.",
        "KP": "Key Passes - Passes that directly lead to a shot (assisted shots).",
        "1/3": "Passes into Final Third - Completed passes that enter the attacking 1/3 of the pitch (not including set pieces).",
        "PPA": "Passes into Penalty Area - Completed passes into the 18-yard box (not including set pieces).",
        "CrsPA": "Crosses into Penalty Area - Completed crosses into the 18-yard box (not including set pieces).",
        "A-xAG": "Assists minus xAG (actual assists minus expected assisted goals)."
    },
    "Player Pass Types": {
        "Live": "Live-ball Passes.",
        "Dead": "Dead-ball Passes (set pieces, corners, throw-ins, goal kicks).",
        "TB": "Through Balls.",
        "Sw": "Switches - Passes traveling more than 40 yards of width.",
        "Crs": "Crosses.",
        "CK": "Corner Kicks.",
        "TI": "Throw-ins Taken.",
        "PassLive": "Live-ball Passes (same as 'Live' but used in some CSVs).",
        "PassDead": "Dead-ball Passes (same as 'Dead' but used in some CSVs)."
    },
    "Player Goal and Shot Creation": {
        "SCA": "Shot-Creating Actions - the two offensive actions directly leading to a shot, such as passes, take-ons and drawing fouls.",
        "SCA90": "Shot-Creating Actions per 90 minutes.",
        "GCA": "Goal-Creating Actions - the two offensive actions directly leading to a goal.",
        "GCA90": "Goal-Creating Actions per 90 minutes."
    },
    "Player Defensive Actions": {
        "Tkl": "Tackles.",
        "TklW": "Tackles Won - Tackles in which the tackler's team won possession of the ball.",
        "Int": "Interceptions.",
        "Tkl%": "% of Dribblers Tackled - Dribblers tackled divided by number of attempts to challenge an opposing dribbler.",
        "Tkl+Int": "Tackles plus Interceptions - simple sum of tackles and interceptions.",
        "Blocks": "Number of times blocking the ball by standing in its path.",
        "Clr": "Clearances.",
        "Err": "Errors - Mistakes leading to an opponent's shot."
    },
    "Player Possession": {
        "Touches": "Number of times a player touched the ball.",
        "PrgC": "Progressive Carries (see Player Standard Stats).",
        "Carries": "Number of times the player controlled the ball with their feet.",
        "TotDist": "Total Carrying Distance (yards).",
        "PrgDist": "Progressive Carrying Distance (yards).",
        "Def Pen": "Touches in the defensive penalty area.",
        "Att Pen": "Touches in the attacking penalty area.",
        "Succ": "Successful Take-Ons.",
        "Succ%": "Successful Take-On Percentage.",
        "Tkld": "Times Tackle During Take-On.",
        "Tkld%": "Tackled During Take-On Percentage."
    },
    "Player Playing Time": {
        "MP": "Matches Played.",
        "Min": "Minutes.",
        "Mn/MP": "Minutes Per Match Played.",
        "Min%": "Percentage of Squad Minutes Played.",
        "Starts": "Matches started.",
        "Subs": "Substitute Appearances.",
        "Mn/Start": "Minutes Per Match Started.",
        "Compl": "Complete Matches Played.",
        "Mn/Sub": "Minutes Per Substitution.",
        "unSub": "Matches as Unused Sub.",
        "PPM": "Points per Match (team points per match when player appeared).",
        "onG": "Goals scored by team while player was on pitch.",
        "onGA": "Goals allowed by team while player was on pitch.",
        "+/-": "Plus/Minus - goals scored minus goals allowed by the team while the player was on the pitch.",
        "+/-90": "Plus/Minus per 90 minutes.",
        "On-Off": "Plus/Minus Net per 90 Minutes.",
        "onxG": "xG while on pitch.",
        "onxGA": "xGA while on pitch.",
        "xG+/-": "xG Plus/Minus.",
        "xG+/-90": "xG Plus/Minus per 90 minutes."
    },
    "Player Miscellaneous Stats": {
        "CrdY": "Yellow Cards.",
        "CrdR": "Red Cards.",
        "Fls": "Fouls Committed.",
        "Fld": "Fouls Drawn.",
        "OG": "Own Goals.",
        "PKwon": "Penalty Kicks Won.",
        "PKcon": "Penalty Kicks Conceded."
    },
    "Player Goalkeeping": {
        "GA": "Goals Against.",
        "GA90": "Goals Against per 90 minutes.",
        "SoTA": "Shots on Target Against.",
        "Save%": "Save Percentage - (Shots on Target Against - Goals Against)/Shots on Target Against. Note: does not include penalty kicks; not all shots on target are stopped by the keeper, many will be stopped by defenders."
    },
    "Player Goalkeeping Advanced": {
        "Saves": "Number of saves recorded by goalkeeper.",
        "W": "Wins - matches won when keeper played.",
        "D": "Draws.",
        "L": "Losses.",
        "CS": "Clean Sheets - matches with no goals allowed.",
        "CS%": "Clean Sheet Percentage.",
        "PKA": "Penalty Kicks Allowed.",
        "PKsv": "Penalty Kicks Saved.",
        "PKm": "Penalty Kicks Missed (not saved).",
        "PSxG": "Post-shot xG (PSxG).",
        "PSxG/SoT": "Post-shot xG per Shot on Target.",
        "PSxG+/-": "PSxG plus/minus (goals minus post-shot xG).",
        "#OPA": "Number of Opponent Penalty Area entries stopped (keeper stat, variable definition).",
        "#OPA/90": "#OPA per 90 minutes.",
        "AvgDist": "Average distance of actions (keeper-specific metric).",
        "Thr": "Throws (keeper distribution).",
        "Launch%": "Percent of long distributions (launch%).",
        "AvgLen": "Average length of distributions.",
        "Opp": "Opponent (context column used in some keeper advanced files).",
        "Stp": "Stops (keeper).",
        "Stp%": "Stop percentage.",
        "Att (GK)": "Attempts (GK) - attempts at distributions by keeper."
    }
}

class StagingDataProcessor:
    def __init__(self, raw_data_path: str):
        # allow toggling of aggressive numeric fill (fill NaN with 0)
        self.raw_data_path = Path(raw_data_path)
        # write staging outputs under data/staging_data by default
        self.staging_data_path = Path("data") / "staging_data"
        self.staging_data_path.mkdir(exist_ok=True)
        # directory for per-file processing reports
        self.reports_path = self.staging_data_path / "reports"
        self.reports_path.mkdir(exist_ok=True)
        # by default keep the previous behavior (fill numeric NaN with 0)
        self.fillna_numeric = True
        
    def get_column_metadata(self, column_name: str) -> str:
        """Get metadata description for a column from the glossary"""
        def _standardize_label(s: str) -> str:
            """Normalize a label to snake_case-ish lowercase for comparison."""
            if s is None:
                return ""
            s = str(s)
            # take text before common separators
            s = s.split(' - ')[0].split(':')[0]
            # remove BOM/NBSP
            s = s.replace('\ufeff', '').replace('\xa0', ' ')
            # lowercase
            s = s.lower()
            # replace non-alphanumeric with underscore
            s = re.sub(r'[^0-9a-z]+', '_', s)
            s = re.sub(r'_+', '_', s)
            s = s.strip('_')
            return s

        # direct match first (some glossary keys may already be normalized)
        for category, definitions in FBREF_GLOSSARY.items():
            if column_name in definitions:
                return definitions[column_name]

        # try to match by normalizing glossary keys and the human-readable phrase in the description
        target = _standardize_label(column_name)
        # explicit mapping from standardized column names to glossary keys (common cases)
        STANDARD_TO_GLOSSARY = {
            'position': 'Pos',
            'competition': 'Comp',
            'birth_year': 'Born',
            'nineties_played': '90s',
            'shot_creating_actions_per_90': 'SCA90',
            'shot_creating_actions_live_pass': 'PassLive',
            'shot_creating_actions_dead_pass': 'PassDead',
            'shot_creating_actions_take_on': 'TO',
            'shot_creating_actions_shot': 'Sh',
            'shot_creating_actions_foul_drawn': 'Fld',
            'goal_creating_actions_per_90': 'GCA90',
            'player_name': 'Player',
            'nation': 'Nation',
            'squad': 'Squad',
            'age': 'Age'
        }
        if target in STANDARD_TO_GLOSSARY:
            gloss_key = STANDARD_TO_GLOSSARY[target]
            for category, definitions in FBREF_GLOSSARY.items():
                if gloss_key in definitions:
                    return definitions[gloss_key]
        for category, definitions in FBREF_GLOSSARY.items():
            for key, desc in definitions.items():
                # normalize the glossary key and the first clause of the description
                key_norm = _standardize_label(key)
                desc_title = str(desc).split(' - ')[0].split(':')[0]
                desc_norm = _standardize_label(desc_title)

                if target == key_norm or target == desc_norm:
                    return desc

        return f"No description available for {column_name}"
    
    def get_table_metadata(self, table_type: str) -> Dict[str, str]:
        """Get metadata for all columns in a table type"""
        metadata = {}
        
        # Get standard column order for this table type
        standard_order = self.get_standard_column_order(table_type)
        
        for col in standard_order:
            metadata[col] = self.get_column_metadata(col)
        
        return metadata
        
    def get_standard_column_order(self, table_type: str) -> List[str]:
        """Define standard column order for each table type"""
        # Define standard column orders for each table type
        standard_orders = {
            'stats_standard': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition', 
                'age', 'birth_year', 'matches_played', 'starts', 'minutes', 'nineties_played',
                'goals', 'assists', 'goals_assists', 'goals_minus_pk', 'penalty_goals', 
                'penalty_attempts', 'yellow_cards', 'red_cards', 'expected_goals', 
                'non_penalty_xg', 'expected_assists', 'npxg_plus_xag', 'progressive_carries',
                'progressive_passes', 'progressive_receives', 'goals_assists_minus_pk', 'xg_plus_xag'
            ],
            'stats_shooting': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'goals', 'shots', 'shots_on_target',
                'shots_on_target_pct', 'shots_per_90', 'shots_on_target_per_90', 'goals_per_shot',
                'goals_per_shot_on_target', 'avg_distance', 'free_kicks', 'penalty_goals',
                'penalty_attempts', 'expected_goals', 'non_penalty_xg', 'npxg_per_shot',
                'goals_minus_xg', 'non_penalty_goals_minus_xg'
            ],
            'stats_passing': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'passes_completed', 'passes_attempted',
                'pass_completion_pct', 'total_distance', 'progressive_distance', 'assists',
                'expected_assists', 'expected_assists_minus_xag', 'key_passes', 'passes_into_final_third',
                'passes_into_penalty_area', 'crosses_into_penalty_area', 'progressive_passes'
            ],
            'stats_defense': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'tackles', 'tackles_won',
                'tackles_def_third', 'tackles_mid_third', 'tackles_att_third', 'tackle_attempts',
                'tackle_pct', 'tackles_lost', 'blocks', 'blocked_shots', 'blocked_passes',
                'interceptions', 'tackles_plus_interceptions', 'clearances', 'errors'
            ],
            'stats_possession': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'touches', 'touches_def_pen',
                'touches_def_third', 'touches_mid_third', 'touches_att_third', 'touches_att_pen',
                'touches_live', 'tackle_attempts', 'tackles_successful', 'tackle_success_pct',
                'tackles_defeated', 'tackles_defeated_pct', 'carries', 'carry_distance',
                'carry_progressive_distance', 'progressive_carries', 'carries_into_final_third',
                'carries_into_penalty_area', 'miscontrols', 'dispossessed', 'receptions',
                'progressive_receives'
            ],
            'stats_playing_time': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'matches_played', 'minutes', 'minutes_per_match',
                'minutes_pct', 'nineties_played', 'starts', 'minutes_per_start', 'complete_matches',
                'substitutions', 'minutes_per_sub', 'unused_sub', 'points_per_match',
                'on_goals', 'on_goals_against', 'plus_minus', 'plus_minus_per_90', 'on_off',
                'on_expected_goals', 'on_expected_goals_against', 'expected_goals_plus_minus',
                'expected_goals_plus_minus_per_90'
            ],
            'stats_misc': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'yellow_cards', 'red_cards',
                'second_yellow', 'fouls', 'fouled', 'offsides', 'crosses', 'interceptions',
                'tackles_won', 'penalties_won', 'penalties_conceded', 'own_goals',
                'ball_recoveries', 'aerials_won', 'aerials_lost', 'aerial_win_pct'
            ],
            'stats_keeper': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'matches_played', 'starts', 'minutes', 'nineties_played',
                'goals_against', 'goals_against_per_90', 'shots_on_target_against', 'saves',
                'save_pct', 'wins', 'draws', 'losses', 'clean_sheets', 'clean_sheet_pct',
                'penalty_attempts', 'penalty_goals_against', 'penalty_saves', 'penalty_missed'
            ],
            'stats_keeper_adv': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'goals_against', 'penalty_goals_against',
                'free_kick_goals_against', 'corner_kick_goals_against', 'own_goals_against',
                'post_shot_expected_goals', 'post_shot_expected_goals_per_shot', 'post_shot_expected_goals_plus_minus',
                'post_shot_expected_goals_plus_minus_per_90', 'launches', 'launch_pct',
                'avg_launch_distance', 'goal_kicks', 'crosses_faced', 'crosses_stopped',
                'cross_stop_pct', 'defensive_actions_outside_penalty_area', 'defensive_actions_outside_penalty_area_per_90',
                'avg_distance_defensive_actions'
            ],
            'stats_gca': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'shot_creating_actions', 'shot_creating_actions_per_90',
                'shot_creating_actions_live_pass', 'shot_creating_actions_dead_pass', 'shot_creating_actions_take_on',
                'shot_creating_actions_shot', 'shot_creating_actions_foul_drawn', 'goal_creating_actions',
                'goal_creating_actions_per_90', 'goal_creating_actions_live_pass', 'goal_creating_actions_dead_pass',
                'goal_creating_actions_take_on', 'goal_creating_actions_shot', 'goal_creating_actions_foul_drawn'
            ],
            'stats_passing_types': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'passes_attempted', 'passes_live',
                'passes_dead', 'passes_free_kick', 'passes_through_ball', 'passes_switched',
                'passes_crosses', 'passes_throw_in', 'passes_corner_kick', 'passes_in',
                'passes_out', 'passes_straight', 'passes_completed', 'passes_offsides',
                'passes_blocks'
            ],
            'stats_passing': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'passes_completed', 'passes_attempted',
                'pass_completion_pct', 'total_distance', 'progressive_distance', 'assists',
                'expected_assists', 'expected_assists_minus_xag', 'key_passes', 'passes_into_final_third',
                'passes_into_penalty_area', 'crosses_into_penalty_area', 'progressive_passes'
            ],
            'stats_gca': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'shot_creating_actions', 'shot_creating_actions_per_90',
                'shot_creating_actions_live_pass', 'shot_creating_actions_dead_pass', 'shot_creating_actions_take_on',
                'shot_creating_actions_shot', 'shot_creating_actions_foul_drawn', 'goal_creating_actions',
                'goal_creating_actions_per_90'
            ],
            'stats_keeper_adv': [
                'rank', 'player_name', 'nation', 'position', 'squad', 'competition',
                'age', 'birth_year', 'nineties_played', 'goals_against', 'penalty_kicks_allowed',
                'free_kick_goals_against', 'corner_kick_goals_against', 'own_goals_against',
                'post_shot_expected_goals', 'post_shot_expected_goals_per_shot', 'post_shot_expected_goals_plus_minus',
                'post_shot_expected_goals_plus_minus_per_90', 'launches', 'launch_pct',
                'avg_launch_distance', 'goal_kicks', 'crosses_faced', 'crosses_stopped',
                'cross_stop_pct', 'defensive_actions_outside_penalty_area', 'defensive_actions_outside_penalty_area_per_90',
                'avg_distance_defensive_actions'
            ]
        }
        
        return standard_orders.get(table_type, [])

    # ---------------------- helper IO / normalization utils ------------------
    def _normalize_column_name(self, name: str) -> str:
        """Normalize a single column name: strip whitespace, remove NBSPs and collapse spaces."""
        if name is None:
            return name
        s = str(name)
        # remove BOM / NBSP and unify whitespace
        s = s.replace('\ufeff', '').replace('\xa0', ' ')
        s = s.strip()
        s = re.sub(r"\s+", ' ', s)
        return s

    def _make_unique_columns(self, cols: List[str]) -> List[str]:
        """Make column names unique by appending .1, .2... to duplicates (preserve first occurrence).
        Returns new list of column names.
        """
        seen = {}
        out = []
        for c in cols:
            base = c
            if base in seen:
                seen[base] += 1
                new = f"{base}.{seen[base] - 1}"
                # ensure the new name isn't already used
                i = 1
                while new in seen:
                    new = f"{base}.{seen[base] - 1}.{i}"
                    i += 1
                out.append(new)
                seen[new] = 1
            else:
                out.append(base)
                seen[base] = 1
        return out

    def _read_csv_smart(self, file_path: Path) -> pd.DataFrame:
        """Read a CSV with heuristics to detect and fix double-headers and encoding issues.
        Strategy:
        - try utf-8-sig read first
        - inspect first data row: if many values match column names, assume a repeated header and re-read with skiprows=1
        - normalize encodings by falling back to latin1 on failure
        """
        # first attempt
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(file_path, encoding='latin1')

        # quick double-header detection: if the first data row contains many tokens equal to column names
        try:
            if len(df) >= 1 and len(df.columns) > 1:
                first_row = df.iloc[0].astype(str).map(lambda x: self._normalize_column_name(x)).tolist()
                cols = [self._normalize_column_name(c) for c in df.columns.astype(str).tolist()]
                intersection = set(first_row) & set(cols)
                # if >= half of the columns appear in the first row, consider it a repeated header
                if len(intersection) >= max(2, len(cols) // 2):
                    # re-read skipping the first row which likely contains the second header
                    try:
                        df = pd.read_csv(file_path, encoding='utf-8-sig', skiprows=1)
                    except Exception:
                        df = pd.read_csv(file_path, encoding='latin1', skiprows=1)
        except Exception:
            # if anything goes wrong with heuristics, return what we have
            pass

        # Normalize column names in place
        df.columns = [self._normalize_column_name(c) for c in df.columns.astype(str).tolist()]
        return df

    # ---------------------- numeric detection & parsing ----------------------
    def _looks_numeric(self, s: pd.Series, threshold: float = 0.75) -> bool:
        """Return True if a reasonable fraction of non-null values in the series look numeric.
        This strips common formatting (commas, percent signs, parentheses) before testing.
        """
        non_null = s.dropna().astype(str).str.strip()
        if len(non_null) == 0:
            return False
        sample = non_null.head(1000)
        cleaned = sample.str.replace(r"[\,\%\u2014\-]", '', regex=True)
        cleaned = cleaned.str.replace(r'^\((.*)\)$', r'-\1', regex=True)
        numeric_like = cleaned.str.match(r'^-?\d*\.?\d+$')
        return float(numeric_like.sum()) / float(len(numeric_like)) >= threshold

    def _parse_numeric_series(self, s: pd.Series) -> pd.Series:
        """Robust numeric parser: remove thousands separators, handle parentheses as negatives, convert percents."""
        s2 = s.astype(str).str.strip()
        s2 = s2.replace({'': pd.NA, 'nan': pd.NA, 'N/A': pd.NA, 'n/a': pd.NA, '—': pd.NA, '-': pd.NA})
        # parentheses -> negative
        s2 = s2.str.replace(r'^\((.*)\)$', r'-\1', regex=True)
        # remove thousands separators
        s2 = s2.str.replace(',', '', regex=False)
        # detect percent
        pct_mask = s2.str.endswith('%') & s2.notna()
        s_clean = s2.str.rstrip('%')
        out = pd.to_numeric(s_clean, errors='coerce')
        if pct_mask.any():
            out.loc[pct_mask] = out.loc[pct_mask] / 100.0
        return out

    def standardize_column_names(self, df: pd.DataFrame, table_type: str) -> pd.DataFrame:
        """
        Standardize column names for consistency, with table-type-specific mapping for ambiguous columns.
        Only unambiguous columns are mapped globally, ambiguous short labels (like 'Att', 'Cmp', 'Cmp%')
        are mapped only within their tables.
        """
        df_clean = df.copy()

        # Table-type-specific mappings for ambiguous columns
        TABLE_COLUMN_MAPPINGS = {
            'stats_passing': {
                'Cmp': 'passes_completed',
                'Att': 'passes_attempted',
                'Cmp%': 'pass_completion_pct',
                'TotDist': 'total_distance',
                'PrgDist': 'progressive_distance',
                'Ast': 'assists',
                'xAG': 'expected_assists',
                'xA': 'expected_assists_xa',
                'A-xAG': 'expected_assists_minus_xag',
                'KP': 'key_passes',
                '1/3': 'passes_into_final_third',
                'PPA': 'passes_into_penalty_area',
                'CrsPA': 'crosses_into_penalty_area',
                'PrgP': 'progressive_passes',
            },
            'stats_keeper_adv': {
                'Cmp': 'launches',
                'Att': 'launch_pct',
                'Cmp%': 'avg_launch_distance',
                'Att (GK)': 'goal_kicks',
                'Thr': 'crosses_faced',
                'Launch%': 'crosses_stopped',
                'AvgLen': 'cross_stop_pct',
                # UNIQUE NAMES BELOW:
                'Opp': 'opponent_penalty_area_entries_stopped',
                'Stp': 'keeper_stops',
                'Stp%': 'keeper_stop_pct',
                '#OPA': 'keeper_opa',
                '#OPA/90': 'keeper_opa_per_90',
                'AvgDist': 'keeper_avg_distance',
                'GA': 'goals_against',
                'PKA': 'penalty_kicks_allowed',
                'FK': 'free_kick_goals_against',
                'CK': 'corner_kick_goals_against',
                'OG': 'own_goals_against',
                'PSxG': 'post_shot_expected_goals',
                'PSxG/SoT': 'post_shot_expected_goals_per_shot',
                'PSxG+/-': 'post_shot_expected_goals_plus_minus',
                '/90': 'post_shot_expected_goals_plus_minus_per_90'
            },
            'stats_passing_types': {
                'Att': 'passes_attempted',
                'Live': 'passes_live',
                'Dead': 'passes_dead',
                'FK': 'passes_free_kick',
                'TB': 'passes_through_ball',
                'Sw': 'passes_switched',
                'Crs': 'passes_crosses',
                'TI': 'passes_throw_in',
                'CK': 'passes_corner_kick',
                'In': 'passes_in',
                'Out': 'passes_out',
                'Str': 'passes_straight',
                'Cmp': 'passes_completed',
                'Off': 'passes_offsides',
                'Blocks': 'passes_blocks',
            }
        }

        # Global mapping for universal columns (NO ambiguous mappings 'Cmp', 'Att', etc.)
        column_mapping = {
            'Rk': 'rank',
            'Player': 'player_name',
            'Nation': 'nation',
            'Pos': 'position',
            'Squad': 'squad',
            'Comp': 'competition',
            'Age': 'age',
            'Born': 'birth_year',
            '90s': 'nineties_played',
            'Gls': 'goals',
            'Ast': 'assists',
            'G+A': 'goals_assists',
            'G-PK': 'goals_minus_pk',
            'PK': 'penalty_goals',
            'PKatt': 'penalty_attempts',
            'CrdY': 'yellow_cards',
            'CrdR': 'red_cards',
            'xG': 'expected_goals',
            'npxG': 'non_penalty_xg',
            'xAG': 'expected_assists',
            'npxG+xAG': 'npxg_plus_xag',
            'PrgC': 'progressive_carries',
            'PrgP': 'progressive_passes',
            'PrgR': 'progressive_receives',
            'G+A-PK': 'goals_assists_minus_pk',
            'xG+xAG': 'xg_plus_xag',
            
            # Shooting stats
            'Sh': 'shots',
            'SoT': 'shots_on_target',
            'SoT%': 'shots_on_target_pct',
            'Sh/90': 'shots_per_90',
            'SoT/90': 'shots_on_target_per_90',
            'G/Sh': 'goals_per_shot',
            'G/SoT': 'goals_per_shot_on_target',
            'Dist': 'avg_distance',
            'FK': 'free_kicks',
            'npxG/Sh': 'npxg_per_shot',
            'G-xG': 'goals_minus_xg',
            'np:G-xG': 'non_penalty_goals_minus_xg',
            
            # Passing stats
            'Cmp': 'passes_completed',
            'Att': 'passes_attempted',
            'Cmp%': 'pass_completion_pct',
            'TotDist': 'total_distance',
            'PrgDist': 'progressive_distance',
            'xA': 'expected_assists',
            'A-xAG': 'expected_assists_minus_xag',
            'KP': 'key_passes',
            '1/3': 'passes_into_final_third',
            'PPA': 'passes_into_penalty_area',
            'CrsPA': 'crosses_into_penalty_area',
            
            # Defense stats
            'Tkl': 'tackles',
            'TklW': 'tackles_won',
            'Def 3rd': 'tackles_def_third',
            'Mid 3rd': 'tackles_mid_third',
            'Att 3rd': 'tackles_att_third',
            'Att': 'tackle_attempts',
            'Tkl%': 'tackle_pct',
            'Lost': 'tackles_lost',
            'Blocks': 'blocks',
            'Sh': 'blocked_shots',
            'Pass': 'blocked_passes',
            'Int': 'interceptions',
            'Tkl+Int': 'tackles_plus_interceptions',
            'Clr': 'clearances',
            'Err': 'errors',
            
            # Possession stats
            'Touches': 'touches',
            'Def Pen': 'touches_def_pen',
            'Def 3rd': 'touches_def_third',
            'Mid 3rd': 'touches_mid_third',
            'Att 3rd': 'touches_att_third',
            'Att Pen': 'touches_att_pen',
            'Live': 'touches_live',
            'Succ': 'tackles_successful',
            'Succ%': 'tackle_success_pct',
            'Tkld': 'tackles_defeated',
            'Tkld%': 'tackles_defeated_pct',
            'Carries': 'carries',
            'TotDist': 'carry_distance',
            'PrgDist': 'carry_progressive_distance',
            'PrgC': 'progressive_carries',
            '1/3': 'carries_into_final_third',
            'CPA': 'carries_into_penalty_area',
            'Mis': 'miscontrols',
            'Dis': 'dispossessed',
            'Rec': 'receptions',
            'PrgR': 'progressive_receives',
            
            # Playing time stats
            'Mn/MP': 'minutes_per_match',
            'Min%': 'minutes_pct',
            'Mn/Start': 'minutes_per_start',
            'Compl': 'complete_matches',
            'Subs': 'substitutions',
            'Mn/Sub': 'minutes_per_sub',
            'unSub': 'unused_sub',
            'PPM': 'points_per_match',
            'onG': 'on_goals',
            'onGA': 'on_goals_against',
            '+/-': 'plus_minus',
            '+/-90': 'plus_minus_per_90',
            'On-Off': 'on_off',
            'onxG': 'on_expected_goals',
            'onxGA': 'on_expected_goals_against',
            'xG+/-': 'expected_goals_plus_minus',
            'xG+/-90': 'expected_goals_plus_minus_per_90',
            
            # Misc stats
            '2CrdY': 'second_yellow',
            'Fls': 'fouls',
            'Fld': 'fouled',
            'Off': 'offsides',
            'Crs': 'crosses',
            'Int': 'interceptions',
            'TklW': 'tackles_won',
            'PKwon': 'penalties_won',
            'PKcon': 'penalties_conceded',
            'OG': 'own_goals',
            'Recov': 'ball_recoveries',
            'Won': 'aerials_won',
            'Lost': 'aerials_lost',
            'Won%': 'aerial_win_pct',
            
            # Keeper stats
            'GA': 'goals_against',
            'GA90': 'goals_against_per_90',
            'SoTA': 'shots_on_target_against',
            'Saves': 'saves',
            'Save%': 'save_pct',
            'W': 'wins',
            'D': 'draws',
            'L': 'losses',
            'CS': 'clean_sheets',
            'CS%': 'clean_sheet_pct',
            'PKA': 'penalty_goals_against',
            'PKsv': 'penalty_saves',
            'PKm': 'penalty_missed',
            
            # Advanced keeper stats
            'PSxG': 'post_shot_expected_goals',
            'PSxG/SoT': 'post_shot_expected_goals_per_shot',
            'PSxG+/-': 'post_shot_expected_goals_plus_minus',
            '/90': 'post_shot_expected_goals_plus_minus_per_90',
            'Cmp': 'launches',
            'Att': 'launch_pct',
            'Cmp%': 'avg_launch_distance',
            'Att (GK)': 'goal_kicks',
            'Thr': 'crosses_faced',
            'Launch%': 'crosses_stopped',
            'AvgLen': 'cross_stop_pct',
            'Opp': 'defensive_actions_outside_penalty_area',
            'Stp': 'defensive_actions_outside_penalty_area_per_90',
            'Stp%': 'avg_distance_defensive_actions',
            '#OPA': 'defensive_actions_outside_penalty_area',
            '#OPA/90': 'defensive_actions_outside_penalty_area_per_90',
            'AvgDist': 'avg_distance_defensive_actions',
            
            # Additional keeper advanced columns
            'PKA': 'penalty_kicks_allowed',
            'FK': 'free_kick_goals_against',
            'CK': 'corner_kick_goals_against',
            'OG': 'own_goals_against',
            
            # Goal and shot creating actions
            'SCA': 'shot_creating_actions',
            'SCA90': 'shot_creating_actions_per_90',
            'PassLive': 'shot_creating_actions_live_pass',
            'PassDead': 'shot_creating_actions_dead_pass',
            'TO': 'shot_creating_actions_take_on',
            'Sh': 'shot_creating_actions_shot',
            'Fld': 'shot_creating_actions_foul_drawn',
            'Def': 'goal_creating_actions_def',
            'GCA': 'goal_creating_actions',
            'GCA90': 'goal_creating_actions_per_90',
            
            # Passing types
            'Live': 'passes_live',
            'Dead': 'passes_dead',
            'FK': 'passes_free_kick',
            'TB': 'passes_through_ball',
            'Sw': 'passes_switched',
            'Crs': 'passes_crosses',
            'TI': 'passes_throw_in',
            'CK': 'passes_corner_kick',
            'In': 'passes_in',
            'Out': 'passes_out',
            'Str': 'passes_straight',
            'Cmp': 'passes_completed',
            'Off': 'passes_offsides',
            'Blocks': 'passes_blocks',
            
            # Additional passing types columns
            'Att': 'passes_attempted',
            
            # Additional GCA columns
            'TO': 'shot_creating_actions_take_on',
            'Sh': 'shot_creating_actions_shot',
            'Fld': 'shot_creating_actions_foul_drawn'
        }

        # Apply the mapping
        table_specific_map = TABLE_COLUMN_MAPPINGS.get(table_type, {})
        rename_dict = {}
        for col in df_clean.columns:
            if col in table_specific_map:
                rename_dict[col] = table_specific_map[col]
            elif col in column_mapping:
                rename_dict[col] = column_mapping[col]
        df_clean = df_clean.rename(columns=rename_dict)
        return df_clean
    
    def clean_numeric_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize numeric data"""
        df_clean = df.copy()

        # Define candidate text columns (keep as text).
        text_columns = ['player_name', 'nation', 'position', 'squad', 'competition']

        for col in df_clean.columns:
            if col in text_columns:
                # leave text columns alone (but ensure stripped strings)
                df_clean[col] = df_clean[col].where(df_clean[col].notna(), None)
                continue

            # Heuristic: if column looks numeric, parse robustly, otherwise leave as-is
            try:
                if self._looks_numeric(df_clean[col]):
                    parsed = self._parse_numeric_series(df_clean[col])
                    if self.fillna_numeric:
                        parsed = parsed.fillna(0)
                    df_clean[col] = parsed
                else:
                    # leave non-numeric-looking columns untouched (but normalize strings)
                    df_clean[col] = df_clean[col].astype(str).str.strip().replace({'nan': None})
            except Exception:
                # fallback: try a best-effort numeric coercion
                parsed = pd.to_numeric(df_clean[col], errors='coerce')
                if self.fillna_numeric:
                    parsed = parsed.fillna(0)
                df_clean[col] = parsed

        return df_clean
    
    def clean_text_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize text data"""
        df_clean = df.copy()
        
        text_columns = ['player_name', 'nation', 'position', 'squad', 'competition']
        
        for col in text_columns:
            if col in df_clean.columns:
                # Convert to string and strip whitespace
                df_clean[col] = df_clean[col].astype(str).str.strip()
                
                # Replace 'nan' strings with empty string
                df_clean[col] = df_clean[col].replace('nan', '')
                
                # Handle empty strings
                df_clean[col] = df_clean[col].replace('', None)
        
        # Special cleaning for nation and competition columns
        if 'nation' in df_clean.columns:
            # Remove lowercase country codes (e.g., "eng ENG" -> "ENG")
            df_clean['nation'] = df_clean['nation'].str.replace(r'^[a-z]{2,3}\s+', '', regex=True)
        
        if 'competition' in df_clean.columns:
            # Remove lowercase country codes (e.g., "eng Premier League" -> "Premier League")
            df_clean['competition'] = df_clean['competition'].str.replace(r'^[a-z]{2,3}\s+', '', regex=True)
        
        return df_clean
    
    def drop_redundant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop redundant columns that don't add value"""
        df_clean = df.copy()
        
        # Drop the "Matches" column as it's redundant (all rows are "Matches")
        if 'Matches' in df_clean.columns:
            df_clean = df_clean.drop(columns=['Matches'])
            logger.info("Dropped redundant 'Matches' column")
        
        return df_clean
    
    def standardize_column_order(self, df: pd.DataFrame, table_type: str) -> pd.DataFrame:
        """Ensure consistent column order and add missing columns"""
        df_clean = df.copy()
        
        # Get standard column order for this table type
        standard_order = self.get_standard_column_order(table_type)
        
        # If no standard order defined, return as is
        if not standard_order:
            logger.warning(f"No standard column order defined for {table_type}, keeping original order")
            return df_clean
        
        # Add missing columns with default values
        for col in standard_order:
            if col not in df_clean.columns:
                if col in ['season', 'table_type']:
                    df_clean[col] = None  # Will be filled later
                elif col == 'processed_at':
                    df_clean[col] = None  # Will be filled later
                else:
                    df_clean[col] = 0  # Default numeric value
        
        # Reorder columns according to standard order, but only include columns that exist
        existing_standard_cols = [col for col in standard_order if col in df_clean.columns]
        extra_cols = [col for col in df_clean.columns if col not in standard_order]
        
        # Combine standard order columns with extra columns
        final_column_order = existing_standard_cols + extra_cols
        
        # Reorder the dataframe
        df_clean = df_clean[final_column_order]
        
        return df_clean

    def add_metadata(self, df: pd.DataFrame, season: str, table_type: str) -> pd.DataFrame:
        """Add metadata columns for staging"""
        df_clean = df.copy()
        
        # Add metadata columns
        df_clean['season'] = season
        df_clean['table_type'] = table_type
        df_clean['processed_at'] = pd.Timestamp.now()
        
        return df_clean
    
    def process_table_file(self, file_path: Path, season: str) -> pd.DataFrame:
        """Process a single table file for staging"""
        logger.info(f"Processing {file_path.name} for season {season}")
        report = {
            'file': str(file_path),
            'rows_in': None,
            'rows_out': None,
            'cols_in': None,
            'cols_out': None,
            'duplicate_columns': [],
            'notes': []
        }

        try:
            # Read CSV with heuristics
            df = self._read_csv_smart(file_path)
            report['rows_in'] = int(len(df))
            report['cols_in'] = int(len(df.columns))

            # Detect duplicate columns
            dup_mask = df.columns.duplicated()
            if dup_mask.any():
                dup_cols = list(pd.Index(df.columns)[dup_mask].tolist())
                report['duplicate_columns'] = dup_cols
                logger.warning(f"Duplicate columns detected in {file_path.name}: {dup_cols}")
                # make unique column names to avoid ambiguous selection
                df.columns = self._make_unique_columns(df.columns.tolist())
                report['notes'].append('made column names unique by appending suffixes')

            # Extract table type from filename
            table_type = file_path.stem.replace(f"{season} ", "").replace(" ", "_")

            # Clean data with proper column ordering
            df_clean = self.drop_redundant_columns(df)
            df_clean = self.standardize_column_names(df_clean, table_type)
            df_clean = self.clean_numeric_data(df_clean)
            df_clean = self.clean_text_data(df_clean)
            df_clean = self.standardize_column_order(df_clean, table_type)
            df_clean = self.add_metadata(df_clean, season, table_type)

            # Remove rows with missing player names (if column exists)
            if 'player_name' in df_clean.columns:
                before = len(df_clean)
                df_clean = df_clean.dropna(subset=['player_name'])
                after = len(df_clean)
                if before != after:
                    report['notes'].append(f'dropped {before-after} rows missing player_name')

            report['rows_out'] = int(len(df_clean))
            report['cols_out'] = int(len(df_clean.columns))

            # write per-file report
            try:
                report_file = self.reports_path / (file_path.stem + '.processing.json')
                with open(report_file, 'w') as rf:
                    json.dump(report, rf, indent=2)
            except Exception as e:
                logger.warning(f"Unable to write processing report for {file_path}: {e}")

            logger.info(f"Processed {len(df_clean)} rows for {table_type} with {len(df_clean.columns)} columns")
            return df_clean

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            # attempt to write report with error
            report['error'] = str(e)
            try:
                report_file = self.reports_path / (file_path.stem + '.processing.json')
                with open(report_file, 'w') as rf:
                    json.dump(report, rf, indent=2)
            except Exception:
                pass
            return pd.DataFrame()
    
    def generate_staging_schema(self, df: pd.DataFrame, table_name: str) -> Dict[str, str]:
        """Generate PostgreSQL schema for staging table"""
        schema = {}
        
        for col in df.columns:
            if col in ['player_name', 'nation', 'position', 'squad', 'competition']:
                schema[col] = 'VARCHAR(255)'
            elif col in ['season', 'table_type']:
                schema[col] = 'VARCHAR(50)'
            elif col == 'processed_at':
                schema[col] = 'TIMESTAMP'
            elif col in ['rank', 'age', 'birth_year', 'matches_played', 'starts', 'minutes', 
                        'yellow_cards', 'red_cards', 'penalty_goals', 'penalty_attempts']:
                schema[col] = 'INTEGER'
            else:
                # All other numeric columns
                schema[col] = 'DECIMAL(10,2)'
        
        return schema
    
    def process_season_for_staging(self, season: str) -> Dict[str, Any]:
        """Process all tables for a season into staging format"""
        # raw CSVs are expected at raw_data/<season>/*.csv
        season_path = self.raw_data_path / season
        
        if not season_path.exists():
            logger.warning(f"Tables directory not found for season {season}")
            return {}
        
        processed_tables = {}
        
        # Process each table file
        for table_file in season_path.glob("*.csv"):
            df_processed = self.process_table_file(table_file, season)
            
            if not df_processed.empty:
                table_name = table_file.stem.replace(f"{season} ", "").replace(" ", "_")
                
                # Generate schema
                schema = self.generate_staging_schema(df_processed, table_name)
                
                processed_tables[table_name] = {
                    'data': df_processed,
                    'schema': schema,
                    'season': season
                }
        
        return processed_tables
    
    def save_staging_data(self, processed_tables: Dict[str, Any], season: str):
        """Save processed data in staging format"""
        season_dir = self.staging_data_path / season
        season_dir.mkdir(exist_ok=True)
        
        for table_name, table_data in processed_tables.items():
            # Save CSV
            # filename format: stg_<table_name>_<season>.csv
            csv_file = season_dir / f"stg_{table_name}_{season}.csv"
            table_data['data'].to_csv(csv_file, index=False)
            
            # Save schema
            schema_file = season_dir / f"{table_name}_schema.json"
            with open(schema_file, 'w') as f:
                json.dump(table_data['schema'], f, indent=2)
            
            # Save metadata
            metadata = self.get_table_metadata(table_name)
            metadata_file = season_dir / f"{table_name}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Saved staging data: {csv_file}")
    
    def create_staging_summary(self, all_processed_tables: Dict[str, Dict[str, Any]]):
        """Create a summary of all processed tables"""
        summary = {
            'total_seasons': len(all_processed_tables),
            'total_tables': sum(len(tables) for tables in all_processed_tables.values()),
            'seasons': {},
            'table_types': set()
        }
        
        for season, tables in all_processed_tables.items():
            summary['seasons'][season] = {
                'table_count': len(tables),
                'tables': list(tables.keys())
            }
            
            for table_name in tables.keys():
                summary['table_types'].add(table_name)
        
        summary['table_types'] = list(summary['table_types'])
        
        # Save summary
        summary_file = self.staging_data_path / "staging_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Created staging summary: {summary_file}")
        return summary
    
    def process_all_seasons_for_staging(self):
        """Process all seasons for staging"""
        # Ensure raw_data_path exists; if not, attempt a sensible fallback
        if not self.raw_data_path.exists():
            alt = Path.cwd() / 'data' / 'raw'
            if alt.exists():
                logger.warning(f"Configured raw_data_path {self.raw_data_path} not found. Using fallback {alt}")
                self.raw_data_path = alt
            else:
                logger.error(f"Raw data path not found: {self.raw_data_path} and fallback {alt} does not exist.")
                return {}

        seasons = [d.name for d in self.raw_data_path.iterdir() 
                  if d.is_dir() and d.name not in ['pipeline_test', 'pipeline_test_no_cs']]

        logger.info(f"Processing seasons for staging: {seasons}")
        
        all_processed_tables = {}
        
        for season in seasons:
            logger.info(f"Processing season {season} for staging...")
            processed_tables = self.process_season_for_staging(season)
            
            if processed_tables:
                all_processed_tables[season] = processed_tables
                self.save_staging_data(processed_tables, season)
        
        # Create summary
        summary = self.create_staging_summary(all_processed_tables)
        
        logger.info("Staging data processing completed!")
        logger.info(f"Processed {summary['total_seasons']} seasons, {summary['total_tables']} tables")
        
        return all_processed_tables

def main():
    # Configuration
    raw_data_path = "/workspaces/NextGenEUPlayers/raw_data"
    
    
    # Initialize processor
    processor = StagingDataProcessor(raw_data_path)
    
    # Process all seasons for staging
    processor.process_all_seasons_for_staging()

if __name__ == "__main__":
    main()
