import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scipy.spatial.distance import cdist
import numpy as np
import requests
import pycountry
import unicodedata

# --- CONFIGURATION ---
st.set_page_config(page_title="NextGen Scout Pro", page_icon="⚽", layout="wide")

# --- ASSET HELPERS ---
@st.cache_data(ttl=3600)
def get_team_badge(team_name):
    if not team_name: return None
    try:
        # TheSportsDB Free API (Key: 3)
        url = f"https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t={team_name}"
        r = requests.get(url, timeout=2)
        data = r.json()
        if data and data.get('teams'):
            return data['teams'][0]['strBadge']
    except:
        return None
    return None

@st.cache_data
def get_flag_url(country_name):
    if not country_name: return None
    
    # 1. Clean the input if it looks like "eng ENG"
    clean_name = country_name
    parts = country_name.split()
    
    # Robust extraction: Look for the uppercase code at the end
    if len(parts) >= 2:
        last_part = parts[-1]
        if last_part.isupper() and len(last_part) == 3:
            clean_name = last_part # "ENG", "USA", "FRA"

    # 2. Manual overrides for non-standard ISO names or Codes
    overrides = {
        "ENG": "gb-eng", "England": "gb-eng",
        "SCT": "gb-sct", "Scotland": "gb-sct",
        "WLS": "gb-wls", "Wales": "gb-wls",
        "NIR": "gb-nir", "Northern Ireland": "gb-nir",
        "USA": "us", "United States": "us",
        "KOR": "kr", "Korea Republic": "kr",
        "CZE": "cz", "Czech Republic": "cz",
        "TUR": "tr", "Türkiye": "tr"
    }
    
    code = overrides.get(clean_name)
    
    if not code:
        try:
            # Try looking up by Alpha-3 (e.g. FRA, DEU)
            if len(clean_name) == 3:
                c = pycountry.countries.get(alpha_3=clean_name)
                if c: return f"https://flagcdn.com/h24/{c.alpha_2.lower()}.png"

            # Fuzzy search
            res = pycountry.countries.search_fuzzy(clean_name)
            if res: code = res[0].alpha_2.lower()
        except:
            pass
    
    return f"https://flagcdn.com/h24/{code}.png" if code else None

def normalize_name(name):
    if not isinstance(name, str): return str(name)
    # Manual overrides for common non-decomposing chars (Turkish, etc.)
    name = name.replace("ı", "i").replace("İ", "I").replace("ø", "o").replace("ß", "ss")
    # Normalize to NFD form (decomposing characters)
    nfd_form = unicodedata.normalize('NFD', name)
    # Filter out non-spacing mark characters (accents) and encode to ASCII
    return "".join(c for c in nfd_form if unicodedata.category(c) != 'Mn')

# --- DATA LOADER ---
@st.cache_resource
def get_connection():
    return duckdb.connect('data/duckdb/players.db', read_only=True)

@st.cache_data
def load_data():
    con = get_connection()
    
    # 1. Outfielders (Join with Valuation and Consistency)
    # We join the new Marts here to avoid complex Python merging later
    query = """
        SELECT 
            sa.*,
            tv.market_value_est_m_eur,
            tv.performance_tier,
            tv.squad_status,
            pc.transfer_risk_rating,
            pc.performance_volatility,
            pc.consistency_score
        FROM mart_scouting_analysis sa
        LEFT JOIN mart_transfer_valuation tv 
            ON sa.player_id = tv.player_id AND sa.season_id = tv.season_id
        LEFT JOIN mart_player_consistency pc
            ON sa.player_id = pc.player_id
        WHERE sa.minutes_90s >= 5
    """
    try:
        df_field = con.execute(query).df()
    except Exception as e:
        # Fallback to basic load if Marts are missing (during dev)
        print(f"Warning: {e}")
        df_field = con.execute("SELECT * FROM mart_scouting_analysis WHERE minutes_90s >= 5").df()

    # 2. Goalkeepers
    try:
        df_gk = con.execute("SELECT * FROM mart_goalkeeping_analysis").df()
    except:
        df_gk = pd.DataFrame() # Fallback if table doesn't exist yet
        
    # 3. Squad Profiles (New Mart)
    try:
        df_squads = con.execute("SELECT * FROM mart_squad_profile").df()
    except:
        df_squads = pd.DataFrame()
    
    # Enrich Outfielders
    df_field['scouting_role'] = df_field['primary_position'].apply(map_role)
    
    # Enrich Goalkeepers (Force Role)
    if not df_gk.empty:
        df_gk['scouting_role'] = 'Goalkeeper'
        df_gk['primary_position'] = 'GK'
    
    return df_field, df_gk, df_squads

# 1. ENRICHMENT: Map Raw Positions to Scouting Roles
def map_role(pos):
    if pd.isna(pos): return "Unknown"
    if 'GK' in pos: return "Goalkeeper"
    # Check composites first to avoid partial matching
    if 'MF' in pos and 'FW' in pos: return "Winger/AM"
    if 'DF' in pos and 'MF' in pos: return "Wingback/DM"
    # Then singles
    if 'FW' in pos: return "Attacker"
    if 'MF' in pos: return "Midfielder"
    if 'DF' in pos: return "Defender"
    return "Other"

# --- HELPER: PIZZA CHART CALCULATIONS ---
def calculate_pizza_percentiles(player_row, peer_df):
    """
    Calculates percentiles for a specific player against their peer group 
    for 4 distinct categories.
    """
    # 1. Define the Menu (The Metrics from your dbt Mart)
    # Keys = Category, Values = list of (DB Column, Display Label)
    
    if player_row['primary_position'] == 'GK':
        menu = {
            "Shot Stopping": [
                ("psxg_plus_minus_per_90", "PSxG +/-"),
                ("save_pct", "Save %"),
                ("goals_against", "Goals Conceded") # Lower is better, need to invert? Rank handles ascending=True by default (low is low rank). For GC, low is GOOD (high rank).
            ],
            "Distribution": [
                ("long_pass_completion_pct", "Long Pass %"),
            ],
            "Sweeping": [
                ("sweeper_actions_per_90", "Sweeper Actions"),
            ],
            "Command": [
                ("crosses_stopped_pct", "Crosses Stopped %"),
                ("clean_sheet_pct", "Clean Sheet %")
            ]
        }
    else:
        menu = {
            "Attacking": [
                ("npxg", "npxG"),
                ("goals", "Goals"),
                ("shots_total", "Shots"),
                ("expected_contribution_per_90", "xG+xAG"),
                ("touches_att_pen", "Touches in Box")
            ],
            "Passing": [
                ("passes_completed", "Passes Cmp"),
                ("pass_progressive_distance", "Prog. Pass Dist"),
                ("key_passes", "Key Passes"),
                ("progressive_passes", "Prog. Passes"),
                ("passes_into_penalty_area", "Passes into PA")
            ],
            "Possession": [
                ("touches", "Touches"),
                ("carry_progressive_distance", "Carry Dist"),
                ("takeons_won", "Dribbles Won"),
                ("progressive_carries", "Prog. Carries"),
                ("progression_total_dist_per_90", "Total Prog")
            ],
            "Defending": [
                ("tackles_won", "Tackles"),
                ("interceptions", "Interceptions"),
                ("recoveries", "Recoveries"),
                ("aerials_won", "Aerials Won"),
                ("defensive_actions_per_90", "Def. Actions")
            ]
        }
    
    data = []
    
    # 2. Calculate Percentile for each metric
    for category, metrics in menu.items():
        for db_col, label in metrics:
            if db_col in peer_df.columns and db_col in player_row.index:
                # Rank the whole peer group
                # pct=True gives 0.0 to 1.0. We multiply by 100 for the chart.
                
                # Special handling: For 'goals_against', Low Value = Good (High Percentile)
                # rank(ascending=False): High Value = Rank 1 (Low Pct). Low Value = Rank N (High Pct).
                # This is correct for Goals Against.
                if db_col == 'goals_against':
                     rank = peer_df[db_col].rank(pct=True, ascending=False) * 100
                else:
                     rank = peer_df[db_col].rank(pct=True) * 100
                
                # Get the specific player's value
                player_rank = rank[peer_df['player_name'] == player_row['player_name']].iloc[0]
                player_raw = player_row[db_col]
                
                data.append({
                    "Category": category,
                    "Metric": label,
                    "Percentile": player_rank,
                    "Raw": player_raw
                })
                
    return pd.DataFrame(data)

def create_pizza_chart(data, player_name, position, season):
    # Colors matching the image style
    colors = {
        "Attacking": "#FF3333",   # Red
        "Passing": "#33A8FF",     # Light Blue
        "Possession": "#FFD700",  # Gold/Yellow
        "Defending": "#663399",    # Purple
        
        # GK Colors
        "Shot Stopping": "#FF3333", # Red
        "Distribution": "#33A8FF",  # Blue
        "Sweeping": "#FFD700",      # Yellow
        "Command": "#663399"        # Purple
    }
    
    # Sort data to group slices by category
    data = data.sort_values(by="Category")
    
    fig = go.Figure()

    # Add the Bars (Slices)
    for category in data['Category'].unique():
        cat_data = data[data['Category'] == category]
        
        fig.add_trace(go.Barpolar(
            r=cat_data['Percentile'],
            theta=cat_data['Metric'],
            name=category,
            marker_color=colors[category],
            marker_line_color="black",
            marker_line_width=1,
            opacity=0.8,
            hoverinfo='text',
            # Tooltip shows Raw Value + Percentile
            text=[f"{row['Metric']}: {row['Raw']:.2f}<br>Percentile: {int(row['Percentile'])}" 
                  for _, row in cat_data.iterrows()]
        ))

    # Layout Configuration to make it look like a Pizza
    fig.update_layout(
        template="plotly_dark",
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                showticklabels=False, # Hide 20, 40, 60 numbers
                ticks=''
            ),
            angularaxis=dict(
                tickfont=dict(size=10, color="white"),
                rotation=90, # Start at top
                direction="clockwise"
            ),
            bgcolor="rgba(0,0,0,0)", # Transparent background inside
            hole=0.15 # The hole in the middle (where image goes)
        ),
        title=dict(
            text=f"{player_name}<br><sub>{position} | {season}</sub>",
            y=0.95
        ),
        legend=dict(
            orientation="h",
            y=-0.1
        ),
        margin=dict(t=80, b=50, l=40, r=40)
    )
    
    return fig

def create_comparison_radar(data_p1, data_p2, name_p1, name_p2):
    # Merge data to ensure alignment
    merged = pd.merge(
        data_p1[['Category', 'Metric', 'Percentile']], 
        data_p2[['Category', 'Metric', 'Percentile']], 
        on=['Category', 'Metric'], 
        suffixes=('_p1', '_p2')
    )
    
    # Sort by Category to keep shape consistent
    merged = merged.sort_values(by='Category')
    
    fig = go.Figure()

    # Player 1 Trace
    fig.add_trace(go.Scatterpolar(
        r=merged['Percentile_p1'],
        theta=merged['Metric'],
        fill='toself',
        name=name_p1,
        line_color='#00CC96',
        opacity=0.6
    ))

    # Player 2 Trace
    fig.add_trace(go.Scatterpolar(
        r=merged['Percentile_p2'],
        theta=merged['Metric'],
        fill='toself',
        name=name_p2,
        line_color='#AB63FA',
        opacity=0.5
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickfont=dict(color="gray"),
            ),
            angularaxis=dict(
                tickfont=dict(size=10, color="white"),
                rotation=90,
                direction="clockwise"
            ),
            bgcolor="rgba(0,0,0,0)"
        ),
        showlegend=True,
        legend=dict(orientation="h", y=-0.1),
        title=dict(text="Head-to-Head Profile (Percentile Rank)", x=0.5),
        template="plotly_dark",
        margin=dict(t=50, b=50, l=50, r=50)
    )
    
    return fig

df, df_gk, df_squads = load_data()

# --- SIDEBAR: GLOBAL FILTERS ---
st.sidebar.header("🌍 Global Filters")

# 1. Season Selection
all_seasons = sorted(df['season_id'].unique(), reverse=True)
selected_seasons = st.sidebar.multiselect("Select Seasons", all_seasons, default=[all_seasons[0]])

# 2. League Filter
all_leagues = sorted(df['competition'].unique())
selected_leagues = st.sidebar.multiselect("Select Leagues", all_leagues, default=all_leagues)

# 3. Team Filter (Dynamic based on League)
available_teams = sorted(df[df['competition'].isin(selected_leagues)]['squad'].unique())
selected_squads = st.sidebar.multiselect("Select Squads (Optional)", available_teams)

# 4. Age Filter
min_age = int(df['age'].min()) if not df.empty and not pd.isna(df['age'].min()) else 15
max_age = int(df['age'].max()) if not df.empty and not pd.isna(df['age'].max()) else 45
selected_age = st.sidebar.slider("Age Range", min_age, max_age, (min_age, max_age))

# 5. U23 Filter
u23_only = st.sidebar.checkbox("💎 U23 Prospects Only", value=False)

# 6. Scouting Role Filter
roles = ["Attacker", "Winger/AM", "Midfielder", "Defender", "Wingback/DM", "Goalkeeper"]
selected_role = st.sidebar.selectbox("Target Position", roles, index=0)

# --- FILTERING LOGIC ---
# Decide which dataframe to use based on Role
if selected_role == "Goalkeeper":
    active_df = df_gk
else:
    active_df = df

# Base filter (Seasons + Leagues)
# Note: GK table might not have 'competition' if not joined, but let's assume standard structure
if not active_df.empty:
    main_filter = (
        (active_df['season_id'].isin(selected_seasons))
    )
    # Only apply league filter if column exists (it should)
    if 'competition' in active_df.columns:
        main_filter = main_filter & (active_df['competition'].isin(selected_leagues))

    # Apply Squad filter if selected
    if selected_squads:
        main_filter = main_filter & (active_df['squad'].isin(selected_squads))

    # Apply Age filter
    if 'age' in active_df.columns:
        main_filter = main_filter & (active_df['age'] >= selected_age[0]) & (active_df['age'] <= selected_age[1])

    # Apply U23 filter
    if u23_only:
        if 'is_u23_prospect' in active_df.columns:
            # Handle boolean or 1/0
            main_filter = main_filter & (active_df['is_u23_prospect'].isin([True, 1]))
        elif 'age' in active_df.columns:
            main_filter = main_filter & (active_df['age'] <= 23)

    filtered_df = active_df[main_filter].copy()
    
    # Filter by Role for the Matrix
    role_df = filtered_df[filtered_df['scouting_role'] == selected_role].copy()
else:
    filtered_df = pd.DataFrame()
    role_df = pd.DataFrame()


# --- NAVIGATION ---
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "📊 Market Analytics"

st.markdown('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)
# Use index to control state, remove key to avoid conflict
tabs = ["📊 Market Analytics", "👤 Player Deep Dive", "⚖️ Comparison", "🕒 Data Audit"]

# SAFETY CHECK: Ensure active_tab is valid (handles renames/reloads)
if st.session_state.active_tab not in tabs:
    st.session_state.active_tab = tabs[0]

active_tab = st.radio("Navigation", tabs, 
                      index=tabs.index(st.session_state.active_tab),
                      label_visibility="collapsed")

# Sync session state if changed manually
if active_tab != st.session_state.active_tab:
    st.session_state.active_tab = active_tab
    st.rerun()

# --- KPI MAP (Used in multiple tabs) ---
kpi_map = {
    "Attacker": {
        "x": "expected_contribution_per_90", "x_label": "Exp. Goal Contrib (xG+xAG)",
        "y": "goal_contribution_per_90", "y_label": "Actual Goals + Assists",
        "size": "shots_total",
        "scoring_metrics": ["npxg", "goals", "assists", "shots_total", "touches_att_pen"]
    },
    "Winger/AM": {
        "x": "key_passes", "x_label": "Key Passes/90",
        "y": "takeons_won", "y_label": "Dribbles Won/90",
        "size": "npxg",
        "scoring_metrics": ["key_passes", "takeons_won", "npxg", "assists", "progressive_carries"]
    },
    "Midfielder": {
        "x": "pass_progressive_distance", "x_label": "Prog. Pass Distance/90",
        "y": "key_passes", "y_label": "Key Passes/90",
        "size": "minutes_90s",
        "scoring_metrics": ["pass_progressive_distance", "key_passes", "interceptions", "tackles_won", "passes_completed"]
    },
    "Defender": {
        "x": "interceptions", "x_label": "Interceptions/90",
        "y": "aerials_won", "y_label": "Aerials Won/90",
        "size": "tackles_won",
        "scoring_metrics": ["interceptions", "aerials_won", "tackles_won", "defensive_actions_per_90", "recoveries"]
    },
    "Wingback/DM": {
        "x": "tackles_won", "x_label": "Tackles Won/90",
        "y": "progression_total_dist_per_90", "y_label": "Ball Progression/90",
        "size": "interceptions",
        "scoring_metrics": ["tackles_won", "progression_total_dist_per_90", "interceptions", "key_passes", "recoveries"]
    },
    "Goalkeeper": {
        "x": "sweeper_actions_per_90", "x_label": "Sweeper Actions/90",
        "y": "psxg_plus_minus_per_90", "y_label": "PSxG +/- per 90 (Shot Stopping)",
        "size": "clean_sheet_pct",
        "scoring_metrics": ["psxg_plus_minus_per_90", "save_pct", "clean_sheet_pct", "sweeper_actions_per_90", "long_pass_completion_pct"]
    }
}

# =========================================================
# TAB 1: POSITION-BASED MARKET ANALYTICS
# =========================================================
if st.session_state.active_tab == "📊 Market Analytics":
    st.title(f"Market Analysis: {selected_role}s")
    
    # --- CONFIGURATION ---
    # Fallback to Attacker if role mapping fails
    cfg = kpi_map.get(selected_role, kpi_map["Attacker"])
    
    # 1. SMART SCORING (Keep existing logic)
    if not role_df.empty:
        score_cols = [c for c in cfg['scoring_metrics'] if c in role_df.columns]
        if score_cols:
            for c in score_cols:
                role_df[c + '_rank'] = role_df[c].rank(pct=True)
            role_df['Overall Score'] = role_df[[c + '_rank' for c in score_cols]].mean(axis=1) * 100
        else:
            role_df['Overall Score'] = 0
            
        # 2. SQUAD TIER / VALUATION (From dbt Marts)
        # Ensure columns exist (fallback for safety)
        if 'market_value_est_m_eur' not in role_df.columns:
            role_df['market_value_est_m_eur'] = 0
        if 'performance_tier' not in role_df.columns:
            role_df['performance_tier'] = "Unknown"

    # --- CONTROLS SECTION ---
    c_ctrl1, c_ctrl2, c_ctrl3 = st.columns([1, 1, 2])
    with c_ctrl1:
        # VIEW MODE: Allow switching color logic to find different types of players
        view_mode = st.selectbox(
            "🎨 View Mode", 
            ["Smart Score (Performance)", "Market Value (Moneyball)", "Age (Prospects)"],
            help="Switch coloring to identify performance, value picks, or youth."
        )
    with c_ctrl2:
        # QUADRANT LINES: Mean vs Median
        avg_mode = st.selectbox("📏 Quadrant Baseline", ["Mean", "Median"], index=0)
    
    st.divider()

    col1, col2 = st.columns([3, 1])
    
    with col1:
        if not role_df.empty:
            # --- DETERMINE COLORING LOGIC ---
            if view_mode == "Smart Score (Performance)":
                color_col = "Overall Score"
                color_scale = "Viridis"
            elif view_mode == "Market Value (Moneyball)":
                color_col = "market_value_est_m_eur"
                color_scale = "RdYlGn_r" # Green = Cheap, Red = Expensive
            else: # Age
                color_col = "age"
                color_scale = "RdYlBu" # Red = Old, Blue = Young

            # --- CALCULATE QUADRANTS ---
            x_ref = role_df[cfg['x']].mean() if avg_mode == "Mean" else role_df[cfg['x']].median()
            y_ref = role_df[cfg['y']].mean() if avg_mode == "Mean" else role_df[cfg['y']].median()

            # --- BUILD SCATTER PLOT ---
            fig = px.scatter(
                role_df,
                x=cfg['x'],
                y=cfg['y'],
                size=cfg['size'],
                color=color_col,
                symbol="is_u23_prospect",
                symbol_map={True: "diamond", False: "circle", 1: "diamond", 0: "circle"},
                hover_name="player_name",
                hover_data=["squad", "age", "season_id", "Overall Score", "market_value_est_m_eur", "performance_tier"],
                color_continuous_scale=color_scale,
                # Embed Player Name for the Click Event
                custom_data=['player_name'],
                height=600
            )

            # --- ADD QUADRANT ANNOTATIONS ---
            # Top Right (Elite)
            fig.add_shape(type="rect", x0=x_ref, y0=y_ref, x1=role_df[cfg['x']].max()*1.1, y1=role_df[cfg['y']].max()*1.1, 
                          fillcolor="green", opacity=0.05, layer="below", line_width=0)
            fig.add_annotation(x=role_df[cfg['x']].max(), y=role_df[cfg['y']].max(), text="🦄 ELITE", showarrow=False, font=dict(color="green", size=14, weight="bold"))

            # Bottom Right (Volume/Specialist)
            fig.add_shape(type="rect", x0=x_ref, y0=role_df[cfg['y']].min()*0.9, x1=role_df[cfg['x']].max()*1.1, y1=y_ref, 
                          fillcolor="orange", opacity=0.05, layer="below", line_width=0)
            
            # Top Left (Efficient)
            fig.add_shape(type="rect", x0=role_df[cfg['x']].min()*0.9, y0=y_ref, x1=x_ref, y1=role_df[cfg['y']].max()*1.1, 
                          fillcolor="blue", opacity=0.05, layer="below", line_width=0)

            # Averages Lines
            fig.add_vline(x=x_ref, line_dash="dash", line_color="grey")
            fig.add_hline(y=y_ref, line_dash="dash", line_color="grey")

            # Layout Polish
            fig.update_layout(
                template="plotly_dark",
                title=dict(text=f"<b>{selected_role} Matrix</b>: {cfg['x_label']} vs {cfg['y_label']}", font=dict(size=20)),
                xaxis=dict(title=cfg['x_label'], gridcolor='#333333'),
                yaxis=dict(title=cfg['y_label'], gridcolor='#333333'),
                plot_bgcolor='#0E1117',
                margin=dict(t=50, l=50, r=50, b=50),
                legend=dict(
                    orientation="h",
                    y=1.1,
                    x=1,
                    xanchor="right",
                    yanchor="bottom",
                    title=dict(text="U23 Prospect")
                )
            )
            
            # INTERACTIVITY
            event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points")
            
            # Click Handler (Same as before)
            if event and len(event['selection']['points']) > 0:
                selected_player = event['selection']['points'][0]['customdata'][0]
                if st.session_state.get('target_player') != selected_player or st.session_state.active_tab != "👤 Player Deep Dive":
                    st.session_state['target_player'] = selected_player
                    st.session_state['player_search_selector'] = selected_player
                    st.session_state.active_tab = "👤 Player Deep Dive"
                    st.rerun()
        else:
            st.warning("No players found.")

    with col2:
        st.markdown("### 🏆 Leaderboards")
        
        # 3. CONTEXTUAL TABS (Mini Tabs for Leaders)
        l_tab1, l_tab2 = st.tabs(["🌟 Top Rated", "💰 Moneyball"])
        
        with l_tab1:
            if not role_df.empty:
                top_rated = role_df.sort_values(by='Overall Score', ascending=False).head(10)
                st.dataframe(
                    top_rated[['player_name', 'squad', 'Overall Score']],
                    column_config={
                        "Overall Score": st.column_config.ProgressColumn("Score", format="%.0f", min_value=0, max_value=100)
                    },
                    hide_index=True, use_container_width=True
                )
        
        with l_tab2:
            # Logic: High Performance Tier + Low Market Value
            if not role_df.empty and 'performance_tier' in role_df.columns:
                # Filter for Elite/High Performers who are cheap (< 30M)
                # Note: Tiers might be case sensitive depending on dbt model output
                gems = role_df[
                    (role_df['performance_tier'].isin(['Elite', 'High Performer'])) & 
                    (role_df['market_value_est_m_eur'] < 30)
                ].sort_values(by='market_value_est_m_eur', ascending=True).head(10)
                
                if not gems.empty:
                    st.dataframe(
                        gems[['player_name', 'market_value_est_m_eur', 'performance_tier']],
                        column_config={
                            "market_value_est_m_eur": st.column_config.NumberColumn("Est. Value (€M)", format="€%.1fM"),
                            "performance_tier": "Tier"
                        },
                        hide_index=True, use_container_width=True
                    )
                else:
                    st.info("No Moneyball picks found.")

    # --- NEW SECTION: LEAGUE CONTEXT ---
    st.divider()
    st.subheader("🌍 Cross-League Calibration")
    st.caption(f"Is the '{cfg['y_label']}' stat inflated in certain leagues?")
    
    if not role_df.empty and 'competition' in role_df.columns:
        # Violin Plot to show distribution density
        fig_violin = px.violin(
            role_df, 
            x="competition", 
            y=cfg['y'], 
            box=True, # Add box plot inside
            points="all", # Show all points
            color="competition",
            hover_name="player_name",
            title=f"Distribution of {cfg['y_label']} by League"
        )
        fig_violin.update_layout(showlegend=False, xaxis_title=None)
        st.plotly_chart(fig_violin, use_container_width=True)
    elif not role_df.empty:
        st.info("League distribution data is not available for this role.")

# =========================================================
# TAB 2: PLAYER DEEP DIVE (Search & Evolve)
# =========================================================
if st.session_state.active_tab == "👤 Player Deep Dive":
    st.header("👤 Player Scouting Report")
    
    # 1. SEARCH BAR (Global Search across all loaded data)
    # We allow searching ANY player, even if not in the current filter
    # Combine names from both lists
    raw_names = sorted(list(set(df['player_name'].unique()) | set(df_gk['player_name'].unique() if not df_gk.empty else [])))
    
    # --- SEARCH NORMALIZATION ---
    # Create a mapping: Normalized -> Original
    # We use a dictionary where keys are the "searchable" versions
    # Example: "Kenan Yildiz" -> "Kenan Yıldız"
    name_map = {normalize_name(name): name for name in raw_names}
    search_options = sorted(name_map.keys())
    
    # Determine default index from session state
    # Session state stores the ORIGINAL name (e.g. "Kenan Yıldız")
    # We need to find which normalized option corresponds to it.
    default_idx = 0
    current_target = st.session_state.get('target_player')
    
    if current_target:
        # Find the normalized key for this target
        norm_target = normalize_name(current_target)
        if norm_target in search_options:
            default_idx = search_options.index(norm_target)

    # STATELESS WIDGET PATTERN
    # We remove the 'key' to prevent Streamlit from holding onto old user selections.
    # We control the widget purely via 'index'.
    selected_search = st.selectbox(
        "🔎 Search Player by Name", 
        search_options, 
        index=default_idx,
        placeholder="Type to search...",
        help="Search using standard Latin characters (e.g. 'Yildiz' finds 'Yıldız')"
    )
    
    # Resolve back to original name for DB lookups
    search_name = name_map.get(selected_search)
    
    # Sync manual selection back to session state
    # If the user changes the dropdown manually, this updates the state and reruns.
    if search_name != st.session_state.get('target_player'):
        st.session_state['target_player'] = search_name
        st.rerun()
    
    if search_name:
        # Get all records for this player
        # Priority: If player is in GK table, use that (richer metrics for GKs)
        # Otherwise use Outfield table
        p_hist = pd.DataFrame()
        if not df_gk.empty and search_name in df_gk['player_name'].unique():
             p_hist = df_gk[df_gk['player_name'] == search_name].sort_values(by='season_id')
        
        if p_hist.empty:
             p_hist = df[df['player_name'] == search_name].sort_values(by='season_id')
        
        # Load Trends Data (The new Dual-Track Model)
        @st.cache_data
        def load_trends(name):
            con = get_connection()
            safe_name = name.replace("'", "''")
            try:
                return con.execute(f"SELECT * FROM mart_player_trends WHERE player_name = '{safe_name}' ORDER BY season_id").df()
            except:
                return pd.DataFrame()

        trends_df = load_trends(search_name)

        if not p_hist.empty:
            # Get the most recent season data for the header
            p_latest = p_hist.iloc[-1]
            
            # --- ENHANCEMENT 1: HEADER & BIO CARD ---
            is_u23 = p_latest['age'] <= 23
            
            # Fetch Assets
            logo_url = get_team_badge(p_latest['squad'])
            flag_url = get_flag_url(p_latest['nation'])
            
            # Clean Nation Name
            raw_nation = str(p_latest['nation'])
            display_nation = raw_nation
            parts = raw_nation.split()
            if len(parts) >= 2 and parts[-1].isupper() and len(parts[-1]) == 3:
                code = parts[-1]
                name_map = {
                    "ENG": "England", "SCT": "Scotland", "WLS": "Wales", "NIR": "N. Ireland", 
                    "USA": "USA", "KOR": "S. Korea", "CZE": "Czechia", "TUR": "Türkiye"
                }
                display_nation = name_map.get(code, code)
                
                # Attempt Pycountry lookup if not in map
                if display_nation == code:
                    try:
                        c = pycountry.countries.get(alpha_3=code)
                        if c: display_nation = c.name
                    except:
                        pass

            # --- HTML COMPONENTS ---
            # 1. U23 Badge (Single line to prevent indentation issues)
            u23_html = ""
            if is_u23:
                u23_html = "<span style='background-color: #00CC96; color: #0E1117; padding: 8px 14px; border-radius: 6px; font-size: 1.0em; vertical-align: middle; margin-left: 15px; font-weight: bold; letter-spacing: 0.5px;'>💎 U23 PROSPECT</span>"
            
            # 2. Team Logo
            logo_html = ""
            if logo_url:
                logo_html = f"<img src='{logo_url}' style='height: 60px; vertical-align: middle; margin-right: 15px;'>"
            
            # 3. Flag
            flag_html = "🏳️"
            if flag_url:
                flag_html = f"<img src='{flag_url}' style='height: 20px; vertical-align: middle; margin-right: 4px;'>"

            # --- RENDER HEADER ---
            # IMPORTANT: HTML strings in st.markdown must NOT have indentation >= 4 spaces
            # or they will render as code blocks. We flatten the string here.
            header_html = f"""
<div style='margin-bottom: 25px;'>
<div style="display: flex; align-items: center;">
{logo_html}
<h1 style='margin: 0; padding: 0; display: inline-block; font-size: 3em;'>{p_latest['player_name']}</h1>
{u23_html}
</div>
<div style='margin-top: 5px; color: #808495; font-size: 1.2em; display: flex; align-items: center;'>
<span style='color: white; font-weight: 500;'>{p_latest['squad']}</span>
&nbsp;&nbsp;•&nbsp;&nbsp; 
{flag_html} {display_nation} 
&nbsp;&nbsp;•&nbsp;&nbsp; 
🎂 {int(p_latest['age'])} Years Old
</div>
</div>
"""
            st.markdown(header_html, unsafe_allow_html=True)
            
            # Create a "Player Card" layout
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Position", p_latest['primary_position'])
            
            # Robust Metric Handling (Handle missing columns between GK/Outfield)
            c2.metric("90s Played", f"{p_latest['minutes_90s']:.1f}")
            
            # NEW: Market Value
            val = p_latest.get('market_value_est_m_eur', 0)
            c3.metric("Est. Value", f"€{val:.1f}M")
            
            # NEW: Risk Badge
            risk = p_latest.get('transfer_risk_rating', 'Unknown')
            c4.metric("Risk Rating", risk)
            
            st.divider()

            # --- CONTEXT CALCULATION (Required for next steps) ---
            if p_latest['primary_position'] == 'GK':
                context_df = df_gk
            else:
                context_df = df
                
            peer_context = context_df[
                (context_df['season_id'] == p_latest['season_id']) & 
                (context_df['primary_position'] == p_latest['primary_position'])
            ]

            # --- ENHANCEMENT 2: AUTOMATED SCOUTING REPORT ---
            col_pizza, col_analysis = st.columns([1, 1])
            
            # Calculate Pizza Data (Reuse existing function)
            pizza_data = calculate_pizza_percentiles(p_latest, peer_context)
            
            with col_pizza:
                if not pizza_data.empty:
                    fig_pizza = create_pizza_chart(
                        pizza_data, p_latest['player_name'], p_latest['primary_position'], p_latest['season_id']
                    )
                    st.plotly_chart(fig_pizza, use_container_width=True)
                else:
                    st.warning("Insufficient data for Pizza Chart.")

            with col_analysis:
                st.subheader("📝 Scouting Notes")
                
                if not pizza_data.empty:
                    # 1. Identify Strengths & Weaknesses
                    strengths = pizza_data[pizza_data['Percentile'] >= 75]['Metric'].tolist()
                    weaknesses = pizza_data[pizza_data['Percentile'] <= 25]['Metric'].tolist()
                    
                    st.markdown("**🟢 Key Strengths:**")
                    if strengths:
                        # Display as tags
                        st.write(" • " + "\n • ".join(strengths[:5])) # Top 5
                    else:
                        st.write("*(No elite metrics detected)*")
                        
                    st.markdown("**🔴 Areas for Improvement:**")
                    if weaknesses:
                        st.write(" • " + "\n • ".join(weaknesses[:5]))
                    else:
                        st.write("*(No significant statistical weaknesses)*")
                    
                    # 2. Squad Context (Big Fish?)
                    # Compare Player's Smart Score vs Squad Average Smart Score
                    if 'Overall Score' in peer_context.columns:
                        squad_avg = peer_context[peer_context['squad'] == p_latest['squad']]['Overall Score'].mean()
                        player_score = p_latest.get('Overall Score', 0) # Assumes you calculated this in Tab 1 logic, or recalculate here
                        
                        # If you haven't calculated 'Overall Score' globally, use a proxy like 'specialist_index' from trends
                        if not trends_df.empty:
                            player_score = trends_df.iloc[-1]['specialist_index']
                            # Re-calculate peer average for specialist index
                            # (Simplified for snippet)
                            pass 



            # =========================================================
            # SECTION 4: SEASON DEEP DIVE (New Feature)
            # =========================================================
            st.subheader("📋 Season Deep Dive")
            
            # 1. Season Selector
            # Reverse sort so latest season is first
            season_options = sorted(p_hist['season_id'].unique(), reverse=True)
            selected_stat_season = st.selectbox("Select Season for Breakdown", season_options, index=0, key="stat_season_selector")
            
            # Get the specific row for that season
            s_row = p_hist[p_hist['season_id'] == selected_stat_season].iloc[0]
            
            # Helper to display metrics safely
            def display_metric(label, col_name, fmt="{:.2f}", suffix=""):
                val = s_row.get(col_name)
                if pd.notna(val):
                    st.metric(label, f"{fmt.format(val)}{suffix}")
                else:
                    st.metric(label, "-")

            # 2. Render Layout based on Position
            if 'save_pct' in s_row and pd.notna(s_row['save_pct']): 
                # --- GOALKEEPER LAYOUT ---
                gk_tab1, gk_tab2, gk_tab3 = st.tabs(["🧤 Shot Stopping", "🧹 Sweeping", "📢 Distribution"])
                
                with gk_tab1:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("PSxG +/-", "psxg_plus_minus_per_90")
                    with c2: display_metric("Save %", "save_pct", "{:.1%}")
                    with c3: display_metric("Goals Against", "goals_against", "{:.0f}")
                    with c4: display_metric("Clean Sheet %", "clean_sheet_pct", "{:.1%}")
                    
                with gk_tab2:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Sweeper Actions", "sweeper_actions_per_90")
                    with c2: display_metric("Crosses Stopped %", "crosses_stopped_pct", "{:.1%}")
                    with c3: display_metric("Avg Def. Dist", "sweeper_avgdist", "{:.1f}", " yd")
                    
                with gk_tab3:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Long Pass %", "long_pass_completion_pct", "{:.1%}")
                    with c2: display_metric("Avg Pass Len", "passes_avglen", "{:.1f}", " yd")
                    with c3: display_metric("Goal Kicks Launched", "goal_kicks_launch_pct", "{:.1%}")

            else:
                # --- OUTFIELDER LAYOUT ---
                of_tab1, of_tab2, of_tab3, of_tab4 = st.tabs(["🔫 Attacking", "🎯 Passing/Creation", "🛡️ Defense", "⚽ Possession"])
                
                with of_tab1:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Non-Pen xG", "npxg")
                    with c2: display_metric("Goals", "goals", "{:.0f}")
                    with c3: display_metric("Shots Total", "shots_total", "{:.0f}")
                    with c4: display_metric("Shots on Target", "shots_on_target", "{:.0f}")
                    
                    st.divider()
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("npxG/Shot", "npxg_per_shot")
                    with c2: display_metric("Goals - xG", "goals_vs_xg") # Calculated on fly if not in DB?
                    with c3: display_metric("Avg Shot Dist", "avg_shot_distance", "{:.1f}", " yd")
                
                with of_tab2:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Exp. Assists (xAG)", "xag")
                    with c2: display_metric("Assists", "assists", "{:.0f}")
                    with c3: display_metric("Key Passes", "key_passes", "{:.0f}")
                    with c4: display_metric("Prog. Passes", "progressive_passes", "{:.0f}")
                    
                    st.divider()
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Passes into Box", "passes_into_penalty_area", "{:.0f}")
                    with c2: display_metric("Through Balls", "through_balls", "{:.0f}")
                    with c3: display_metric("Switches", "switches", "{:.0f}")
                    with c4: display_metric("Crosses", "crosses", "{:.0f}")

                with of_tab3:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Tackles Won", "tackles_won", "{:.0f}")
                    with c2: display_metric("Interceptions", "interceptions", "{:.0f}")
                    with c3: display_metric("Blocks", "shots_blocked", "{:.0f}")
                    with c4: display_metric("Recoveries", "recoveries", "{:.0f}")
                    
                    st.divider()
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Aerials Won", "aerials_won", "{:.0f}")
                    with c2: display_metric("Aerial Win %", "aerial_win_pct", "{:.1%}") # Ensure this column exists or calc it
                    with c3: display_metric("Tackles (Att 3rd)", "tackles_att_3rd", "{:.0f}")
                    with c4: display_metric("Fouls Committed", "fouls_committed", "{:.0f}")

                with of_tab4:
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Touches", "touches", "{:.0f}")
                    with c2: display_metric("Prog. Carries", "progressive_carries", "{:.0f}")
                    with c3: display_metric("Take-ons Won", "takeons_won", "{:.0f}")
                    with c4: display_metric("Carries into Box", "carries_into_penalty_area", "{:.0f}")
                    
                    st.divider()
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: display_metric("Touches in Box", "touches_att_pen", "{:.0f}")
                    with c2: display_metric("Miscontrols", "miscontrols", "{:.0f}")
                    with c3: display_metric("Dispossessed", "dispossessed", "{:.0f}")
                    with c4: display_metric("Fouls Drawn", "fouls_drawn", "{:.0f}")

            st.divider()
            
            if not trends_df.empty:
                st.subheader("📈 Multidimensional Evolution")
                
                # 1. Determine the Label for the Specialist Trace
                pos = trends_df.iloc[0]['primary_position']
                if 'GK' in pos:
                    spec_label = "Shot Stopping (PSxG+/-)"
                elif 'DF' in pos:
                    spec_label = "Defensive Workrate (Actions/90)"
                elif 'MF' in pos:
                    spec_label = "Ball Progression (Yards/90)"
                else:
                    spec_label = "Attacking Output" # For strikers, lines overlap, which is fine

                # 2. Create Dual-Axis Chart
                fig = go.Figure()
                
                # Trace A: The Main Job (Line Chart)
                fig.add_trace(go.Scatter(
                    x=trends_df['season_id'], 
                    y=trends_df['specialist_index'], 
                    name=f"Primary Role: {spec_label}",
                    mode='lines+markers',
                    line=dict(color='#00CC96', width=3)
                ))

                # Trace B: The Goal Scoring (Bar Chart - Overlay)
                # We only show this for Outfielders
                if 'GK' not in pos:
                    fig.add_trace(go.Bar(
                        x=trends_df['season_id'], 
                        y=trends_df['scoring_index'], 
                        name="Goal Threat (xG+xAG/90)",
                        marker_color='#636EFA',
                        opacity=0.3,
                        yaxis='y2' # Plot on secondary axis
                    ))

                # Layout Updates
                fig.update_layout(
                    title=f"{search_name}: Role Performance vs Goal Threat",
                    xaxis=dict(title="Season"),
                    yaxis=dict(
                        title=dict(text=spec_label, font=dict(color="#00CC96")),
                        tickfont=dict(color="#00CC96")
                    ),
                    yaxis2=dict(
                        title=dict(text="Goal Threat (xG+xAG)", font=dict(color="#636EFA")),
                        tickfont=dict(color="#636EFA"),
                        overlaying='y',
                        side='right',
                        range=[0, 1.5] # Fix range to make comparisons easier
                    ),
                    legend=dict(x=0, y=1.1, orientation='h'),
                    hovermode="x unified"
                )
                
                st.plotly_chart(fig, use_container_width=True)

            st.divider()

            # --- ENHANCEMENT 3: SIMILARITY SEARCH (KNN) ---
            st.subheader(f"🧬 Statistical Doppelgängers ({p_latest['season_id']})")
            st.markdown(f"Players in the same role with the closest statistical profile to **{search_name}**.")
            
            if not pizza_data.empty and len(peer_context) > 5:
                # 1. Prepare the Vector Space
                # We use the raw metrics from the Pizza Menu configuration
                # Extract just the raw metric column names from the menu
                # (Re-instantiate the menu logic briefly to get col names)
                metric_cols = []
                # ... (Copy the menu dict logic from calculate_pizza_percentiles or define globally) ...
                # Quick hack: extract metric columns from the pizza_data used
                # This requires mapping Display Names back to DB columns. 
                # BETTER WAY: Use the percentile columns available in 'peer_context' if they exist, 
                # OR calculate z-scores on the fly.
                
                # Let's use the numeric columns available in peer_context
                # Filter for volume/ratio metrics, exclude IDs
                numeric_cols = peer_context.select_dtypes(include=np.number).columns.tolist()
                exclude = ['age', 'born', 'minutes_played', 'matches_played', 'starts', 'season_id', 'minutes_90s']
                feature_cols = [c for c in numeric_cols if c not in exclude and 'pct' not in c]
                
                # Fill NA
                peers_filled = peer_context[feature_cols].fillna(0)
                
                # Normalize Features (Z-Score) to ensure fair distance calculation
                # (Goals shouldn't outweigh xG just because the number is bigger)
                peers_norm = (peers_filled - peers_filled.mean()) / peers_filled.std()
                
                # Get Target Vector
                target_vector = peers_norm[peer_context['player_name'] == search_name]
                
                if not target_vector.empty:
                    # Calculate Distance to ALL peers
                    distances = cdist(target_vector, peers_norm, metric='euclidean')
                    
                    # Add to dataframe
                    sim_df = peer_context.copy()
                    sim_df['distance'] = distances[0]
                    sim_df['similarity'] = 1 / (1 + sim_df['distance']) # Convert to 0-1 Score
                    
                    # Filter out the player themselves
                    sim_df = sim_df[sim_df['player_name'] != search_name]
                    
                    # Get Top 5
                    top_matches = sim_df.sort_values(by='similarity', ascending=False).head(5)
                    
                    # Display
                    sim_cols = st.columns(5)
                    for i, (_, row) in enumerate(top_matches.iterrows()):
                        with sim_cols[i]:
                            st.metric(label=row['player_name'], value=f"{row['similarity']:.0%}", delta=row['squad'])
                            st.caption(f"{row['age']:.0f} yo | {row['nation']}")
                else:
                    st.error("Could not generate similarity vector.")
            else:
                st.info("Not enough peers to perform similarity search.")
            
        else:
            st.warning("Player found in list but no data available (likely < 5.0 90s played).")

# =========================================================
# TAB 3: COMPARISON (Enhanced)
# =========================================================
if st.session_state.active_tab == "⚖️ Comparison":
    st.subheader("⚔️ Player Comparison")
    
    # Use filtered_df to allow comparing players in the current selection
    # If empty, fallback to df
    comp_source = filtered_df if not filtered_df.empty else df
    
    # --- SEARCH NORMALIZATION FOR COMPARISON ---
    comp_raw_names = sorted(comp_source['player_name'].unique())
    comp_name_map = {normalize_name(name): name for name in comp_raw_names}
    comp_search_options = sorted(comp_name_map.keys())

    c1, c2 = st.columns(2)
    
    # Player A Selector
    p1_input = c1.selectbox("Player A", comp_search_options, index=0 if comp_search_options else None)
    p1_name = comp_name_map.get(p1_input)
    
    # Player B Selector
    # Default to 2nd item if available
    default_idx_2 = 1 if len(comp_search_options) > 1 else 0
    p2_input = c2.selectbox("Player B", comp_search_options, index=default_idx_2 if comp_search_options else None)
    p2_name = comp_name_map.get(p2_input)
    
    if p1_name and p2_name:
        # Get latest season for selected players in the filtered set
        p1_rows = comp_source[comp_source['player_name'] == p1_name]
        p2_rows = comp_source[comp_source['player_name'] == p2_name]
        
        if not p1_rows.empty and not p2_rows.empty:
            p1 = p1_rows.sort_values('season_id').iloc[-1]
            p2 = p2_rows.sort_values('season_id').iloc[-1]
            
            # --- 1. RADAR CHART (HEAD-TO-HEAD) ---
            # Define Peer Context for Percentiles (Global Context for fairness)
            def get_peer_context(player_row):
                if player_row['primary_position'] == 'GK':
                    base = df_gk
                else:
                    base = df
                return base[
                    (base['season_id'] == player_row['season_id']) & 
                    (base['primary_position'] == player_row['primary_position'])
                ]

            ctx_p1 = get_peer_context(p1)
            ctx_p2 = get_peer_context(p2)
            
            # Calculate Percentiles
            stats_p1 = calculate_pizza_percentiles(p1, ctx_p1)
            stats_p2 = calculate_pizza_percentiles(p2, ctx_p2)
            
            col_radar, col_stats = st.columns([1, 1])
            
            with col_radar:
                if not stats_p1.empty and not stats_p2.empty:
                    st.plotly_chart(create_comparison_radar(stats_p1, stats_p2, p1_name, p2_name), use_container_width=True)
                else:
                    st.warning("Insufficient data for Radar Chart.")

            with col_stats:
                st.markdown("### 📊 Statistical Edge")
                
                if not stats_p1.empty and not stats_p2.empty:
                    # --- 2. BETTER AT SUMMARY ---
                    # Merge stats to compare
                    merged = pd.merge(
                        stats_p1[['Metric', 'Percentile']], 
                        stats_p2[['Metric', 'Percentile']], 
                        on='Metric', suffixes=('_p1', '_p2')
                    )
                    
                    # Find biggest advantages
                    merged['diff'] = merged['Percentile_p1'] - merged['Percentile_p2']
                    
                    p1_adv = merged[merged['diff'] > 15].sort_values('diff', ascending=False).head(3)
                    p2_adv = merged[merged['diff'] < -15].sort_values('diff', ascending=True).head(3)
                    
                    if not p1_adv.empty:
                        st.markdown(f"**{p1_name}** is significantly better at:")
                        for _, row in p1_adv.iterrows():
                            st.markdown(f"- {row['Metric']} (+{int(row['diff'])}% percentile)")
                    
                    if not p2_adv.empty:
                        st.markdown(f"**{p2_name}** is significantly better at:")
                        for _, row in p2_adv.iterrows():
                            st.markdown(f"- {row['Metric']} (+{int(abs(row['diff']))}% percentile)")
                            
                    if p1_adv.empty and p2_adv.empty:
                        st.info("These players have very similar statistical profiles.")
                else:
                    st.warning("Insufficient data for statistical comparison.")

            st.divider()
            
            # --- 3. DETAILED COMPARISON TABLE ---
            st.subheader("📋 Detailed Metrics")
            
            if not stats_p1.empty and not stats_p2.empty:
                # Combine the stats dataframes for the table
                # We want to show Raw Value (Percentile)
                
                # Helper to format
                def fmt_stat(val, pct):
                    return f"{val:.2f} ({int(pct)}%)"
                
                # Re-merge with Raw values
                full_p1 = stats_p1.set_index('Metric')
                full_p2 = stats_p2.set_index('Metric')
                
                # Create comparison dict
                comp_data = []
                for metric in full_p1.index:
                    if metric in full_p2.index:
                        row_p1 = full_p1.loc[metric]
                        row_p2 = full_p2.loc[metric]
                        
                        comp_data.append({
                            "Category": row_p1['Category'],
                            "Metric": metric,
                            p1_name: fmt_stat(row_p1['Raw'], row_p1['Percentile']),
                            p2_name: fmt_stat(row_p2['Raw'], row_p2['Percentile']),
                            "raw_p1": row_p1['Raw'], # Hidden for styling logic
                            "raw_p2": row_p2['Raw']
                        })
                
                comp_df = pd.DataFrame(comp_data)
                
                if not comp_df.empty:
                    # Sort by Category
                    comp_df = comp_df.sort_values('Category')
                    
                    # Display
                    st.dataframe(
                        comp_df[['Category', 'Metric', p1_name, p2_name]],
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "Category": st.column_config.TextColumn("Category", width="small"),
                            "Metric": st.column_config.TextColumn("Metric", width="medium"),
                        }
                    )
                else:
                    st.warning("No overlapping metrics found.")
            else:
                st.warning("Insufficient data for detailed table.")

        else:
            st.warning("Could not find data for one of the selected players in the current filter.")

# =========================================================
# TAB 4: DATA AUDIT
# =========================================================
if st.session_state.active_tab == "🕒 Data Audit":
    st.header("🕒 Data Ingestion Log")
    
    # 1. Database Stats
    con = get_connection()
    try:
        # Check raw metadata
        # Try to get count from marts first
        try:
            row_count = con.execute("SELECT count(*) FROM mart_scouting_analysis").fetchone()[0]
        except:
            row_count = "N/A (Marts not built)"
            
        # Get latest load time (Assuming you have a 'load_timestamp' or we infer it)
        # If you don't have a load_timestamp in Marts, we can check Raw
        # Try 2025-2026 first, then 2024-2025
        try:
            last_load = con.execute("SELECT max(load_timestamp) FROM raw.standard_stats_2025_2026").fetchone()[0]
            audit_table = "raw.standard_stats_2025_2026"
        except:
            try:
                last_load = con.execute("SELECT max(load_timestamp) FROM raw.standard_stats_2024_2025").fetchone()[0]
                audit_table = "raw.standard_stats_2024_2025"
            except:
                last_load = "Unknown"
                audit_table = None
                
    except Exception as e:
        row_count = "Error"
        last_load = f"Error: {e}"
        audit_table = None

    c1, c2 = st.columns(2)
    c1.metric("Total Players (Marts)", row_count)
    c2.metric("Last Data Update", str(last_load))

    st.divider()

    # 2. Show Newest Arrivals
    st.subheader("🆕 Recently Added / Updated Players")
    
    if audit_table:
        try:
            # Fetch raw data for the log
            # Adjust table name to your current season
            recent_df = con.execute(f"""
                SELECT 
                    unnamed_1_level_0_player as Player,
                    unnamed_4_level_0_squad as Squad,
                    load_timestamp as Loaded_At
                FROM {audit_table}
                ORDER BY load_timestamp DESC
                LIMIT 20
            """).df()
            
            st.dataframe(recent_df, use_container_width=True)
            
        except Exception as e:
            st.info(f"Could not load audit log. Ensure 'load_timestamp' exists in raw tables. Error: {e}")
    else:
        st.warning("Could not determine audit table.")
