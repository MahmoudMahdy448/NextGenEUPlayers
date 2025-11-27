import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# --- CONFIGURATION ---
st.set_page_config(page_title="NextGen Scout Pro", page_icon="⚽", layout="wide")

# --- DATA LOADER ---
@st.cache_resource
def get_connection():
    return duckdb.connect('data/duckdb/players.db', read_only=True)

@st.cache_data
def load_data():
    con = get_connection()
    # Load the main mart
    # CORRECTED: Removed 'marts.' prefix based on DB check
    df = con.execute("SELECT * FROM mart_scouting_analysis WHERE minutes_90s >= 5").df()
    
    df['scouting_role'] = df['primary_position'].apply(map_role)
    return df

# 1. ENRICHMENT: Map Raw Positions to Scouting Roles
def map_role(pos):
    if pd.isna(pos): return "Unknown"
    if 'GK' in pos: return "Goalkeeper"
    if 'FW' in pos: return "Attacker"
    if 'MF' in pos and 'FW' in pos: return "Winger/AM"
    if 'MF' in pos: return "Midfielder"
    if 'DF' in pos and 'MF' in pos: return "Wingback/DM"
    if 'DF' in pos: return "Defender"
    return "Other"

df = load_data()

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

# 4. Scouting Role Filter
roles = ["Attacker", "Winger/AM", "Midfielder", "Defender", "Wingback/DM"]
selected_role = st.sidebar.selectbox("Target Position", roles, index=0)

# --- FILTERING LOGIC ---
# Base filter (Seasons + Leagues)
main_filter = (
    (df['season_id'].isin(selected_seasons)) & 
    (df['competition'].isin(selected_leagues))
)

# Apply Squad filter if selected
if selected_squads:
    main_filter = main_filter & (df['squad'].isin(selected_squads))

filtered_df = df[main_filter].copy()

# Filter by Role for the Matrix
role_df = filtered_df[filtered_df['scouting_role'] == selected_role].copy()


# --- TAB STRUCTURE ---
tab_matrix, tab_profile, tab_compare = st.tabs(["📊 Market Analytics", "👤 Player Deep Dive", "⚖️ Comparison"])

# --- KPI MAP (Used in multiple tabs) ---
kpi_map = {
    "Attacker": {
        "x": "expected_contribution_per_90", "x_label": "Exp. Goal Contrib (xG+xAG)",
        "y": "goal_contribution_per_90", "y_label": "Actual Goals + Assists",
        "size": "shots_total"
    },
    "Winger/AM": {
        "x": "key_passes", "x_label": "Key Passes/90",
        "y": "takeons_won", "y_label": "Dribbles Won/90",
        "size": "npxg"
    },
    "Midfielder": {
        "x": "pass_progressive_distance", "x_label": "Prog. Pass Distance/90",
        "y": "key_passes", "y_label": "Key Passes/90",
        "size": "minutes_90s"
    },
    "Defender": {
        "x": "interceptions", "x_label": "Interceptions/90",
        "y": "aerials_won", "y_label": "Aerials Won/90",
        "size": "tackles_won"
    },
    "Wingback/DM": {
        "x": "tackles_won", "x_label": "Tackles Won/90",
        "y": "progression_total_dist_per_90", "y_label": "Ball Progression/90",
        "size": "interceptions"
    }
}

# =========================================================
# TAB 1: POSITION-BASED MARKET ANALYTICS
# =========================================================
with tab_matrix:
    st.title(f"Market Analysis: {selected_role}s")
    
    # Fallback to Attacker if role mapping fails
    cfg = kpi_map.get(selected_role, kpi_map["Attacker"])
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if not role_df.empty:
            fig = px.scatter(
                role_df,
                x=cfg['x'],
                y=cfg['y'],
                size=cfg['size'],
                color="is_u23_prospect",
                hover_name="player_name",
                hover_data=["squad", "age", "season_id"],
                color_discrete_map={True: "#00CC96", False: "#EF553B"},
                title=f"{selected_role} Performance Matrix ({selected_seasons})",
                labels={cfg['x']: cfg['x_label'], cfg['y']: cfg['y_label'], "is_u23_prospect": "U23?"}
            )
            # Add Averages
            fig.add_vline(x=role_df[cfg['x']].mean(), line_dash="dot", line_color="grey", annotation_text="Avg")
            fig.add_hline(y=role_df[cfg['y']].mean(), line_dash="dot", line_color="grey", annotation_text="Avg")
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No players found for this role/filter combination.")

    with col2:
        st.subheader("Leaderboard")
        if not role_df.empty:
            # Sort by the "Y-Axis" metric of the chosen role
            leaders = role_df.sort_values(by=cfg['y'], ascending=False).head(10)
            st.dataframe(
                leaders[['player_name', 'squad', 'age', cfg['y']]]
                .style.format({cfg['y']: "{:.2f}"}),
                use_container_width=True,
                hide_index=True
            )

# =========================================================
# TAB 2: PLAYER DEEP DIVE (Search & Evolve)
# =========================================================
with tab_profile:
    st.header("👤 Player Scouting Report")
    
    # 1. SEARCH BAR (Global Search across all loaded data)
    # We allow searching ANY player, even if not in the current filter
    all_player_names = sorted(df['player_name'].unique())
    search_name = st.selectbox("🔎 Search Player by Name", all_player_names, index=None, placeholder="Type to search...")
    
    if search_name:
        # Get all records for this player
        p_hist = df[df['player_name'] == search_name].sort_values(by='season_id')
        
        # Get the most recent season data for the header
        p_latest = p_hist.iloc[-1]
        
        # --- HEADER SECTION ---
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Nation", p_latest['nation'])
        c2.metric("Current Team", p_latest['squad'])
        c3.metric("Age", p_latest['age'])
        c4.metric("Position", p_latest['primary_position'])
        
        st.divider()
        
        # --- EVOLUTION CHART (Time Series) ---
        st.subheader("📈 Season-over-Season Evolution")
        
        # Determine metrics to show based on their role
        p_role = map_role(p_latest['primary_position'])
        p_cfg = kpi_map.get(p_role, kpi_map["Attacker"])
        
        evol_fig = go.Figure()
        evol_fig.add_trace(go.Scatter(x=p_hist['season_id'], y=p_hist[p_cfg['x']], name=p_cfg['x_label'], mode='lines+markers'))
        evol_fig.add_trace(go.Scatter(x=p_hist['season_id'], y=p_hist[p_cfg['y']], name=p_cfg['y_label'], mode='lines+markers'))
        # Always add minutes to see playing time context
        evol_fig.add_trace(go.Bar(x=p_hist['season_id'], y=p_hist['minutes_90s'], name='90s Played', opacity=0.3, yaxis='y2'))
        
        evol_fig.update_layout(
            title="Development Trajectory",
            yaxis2=dict(title="90s Played", overlaying='y', side='right'),
            hovermode="x unified"
        )
        st.plotly_chart(evol_fig, use_container_width=True)
        
        # --- DETAILED STATS TABLE ---
        st.subheader("📋 Detailed Season Stats")
        display_cols = ['season_id', 'squad', 'competition', 'age', 'minutes_90s', 
                        'goals', 'assists', 'npxg', 'tackles_won', 'interceptions', 'progression_total_dist_per_90']
        
        # Filter cols that exist
        display_cols = [c for c in display_cols if c in p_hist.columns]

        st.dataframe(p_hist[display_cols].style.format({
            'npxg': "{:.2f}",
            'progression_total_dist_per_90': "{:.1f}"
        }), use_container_width=True)

# =========================================================
# TAB 3: COMPARISON (Basic)
# =========================================================
with tab_compare:
    st.subheader("⚔️ Player Comparison")
    
    # Use filtered_df to allow comparing players in the current selection
    # If empty, fallback to df
    comp_source = filtered_df if not filtered_df.empty else df
    
    c1, c2 = st.columns(2)
    p1_name = c1.selectbox("Player A", sorted(comp_source['player_name'].unique()), index=0 if not comp_source.empty else None)
    p2_name = c2.selectbox("Player B", sorted(comp_source['player_name'].unique()), index=1 if len(comp_source) > 1 else None)
    
    if p1_name and p2_name:
        # Get latest season for selected players in the filtered set
        # If multiple seasons selected, we might want to aggregate or pick latest. 
        # For simplicity, let's pick the row with max minutes or latest season.
        
        p1_rows = comp_source[comp_source['player_name'] == p1_name]
        p2_rows = comp_source[comp_source['player_name'] == p2_name]
        
        if not p1_rows.empty and not p2_rows.empty:
            p1 = p1_rows.sort_values('season_id').iloc[-1]
            p2 = p2_rows.sort_values('season_id').iloc[-1]
            
            # Comparison Table
            comp_data = {
                'Metric': ['Age', 'xG+xAG/90', 'Progression Dist/90', 'Defensive Actions/90', 'Key Passes/90', 'Tackles Won/90'],
                p1_name: [p1['age'], p1['expected_contribution_per_90'], p1['progression_total_dist_per_90'], 
                          p1['defensive_actions_per_90'], p1['key_passes'], p1['tackles_won']],
                p2_name: [p2['age'], p2['expected_contribution_per_90'], p2['progression_total_dist_per_90'], 
                          p2['defensive_actions_per_90'], p2['key_passes'], p2['tackles_won']]
            }
            
            comp_df = pd.DataFrame(comp_data, columns=['Metric', p1_name, p2_name])
            
            # Style the dataframe: Highlight the winner in Green
            def highlight_winner(row):
                styles = [''] # Metric column
                if row['Metric'] == 'Age': 
                    return styles + ['', '']
                
                v1 = row[p1_name]
                v2 = row[p2_name]
                
                if pd.isna(v1) or pd.isna(v2):
                    return styles + ['', '']

                if v1 > v2:
                    styles.extend(['background-color: #d4edda', ''])
                elif v2 > v1:
                    styles.extend(['', 'background-color: #d4edda'])
                else:
                    styles.extend(['', ''])
                return styles

            st.table(comp_df.style.apply(highlight_winner, axis=1))
        else:
            st.warning("Could not find data for one of the selected players in the current filter.")
