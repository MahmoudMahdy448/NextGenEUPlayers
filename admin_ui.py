import streamlit as st
import duckdb
import pandas as pd
import os

# Page config
st.set_page_config(page_title="DuckDB Explorer", layout="wide")

# Title
st.title("🦆 DuckDB Explorer")

# Database connection
DB_PATH = "data/duckdb/players.db"

if not os.path.exists(DB_PATH):
    st.error(f"Database not found at {DB_PATH}")
    st.stop()

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

try:
    con = get_connection()
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# Sidebar - Schema Selection
schemas = [s[0] for s in con.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name").fetchall()]
selected_schema = st.sidebar.selectbox("Select Schema", schemas, index=schemas.index('raw') if 'raw' in schemas else 0)

# Sidebar - Table Selection
tables = [t[0] for t in con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{selected_schema}' ORDER BY table_name").fetchall()]

if not tables:
    st.warning(f"No tables found in schema '{selected_schema}'")
else:
    selected_table = st.sidebar.selectbox("Select Table", tables)
    
    # Main Content
    st.header(f"Table: {selected_schema}.{selected_table}")
    
    # Get row count
    row_count = con.execute(f"SELECT count(*) FROM {selected_schema}.{selected_table}").fetchone()[0]
    st.info(f"Total Rows: {row_count}")
    
    # Query
    limit = st.slider("Rows to display", 5, 1000, 50)
    
    # Custom SQL option
    show_sql = st.checkbox("Write Custom SQL")
    
    if show_sql:
        default_query = f"SELECT * FROM {selected_schema}.{selected_table} LIMIT {limit}"
        query = st.text_area("SQL Query", default_query, height=150)
    else:
        query = f"SELECT * FROM {selected_schema}.{selected_table} LIMIT {limit}"
    
    if st.button("Run Query") or not show_sql:
        try:
            df = con.execute(query).df()
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Error executing query: {e}")

# Footer
st.sidebar.markdown("---")
st.sidebar.text("NextGenEUPlayers Explorer")
