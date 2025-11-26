import duckdb
import os

# Constants
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "duckdb", "players.db")

# List of base table names we expect to find (from scraper)
TARGET_TABLES = [
    "standard_stats",
    "goalkeeping",
    "advanced_goalkeeping",
    "shooting",
    "passing",
    "pass_types",
    "goal_and_shot_creation",
    "defensive_actions",
    "possession",
    "playing_time",
    "miscellaneous_stats"
]

def transform_staging_data():
    """Transforms raw data into a clean 'staging' schema."""
    
    con = duckdb.connect(DB_PATH)
    
    # Create schema
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    print("Schema 'staging' ensured.")

    # Get list of all tables in 'raw' schema to dynamically find available seasons
    raw_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'").fetchall()
    raw_tables = [t[0] for t in raw_tables]

    for base_table in TARGET_TABLES:
        print(f"\nProcessing staging table: {base_table}")
        
        # Find all raw tables that match this base table (e.g., standard_stats_2023_2024, standard_stats_2024_2025)
        matching_tables = [t for t in raw_tables if t.startswith(base_table + "_")]
        
        if not matching_tables:
            print(f"  No raw tables found for {base_table}. Skipping.")
            continue
            
        print(f"  Found {len(matching_tables)} source tables: {', '.join(matching_tables)}")
        
        # Construct the UNION ALL query
        # We select * from each, assuming the scraper produced identical schemas. 
        # If schemas drift, we might need more complex logic (selecting common columns).
        # DuckDB's `UNION BY NAME` is perfect for this! It handles slight schema variations by filling NULLs.
        
        union_parts = []
        for tbl in matching_tables:
            union_parts.append(f"SELECT * FROM raw.{tbl}")
        
        union_query = " UNION ALL BY NAME ".join(union_parts)
        
        # Create staging table
        staging_table_name = f"staging.{base_table}"
        
        # Transformation Logic:
        # 1. Create table as Select ...
        # 2. We can apply cleaning here. For now, we rely on read_csv_auto's type inference 
        #    and normalize_names=True from the load step.
        #    We add a 'processed_at' timestamp.
        
        final_query = f"""
            CREATE OR REPLACE TABLE {staging_table_name} AS
            SELECT 
                *,
                now() as processed_at
            FROM (
                {union_query}
            )
        """
        
        try:
            con.execute(final_query)
            row_count = con.execute(f"SELECT count(*) FROM {staging_table_name}").fetchone()[0]
            print(f"  Successfully created {staging_table_name} with {row_count} rows.")
        except Exception as e:
            print(f"  Error creating {staging_table_name}: {e}")

    print("\nStaging transformation complete.")
    con.close()

if __name__ == "__main__":
    transform_staging_data()
