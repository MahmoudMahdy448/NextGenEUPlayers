import duckdb
import os
import glob

# Constants
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "duckdb", "players.db")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

def load_raw_data():
    """Loads raw CSVs into the DuckDB 'raw' schema."""
    
    # Ensure DB directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    con = duckdb.connect(DB_PATH)
    
    # Create schema
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    print("Schema 'raw' ensured.")

    # Find all season folders
    season_folders = glob.glob(os.path.join(DATA_DIR, "*"))
    
    for season_path in season_folders:
        if not os.path.isdir(season_path):
            continue
            
        season = os.path.basename(season_path)
        print(f"\nLoading data for season: {season}")
        
        # Find all CSVs in this season folder
        csv_files = glob.glob(os.path.join(season_path, "*.csv"))
        
        for csv_file in csv_files:
            table_name = os.path.basename(csv_file).replace(".csv", "")
            # Sanitize table name (should already be clean from scraper, but good to be safe)
            table_name = table_name.replace("-", "_")
            
            # Construct raw table name: raw.standard_stats_2023_2024
            # Filename is already standard_stats_2023-2024.csv, so table_name is standard_stats_2023_2024
            raw_table_name = f"raw.{table_name}"
            
            print(f"  - Loading {os.path.basename(csv_file)} -> {raw_table_name}")
            
            # Load CSV
            # We use read_csv_auto for automatic type inference
            query = f"""
                CREATE OR REPLACE TABLE {raw_table_name} AS 
                SELECT *, '{season}' as season_id 
                FROM read_csv_auto('{csv_file}', normalize_names=True);
            """
            try:
                con.execute(query)
            except Exception as e:
                print(f"    Error loading {raw_table_name}: {e}")

    print("\nRaw load complete.")
    con.close()

if __name__ == "__main__":
    load_raw_data()
