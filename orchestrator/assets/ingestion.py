from dagster import asset, Output, AssetExecutionContext, MetadataValue
import subprocess
import duckdb
import os

@asset(
    description="Scrapes FBref data and saves to CSV",
    group_name="ingestion",
    compute_kind="python"
)
def raw_csv_files(context: AssetExecutionContext):
    """
    Executes the scraping script. 
    In a real prod env, this might be an external ECS task, 
    but for now we run it locally.
    """
    context.log.info("Starting FBref Scraper...")
    
    # Run the existing script as a subprocess
    # We assume the CWD is the project root
    result = subprocess.run(
        ["python", "ingestion/fbref_scraper.py"], 
        capture_output=True, 
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Scraper failed: {result.stderr}")
    
    context.log.info(result.stdout)
    return Output(
        value="Data Scraped", 
        metadata={"stdout": result.stdout}
    )

@asset(
    description="Loads Raw CSVs into DuckDB and reports row counts",
    group_name="ingestion",
    compute_kind="duckdb",
    deps=[raw_csv_files]
)
def raw_duckdb_tables(context: AssetExecutionContext):
    context.log.info("Loading CSVs to DuckDB...")
    
    # 1. Run the Loading Script
    result = subprocess.run(
        ["python", "ingestion/load_raw.py"], 
        capture_output=True, 
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Loader failed: {result.stderr}")
        
    context.log.info(result.stdout)

    # 2. CALCULATE METADATA (The New Part)
    # Connect to the DB to count rows
    # Use environment variable for path if available, else default
    db_path = os.getenv("DUCKDB_PATH", os.path.join("data", "duckdb", "players.db"))
    
    row_count = 0
    table_count = 0
    
    try:
        con = duckdb.connect(db_path, read_only=True)
        # Get count of the main table (e.g., standard_stats for the latest season)
        # We'll try to find the latest standard_stats table dynamically or hardcode for now
        # Let's look for standard_stats_2025_2026 as per user request, or fallback
        try:
            row_count = con.execute("SELECT count(*) FROM raw.standard_stats_2025_2026").fetchone()[0]
        except:
            # Fallback to 2024-2025 if 2025-2026 doesn't exist yet
            try:
                row_count = con.execute("SELECT count(*) FROM raw.standard_stats_2024_2025").fetchone()[0]
            except:
                row_count = 0
        
        # Get total tables count
        table_count = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'raw'").fetchone()[0]
        con.close()
    except Exception as e:
        context.log.warning(f"Could not fetch metadata: {e}")

    # 3. Return Output with Metadata
    return Output(
        value="Raw Data Loaded",
        metadata={
            "Latest Season Rows": MetadataValue.int(row_count),
            "Total Raw Tables": MetadataValue.int(table_count),
            "Log Output": MetadataValue.md(result.stdout)
        }
    )
