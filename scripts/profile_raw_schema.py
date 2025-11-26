import duckdb
import os
import json
import pandas as pd

# Constants
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "duckdb", "players.db")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schemas", "raw")

def profile_raw_schema():
    """Profiles the 'raw' schema in DuckDB and generates MD/JSON reports."""
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Ensure DB directory exists (though DB file should exist from load_raw step)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    con = duckdb.connect(DB_PATH)
    
    # Get all tables in raw schema
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw' ORDER BY table_name").fetchall()
    tables = [t[0] for t in tables]
    
    full_profile = {}
    md_lines = ["# Raw Schema Profile", "", f"**Total Tables**: {len(tables)}", ""]
    
    for table in tables:
        print(f"Profiling {table}...")
        
        # Get Row Count
        row_count = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        
        # Get Column Info (Name, Type, Position)
        cols_info = con.execute(f"""
            SELECT column_name, data_type, ordinal_position 
            FROM information_schema.columns 
            WHERE table_schema = 'raw' AND table_name = '{table}' 
            ORDER BY ordinal_position
        """).fetchall()
        
        table_profile = {
            "row_count": row_count,
            "columns": []
        }
        
        md_lines.append(f"## Table: `{table}`")
        md_lines.append(f"- **Rows**: {row_count}")
        md_lines.append("| Position | Column | Type | Null Count | Null % |")
        md_lines.append("| :--- | :--- | :--- | :--- | :--- |")
        
        for col_name, dtype, pos in cols_info:
            # Calculate Nulls
            null_count = con.execute(f"SELECT count(*) FROM raw.{table} WHERE {col_name} IS NULL").fetchone()[0]
            null_pct = (null_count / row_count * 100) if row_count > 0 else 0
            
            col_profile = {
                "name": col_name,
                "position": pos,
                "type": dtype,
                "null_count": null_count,
                "null_percentage": round(null_pct, 2)
            }
            table_profile["columns"].append(col_profile)
            
            md_lines.append(f"| {pos} | `{col_name}` | {dtype} | {null_count} | {null_pct:.2f}% |")
        
        full_profile[table] = table_profile
        md_lines.append("")
        
    con.close()
    
    # Save JSON
    json_path = os.path.join(OUTPUT_DIR, "raw_profile.json")
    with open(json_path, "w") as f:
        json.dump(full_profile, f, indent=2)
    print(f"Saved JSON profile to {json_path}")
    
    # Save Markdown
    md_path = os.path.join(OUTPUT_DIR, "raw_profile.md")
    with open(md_path, "w") as f:
        f.write("\n".join(md_lines))
    print(f"Saved Markdown profile to {md_path}")

if __name__ == "__main__":
    profile_raw_schema()
