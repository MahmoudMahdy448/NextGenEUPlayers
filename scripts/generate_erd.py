import duckdb

def generate_mermaid_erd(db_path='data/duckdb/players.db', schemas=['main']):
    con = duckdb.connect(db_path, read_only=True)
    
    print("```mermaid")
    print("erDiagram")
    
    for schema in schemas:
        # Get all tables in schema
        tables = con.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
            AND (table_name LIKE 'mart_%' OR table_name LIKE 'dim_%')
        """).fetchall()
        
        for (table,) in tables:
            # Clean table name for Mermaid (remove spaces if any)
            safe_table_name = table.replace(".", "_")
            
            print(f"    {safe_table_name} {{")
            
            # Get columns
            columns = con.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
            """).fetchall()
            
            for col, dtype in columns:
                # Add key indicators (heuristic based)
                key_type = ""
                if "player_id" in col:
                    key_type = "PK,FK"
                elif "_id" in col:
                    key_type = "FK"
                
                print(f"        {dtype} {col} {key_type}")
                
            print("    }")
            
    # Define Relationships (Heuristic: Connecting Marts on player_id)
    # This is a simplified logic to show connectivity
    print("\n    %% Relationships")
    print("    mart_scouting_analysis ||--|| mart_player_trends : \"shares player_id\"")
    print("    mart_scouting_analysis ||--|| mart_transfer_valuation : \"shares player_id\"")
    # print("    mart_scouting_analysis }|--|| dim_players : \"defined by\"") # dim_players might not exist yet

    print("```")

if __name__ == "__main__":
    generate_mermaid_erd()
