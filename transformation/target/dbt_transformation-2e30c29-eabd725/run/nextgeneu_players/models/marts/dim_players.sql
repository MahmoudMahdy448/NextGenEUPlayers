
  
    
    

    create  table
      "players"."main"."dim_players__dbt_tmp"
  
    as (
      WITH unique_players AS (
    SELECT 
        player_id,
        -- Get the most recently used name (in case it changed)
        arg_max(player_name, season_id) as player_name,
        -- Get the most recent nation/position
        arg_max(nation, season_id) as nation,
        arg_max(primary_position, season_id) as primary_position,
        max(birth_year) as birth_year
    FROM "players"."main"."int_player_season_stats"
    GROUP BY player_id
)

SELECT * FROM unique_players
    );
  
  