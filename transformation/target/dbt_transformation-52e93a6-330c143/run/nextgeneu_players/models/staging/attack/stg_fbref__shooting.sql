
  
  create view "players"."main"."stg_fbref__shooting__dbt_tmp" as (
    

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."shooting_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."shooting_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."shooting_2025_2026"
        
    
)

SELECT
    
    md5(concat(
        coalesce(unnamed_1_level_0_player, ''), 
        '-', 
        coalesce(unnamed_7_level_0_born, ''), 
        '-', 
        coalesce(unnamed_2_level_0_nation, '')
    ))
 as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    
    TRY_CAST(REPLACE(standard_sh, ',', '') AS INTEGER)
 as shots_total,
    
    TRY_CAST(REPLACE(standard_sot, ',', '') AS INTEGER)
 as shots_on_target,
    TRY_CAST(standard_dist AS FLOAT) as avg_shot_distance

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
  );
