
  
  create view "players"."main"."stg_fbref__passing__dbt_tmp" as (
    

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."passing_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."passing_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."passing_2025_2026"
        
    
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
    
    
    TRY_CAST(REPLACE(total_cmp, ',', '') AS INTEGER)
 as passes_completed,
    
    TRY_CAST(REPLACE(total_att, ',', '') AS INTEGER)
 as passes_attempted,
    
    TRY_CAST(REPLACE(total_prgdist, ',', '') AS INTEGER)
 as pass_progressive_distance,
    
    TRY_CAST(REPLACE(unnamed_27_level_0_kp, ',', '') AS INTEGER)
 as key_passes,
    
    TRY_CAST(REPLACE(unnamed_29_level_0_ppa, ',', '') AS INTEGER)
 as passes_into_penalty_area,
    
    TRY_CAST(REPLACE(unnamed_30_level_0_crspa, ',', '') AS INTEGER)
 as crosses_into_penalty_area

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
  );
