
  
  create view "players"."main"."stg_fbref__goalkeeping__dbt_tmp" as (
    

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."goalkeeping_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."goalkeeping_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."goalkeeping_2025_2026"
        
    
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
    
    
    TRY_CAST(REPLACE(performance_ga, ',', '') AS INTEGER)
 as goals_against,
    
    TRY_CAST(REPLACE(performance_saves, ',', '') AS INTEGER)
 as saves,
    TRY_CAST(performance_save AS FLOAT) as save_pct,
    
    TRY_CAST(REPLACE(performance_cs, ',', '') AS INTEGER)
 as clean_sheets,
    TRY_CAST(performance_cs_1 AS FLOAT) as clean_sheet_pct,
    
    TRY_CAST(REPLACE(penalty_kicks_pka, ',', '') AS INTEGER)
 as pk_allowed,
    
    TRY_CAST(REPLACE(penalty_kicks_pksv, ',', '') AS INTEGER)
 as pk_saved

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
  );
