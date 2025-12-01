
  
  create view "players"."main"."stg_fbref__possession__dbt_tmp" as (
    

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."possession_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."possession_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."possession_2025_2026"
        
    
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
    
    
    TRY_CAST(REPLACE(touches_touches, ',', '') AS INTEGER)
 as touches,
    
    TRY_CAST(REPLACE(touches_att_pen, ',', '') AS INTEGER)
 as touches_att_pen,
    
    TRY_CAST(REPLACE(takeons_succ, ',', '') AS INTEGER)
 as takeons_won,
    
    TRY_CAST(REPLACE(takeons_att, ',', '') AS INTEGER)
 as takeons_attempted,
    
    TRY_CAST(REPLACE(carries_prgdist, ',', '') AS INTEGER)
 as carry_progressive_distance,
    
    TRY_CAST(REPLACE(carries_cpa, ',', '') AS INTEGER)
 as carries_into_penalty_area

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
  );
