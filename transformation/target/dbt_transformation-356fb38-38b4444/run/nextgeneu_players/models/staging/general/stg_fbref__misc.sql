
  
  create view "players"."main"."stg_fbref__misc__dbt_tmp" as (
    

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."miscellaneous_stats_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."miscellaneous_stats_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."miscellaneous_stats_2025_2026"
        
    
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
    
    
    TRY_CAST(REPLACE(performance_crdy, ',', '') AS INTEGER)
 as yellow_cards,
    
    TRY_CAST(REPLACE(performance_crdr, ',', '') AS INTEGER)
 as red_cards,
    
    TRY_CAST(REPLACE(performance_fls, ',', '') AS INTEGER)
 as fouls_committed,
    
    TRY_CAST(REPLACE(performance_fld, ',', '') AS INTEGER)
 as fouls_drawn,
    
    TRY_CAST(REPLACE(performance_off, ',', '') AS INTEGER)
 as offsides,
    
    TRY_CAST(REPLACE(performance_recov, ',', '') AS INTEGER)
 as ball_recoveries,
    
    TRY_CAST(REPLACE(aerial_duels_won, ',', '') AS INTEGER)
 as aerials_won,
    
    TRY_CAST(REPLACE(aerial_duels_lost, ',', '') AS INTEGER)
 as aerials_lost

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
  );
