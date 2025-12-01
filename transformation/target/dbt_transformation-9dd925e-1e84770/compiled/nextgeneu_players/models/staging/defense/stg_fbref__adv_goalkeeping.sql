

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."advanced_goalkeeping_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."advanced_goalkeeping_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."advanced_goalkeeping_2025_2026"
        
    
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
    
    TRY_CAST(expected_psxg AS FLOAT) as psxg, -- Post-Shot Expected Goals (How hard were the shots?)
    TRY_CAST(expected_psxg_1 AS FLOAT) as psxg_plus_minus, -- The "Shot Stopping" Metric
    
    
    TRY_CAST(REPLACE(launched_cmp, ',', '') AS INTEGER)
 as long_passes_completed,
    TRY_CAST(launched_cmp_1 AS FLOAT) as long_pass_completion_pct,
    
    
    TRY_CAST(REPLACE(crosses_stp, ',', '') AS INTEGER)
 as crosses_stopped,
    TRY_CAST(crosses_stp_1 AS FLOAT) as crosses_stopped_pct,
    
    
    TRY_CAST(REPLACE(sweeper_opa, ',', '') AS INTEGER)
 as defensive_actions_outside_pen_area

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'