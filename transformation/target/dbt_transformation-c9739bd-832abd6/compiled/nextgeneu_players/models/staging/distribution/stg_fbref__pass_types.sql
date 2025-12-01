

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."pass_types_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."pass_types_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."pass_types_2025_2026"
        
    
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
    
    -- High Value Creation Stats
    
    TRY_CAST(REPLACE(pass_types_tb, ',', '') AS INTEGER)
 as through_balls,
    
    TRY_CAST(REPLACE(pass_types_sw, ',', '') AS INTEGER)
 as switches,
    
    TRY_CAST(REPLACE(pass_types_crs, ',', '') AS INTEGER)
 as crosses,
    
    TRY_CAST(REPLACE(corner_kicks_in, ',', '') AS INTEGER)
 as corners_inswing,
    
    TRY_CAST(REPLACE(corner_kicks_out, ',', '') AS INTEGER)
 as corners_outswing

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'