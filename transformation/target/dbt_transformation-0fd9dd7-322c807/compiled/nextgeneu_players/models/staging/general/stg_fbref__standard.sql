-- 1. CONFIGURATION


WITH raw_union AS (
    
        SELECT 
            *, 
            '2023_2024' as source_season_id 
        FROM "players"."raw"."standard_stats_2023_2024"
         UNION ALL 
    
        SELECT 
            *, 
            '2024_2025' as source_season_id 
        FROM "players"."raw"."standard_stats_2024_2025"
         UNION ALL 
    
        SELECT 
            *, 
            '2025_2026' as source_season_id 
        FROM "players"."raw"."standard_stats_2025_2026"
        
    
),

cleaned AS (
    SELECT
        -- 1. IDENTITY (The Golden Key)
        
    md5(concat(
        coalesce(unnamed_1_level_0_player, ''), 
        '-', 
        coalesce(unnamed_7_level_0_born, ''), 
        '-', 
        coalesce(unnamed_2_level_0_nation, '')
    ))
 as player_id,
        
        -- 2. METADATA
        unnamed_1_level_0_player as player_name,
        unnamed_2_level_0_nation as nation,
        unnamed_4_level_0_squad  as squad,
        unnamed_3_level_0_pos    as primary_position,
        unnamed_5_level_0_comp   as competition,
        source_season_id         as season_id,
        
        -- 3. DEMOGRAPHICS
        
    TRY_CAST(SPLIT_PART(unnamed_6_level_0_age, '-', 1) AS INTEGER)
 as age,
        
    TRY_CAST(REPLACE(unnamed_7_level_0_born, ',', '') AS INTEGER)
 as birth_year,

        -- 4. PLAYING TIME
        
    TRY_CAST(REPLACE(playing_time_mp, ',', '') AS INTEGER)
 as matches_played,
        
    TRY_CAST(REPLACE(playing_time_starts, ',', '') AS INTEGER)
 as matches_started,
        
    TRY_CAST(REPLACE(playing_time_min, ',', '') AS INTEGER)
 as minutes_played,
        TRY_CAST(playing_time_90s AS FLOAT) as minutes_90s,

        -- 5. PERFORMANCE METRICS (Using the macro to handle commas/types)
        
    TRY_CAST(REPLACE(performance_gls, ',', '') AS INTEGER)
 as goals,
        
    TRY_CAST(REPLACE(performance_ast, ',', '') AS INTEGER)
 as assists,
        
    TRY_CAST(REPLACE(performance_ga, ',', '') AS INTEGER)
 as goals_assists,
        
    TRY_CAST(REPLACE(performance_pk, ',', '') AS INTEGER)
 as penalties_made,
        
    TRY_CAST(REPLACE(performance_pkatt, ',', '') AS INTEGER)
 as penalties_att,
        
        -- 6. EXPECTED METRICS
        TRY_CAST(expected_xg AS FLOAT) as xg,
        TRY_CAST(expected_npxg AS FLOAT) as npxg,
        TRY_CAST(expected_xag AS FLOAT) as xag,
        
        -- 7. PROGRESSION
        
    TRY_CAST(REPLACE(progression_prgc, ',', '') AS INTEGER)
 as progressive_carries,
        
    TRY_CAST(REPLACE(progression_prgp, ',', '') AS INTEGER)
 as progressive_passes,
        
    TRY_CAST(REPLACE(progression_prgr, ',', '') AS INTEGER)
 as progressive_receptions

    FROM raw_union
)

SELECT * FROM cleaned
WHERE player_name != 'Player' -- Filter header rows
  AND player_name IS NOT NULL