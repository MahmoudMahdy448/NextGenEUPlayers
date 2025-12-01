

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."playing_time_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."playing_time_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."playing_time_2025_2026"
        
    
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
    unnamed_1_level_0_player as player_name,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    
    TRY_CAST(REPLACE(playing_time_mp, ',', '') AS INTEGER)
 as matches_played,
    
    TRY_CAST(REPLACE(starts_starts, ',', '') AS INTEGER)
 as starts,
    
    TRY_CAST(REPLACE(subs_subs, ',', '') AS INTEGER)
 as substitutions_on,
    
    TRY_CAST(REPLACE(subs_unsub, ',', '') AS INTEGER)
 as unused_sub,
    
    -- Team Success (Plus/Minus)
    TRY_CAST(team_success_onoff AS FLOAT) as plus_minus_net_per_90

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'