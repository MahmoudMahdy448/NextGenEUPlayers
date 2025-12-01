

WITH raw_union AS (
    
        SELECT *, '2023_2024' as source_season_id FROM "players"."raw"."defensive_actions_2023_2024"
         UNION ALL 
    
        SELECT *, '2024_2025' as source_season_id FROM "players"."raw"."defensive_actions_2024_2025"
         UNION ALL 
    
        SELECT *, '2025_2026' as source_season_id FROM "players"."raw"."defensive_actions_2025_2026"
        
    
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
    
    
    TRY_CAST(REPLACE(tackles_tkl, ',', '') AS INTEGER)
 as tackles_total,
    
    TRY_CAST(REPLACE(tackles_tklw, ',', '') AS INTEGER)
 as tackles_won,
    
    TRY_CAST(REPLACE(tackles_att_3rd, ',', '') AS INTEGER)
 as tackles_att_3rd,
    
    TRY_CAST(REPLACE(unnamed_21_level_0_int, ',', '') AS INTEGER)
 as interceptions,
    
    TRY_CAST(REPLACE(unnamed_23_level_0_clr, ',', '') AS INTEGER)
 as clearances,
    
    TRY_CAST(REPLACE(blocks_sh, ',', '') AS INTEGER)
 as shots_blocked

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'