WITH player_stats AS (
    SELECT * FROM "players"."main"."mart_scouting_analysis"
)

SELECT
    squad,
    season_id,
    competition,
    primary_position,
    
    -- Squad Depth Info
    count(distinct player_id) as depth_count,
    avg(age) as avg_age,
    
    -- The "Bar" to beat (Average Starter Performance)
    -- We assume "Starters" are those with > 15.0 90s played
    avg(CASE WHEN minutes_90s > 15 THEN expected_contribution_per_90 END) as starter_avg_xG_xAG,
    avg(CASE WHEN minutes_90s > 15 THEN progression_total_dist_per_90 END) as starter_avg_progression,
    avg(CASE WHEN minutes_90s > 15 THEN defensive_actions_per_90 END) as starter_avg_def_actions

FROM player_stats
GROUP BY 1, 2, 3, 4