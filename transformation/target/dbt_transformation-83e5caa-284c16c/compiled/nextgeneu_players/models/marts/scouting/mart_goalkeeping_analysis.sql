WITH base_stats AS (
    SELECT * FROM "players"."main"."int_goalkeeper_season_stats"
    WHERE minutes_90s >= 5.0
),

per_90_calculations AS (
    SELECT 
        *,
        -- Normalization
        
    CASE 
        WHEN minutes_90s = 0 OR minutes_90s IS NULL THEN 0
        ELSE psxg_plus_minus / minutes_90s
    END
 as psxg_plus_minus_per_90,
        
    CASE 
        WHEN minutes_90s = 0 OR minutes_90s IS NULL THEN 0
        ELSE sweeper_actions / minutes_90s
    END
 as sweeper_actions_per_90,
        
        -- FLAGS
        CASE WHEN (2026 - birth_year) <= 23 THEN TRUE ELSE FALSE END as is_u23_prospect
    FROM base_stats
)

SELECT 
    *,
    -- Percentiles (Compare GK vs GK only)
    PERCENT_RANK() OVER (PARTITION BY season_id ORDER BY psxg_plus_minus_per_90 ASC) as percentile_shot_stopping,
    PERCENT_RANK() OVER (PARTITION BY season_id ORDER BY crosses_stopped_pct ASC) as percentile_command,
    PERCENT_RANK() OVER (PARTITION BY season_id ORDER BY sweeper_actions_per_90 ASC) as percentile_sweeping,
    PERCENT_RANK() OVER (PARTITION BY season_id ORDER BY long_pass_completion_pct ASC) as percentile_distribution
FROM per_90_calculations