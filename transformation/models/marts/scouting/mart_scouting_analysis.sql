WITH base_stats AS (
    SELECT * FROM {{ ref('int_player_season_stats') }}
    WHERE minutes_90s >= 5.0 -- Filter: Remove noise (players with < 5 games)
),

per_90_calculations AS (
    SELECT 
        *,
        -- 1. ATTACK SCORES
        {{ safe_divide('goals + assists', 'minutes_90s') }} as goal_contribution_per_90,
        {{ safe_divide('npxg + xag', 'minutes_90s') }} as expected_contribution_per_90,
        
        -- 2. PROGRESSION SCORES (The "Engine" Metric)
        {{ safe_divide('pass_progressive_distance + carry_progressive_distance', 'minutes_90s') }} as progression_total_dist_per_90,
        
        -- 3. DEFENSIVE SCORES (The "Workrate" Metric)
        {{ safe_divide('tackles_won + interceptions + recoveries', 'minutes_90s') }} as defensive_actions_per_90,
        
        -- 4. FLAGS
        CASE WHEN (2026 - birth_year) <= 23 THEN TRUE ELSE FALSE END as is_u23_prospect
    FROM base_stats
)

SELECT 
    *,
    -- Percentile Ranks (0.0 to 1.0)
    -- Compare players only against peers in the SAME position and SAME season
    PERCENT_RANK() OVER (
        PARTITION BY season_id, primary_position 
        ORDER BY expected_contribution_per_90 ASC
    ) as percentile_attacking,

    PERCENT_RANK() OVER (
        PARTITION BY season_id, primary_position 
        ORDER BY progression_total_dist_per_90 ASC
    ) as percentile_progression,

    PERCENT_RANK() OVER (
        PARTITION BY season_id, primary_position 
        ORDER BY defensive_actions_per_90 ASC
    ) as percentile_defense

FROM per_90_calculations
