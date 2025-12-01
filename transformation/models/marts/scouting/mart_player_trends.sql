WITH combined_stats AS (
    -- OUTFIELDERS
    SELECT 
        player_id, player_name, season_id, squad, age, primary_position,
        minutes_90s,
        
        -- 1. The Raw Scoring Metrics (Keep these for everyone!)
        goals,
        npxg,
        expected_contribution_per_90, -- (npxG + xAG)
        
        -- 2. The Specialist Metrics
        progression_total_dist_per_90,
        defensive_actions_per_90,
        0.0 as gk_metric
    FROM {{ ref('mart_scouting_analysis') }}
    
    UNION ALL
    
    -- GOALKEEPERS (Scoring is irrelevant, set to 0)
    SELECT 
        player_id, player_name, season_id, squad, age, 'GK' as primary_position,
        minutes_90s,
        
        0 as goals,
        0.0 as npxg,
        0.0 as expected_contribution_per_90,
        
        0.0 as progression_total_dist_per_90,
        0.0 as defensive_actions_per_90,
        psxg_plus_minus_per_90 as gk_metric
    FROM {{ ref('mart_goalkeeping_analysis') }}
),

calc_indices AS (
    SELECT 
        *,
        -- TRACK A: The Specialist Index (Role-Based)
        CASE 
            WHEN primary_position IN ('MF', 'MF,DF') THEN progression_total_dist_per_90
            WHEN primary_position IN ('DF', 'DF,MF') THEN defensive_actions_per_90
            WHEN primary_position = 'GK' THEN gk_metric
            ELSE expected_contribution_per_90 -- For Attackers, Specialist = Scoring
        END as specialist_index,
        
        -- TRACK B: The Universal Scoring Index (For all Outfielders)
        expected_contribution_per_90 as scoring_index
    FROM combined_stats
    WHERE minutes_90s >= 5.0
)

SELECT 
    player_id, player_name, season_id, primary_position, age,
    
    -- Metrics
    specialist_index,
    scoring_index,
    goals,
    npxg,
    
    -- Trends (Calculated on the Specialist Index)
    specialist_index - LAG(specialist_index) OVER (
        PARTITION BY player_id ORDER BY season_id
    ) as yoy_change_specialist,
    
    scoring_index - LAG(scoring_index) OVER (
        PARTITION BY player_id ORDER BY season_id
    ) as yoy_change_scoring

FROM calc_indices
