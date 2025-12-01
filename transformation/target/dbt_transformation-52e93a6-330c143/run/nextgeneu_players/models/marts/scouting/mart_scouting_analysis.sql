
  
    
    

    create  table
      "players"."main"."mart_scouting_analysis__dbt_tmp"
  
    as (
      WITH base_stats AS (
    SELECT * FROM "players"."main"."int_player_season_stats"
    WHERE minutes_90s >= 5.0 -- Filter: Remove noise (players with < 5 games)
),

per_90_calculations AS (
    SELECT 
        *,
        -- 1. ATTACK SCORES
        
    CASE 
        WHEN minutes_90s = 0 OR minutes_90s IS NULL THEN 0
        ELSE goals + assists / minutes_90s
    END
 as goal_contribution_per_90,
        
    CASE 
        WHEN minutes_90s = 0 OR minutes_90s IS NULL THEN 0
        ELSE npxg + xag / minutes_90s
    END
 as expected_contribution_per_90,
        
        -- 2. PROGRESSION SCORES (The "Engine" Metric)
        
    CASE 
        WHEN minutes_90s = 0 OR minutes_90s IS NULL THEN 0
        ELSE pass_progressive_distance + carry_progressive_distance / minutes_90s
    END
 as progression_total_dist_per_90,
        
        -- 3. DEFENSIVE SCORES (The "Workrate" Metric)
        
    CASE 
        WHEN minutes_90s = 0 OR minutes_90s IS NULL THEN 0
        ELSE tackles_won + interceptions + recoveries / minutes_90s
    END
 as defensive_actions_per_90,
        
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
    );
  
  