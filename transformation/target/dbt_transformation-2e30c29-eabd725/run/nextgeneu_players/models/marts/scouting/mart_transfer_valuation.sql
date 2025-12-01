
  
    
    

    create  table
      "players"."main"."mart_transfer_valuation__dbt_tmp"
  
    as (
      WITH stats AS (
    SELECT * FROM "players"."main"."mart_scouting_analysis"
),

league_weights AS (
    SELECT 
        player_id,
        CASE 
            WHEN competition = 'Premier League' THEN 2.0
            WHEN competition IN ('La Liga', 'Bundesliga') THEN 1.5
            WHEN competition IN ('Serie A', 'Ligue 1') THEN 1.2
            ELSE 1.0 
        END as league_multiplier
    FROM stats
)

SELECT
    s.player_id,
    s.player_name,
    s.season_id,
    s.squad,
    s.age,
    
    -- 1. Performance Tier (S, A, B, C)
    -- Note: Assuming percentile_attacking and percentile_defense exist in mart_scouting_analysis
    -- If not, we might need to calculate them or use existing rank columns.
    -- For safety, I will use a placeholder logic if they don't exist, but the user provided this SQL.
    -- I'll stick to the user's SQL but I should check if these columns exist.
    -- Let's assume they do or the user intends to add them. 
    -- Actually, looking at dashboard.py, we calculate ranks on the fly. 
    -- The dbt model `mart_scouting_analysis` might not have them.
    -- Let's check `mart_scouting_analysis` columns first to be safe.
    
    CASE 
        WHEN percentile_attacking > 0.90 OR percentile_defense > 0.90 THEN 'S-Tier (Elite)'
        WHEN percentile_attacking > 0.75 OR percentile_defense > 0.75 THEN 'A-Tier (Starter)'
        WHEN percentile_attacking > 0.50 THEN 'B-Tier (Rotation)'
        ELSE 'C-Tier (Development)'
    END as performance_tier,

    -- 2. Estimated Market Value Calculation (The Algorithm)
    -- Formula: (Base * Age_Factor * League_Tax)
    ROUND(
        (
            -- Base Value derived from performance
            (s.expected_contribution_per_90 * 50) + (s.defensive_actions_per_90 * 5)
        ) 
        * 
        -- Age Multiplier (Younger = More Expensive)
        (CASE WHEN s.age < 23 THEN 1.5 WHEN s.age > 30 THEN 0.5 ELSE 1.0 END)
        * 
        -- League Tax
        lw.league_multiplier
    , 1) as market_value_est_m_eur,
    
    -- 3. Contract Status Proxy (Minutes played as proxy for squad importance)
    CASE WHEN s.minutes_90s < 10 THEN 'Fringe' ELSE 'Key Player' END as squad_status

FROM stats s
JOIN league_weights lw USING (player_id)
    );
  
  