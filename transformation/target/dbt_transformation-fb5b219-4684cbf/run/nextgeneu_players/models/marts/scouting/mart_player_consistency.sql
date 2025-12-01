
  
    
    

    create  table
      "players"."main"."mart_player_consistency__dbt_tmp"
  
    as (
      WITH trends AS (
    SELECT * FROM "players"."main"."mart_player_trends"
),

volatility_calc AS (
    SELECT 
        player_id,
        player_name,
        primary_position,
        
        -- How many seasons do we have data for?
        count(distinct season_id) as seasons_tracked,
        
        -- Volatility (Standard Deviation of their Specialist Index)
        stddev(specialist_index) as performance_volatility,
        
        -- Peak Performance
        max(specialist_index) as career_peak_performance,
        
        -- Current Form (Last season vs Career Average)
        (arg_max(specialist_index, season_id) - avg(specialist_index)) as form_vs_avg
        
    FROM trends
    GROUP BY 1, 2, 3
)

SELECT 
    *,
    -- Risk Classification
    CASE 
        WHEN seasons_tracked < 2 THEN 'Unknown Risk (New Entry)'
        WHEN performance_volatility > 0.20 THEN 'High Risk (Inconsistent)'
        WHEN form_vs_avg < -0.1 THEN 'Declining Risk (Past Peak)'
        ELSE 'Safe Bet (Consistent)'
    END as transfer_risk_rating
FROM volatility_calc
    );
  
  