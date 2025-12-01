
  
  create view "players"."main"."int_goalkeeper_season_stats__dbt_tmp" as (
    WITH base AS (SELECT * FROM "players"."main"."stg_fbref__standard"), -- We still need Standard for Age/Nation/Minutes
     gk_basic AS (SELECT * FROM "players"."main"."stg_fbref__goalkeeping"),
     gk_adv   AS (SELECT * FROM "players"."main"."stg_fbref__adv_goalkeeping")

SELECT 
    b.player_id,
    b.player_name,
    b.season_id,
    b.squad,
    b.nation,
    b.age,
    b.birth_year,
    b.minutes_90s,
    
    -- Basic
    g.goals_against,
    g.save_pct,
    g.clean_sheet_pct,
    g.pk_saved,
    
    -- Advanced (The Real Money Stats)
    a.psxg,
    a.psxg_plus_minus, -- Positive = Elite Shot Stopping
    a.crosses_stopped_pct, -- Command of Area
    a.defensive_actions_outside_pen_area as sweeper_actions,
    a.long_pass_completion_pct

FROM base b
INNER JOIN gk_basic g USING (player_id, squad, season_id) -- Inner Join because we ONLY want GKs
LEFT JOIN gk_adv a    USING (player_id, squad, season_id)
  );
