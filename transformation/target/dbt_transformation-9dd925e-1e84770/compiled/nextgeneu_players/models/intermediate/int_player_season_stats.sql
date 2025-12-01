WITH standard AS (SELECT * FROM "players"."main"."stg_fbref__standard"),
     shooting AS (SELECT * FROM "players"."main"."stg_fbref__shooting"),
     passing  AS (SELECT * FROM "players"."main"."stg_fbref__passing"),
     pass_types AS (SELECT * FROM "players"."main"."stg_fbref__pass_types"),
     defense  AS (SELECT * FROM "players"."main"."stg_fbref__defense"),
     poss     AS (SELECT * FROM "players"."main"."stg_fbref__possession"),
     misc     AS (SELECT * FROM "players"."main"."stg_fbref__misc"),
     creation AS (SELECT * FROM "players"."main"."stg_fbref__creation"),
     playing_time AS (SELECT * FROM "players"."main"."stg_fbref__playing_time")

SELECT
    -- Anchors
    s.player_id,
    s.player_name,
    s.season_id,
    s.squad,
    s.competition,
    s.nation,
    s.primary_position,
    s.age,
    s.birth_year,
    s.minutes_played,
    s.minutes_90s,

    -- Attacking Metrics
    s.goals,
    s.assists,
    s.npxg,
    s.xag,
    coalesce(sh.shots_total, 0) as shots_total,
    coalesce(sh.shots_on_target, 0) as shots_on_target,
    coalesce(c.sca, 0) as sca,
    coalesce(c.gca, 0) as gca,

    -- Possession & Creation
    coalesce(p.key_passes, 0) as key_passes,
    coalesce(p.pass_progressive_distance, 0) as pass_progressive_distance,
    coalesce(pt.through_balls, 0) as through_balls,
    coalesce(pt.switches, 0) as switches,
    coalesce(pt.crosses, 0) as crosses,
    coalesce(poss.carry_progressive_distance, 0) as carry_progressive_distance,
    coalesce(poss.takeons_won, 0) as takeons_won,

    -- Defensive Workrate
    coalesce(d.tackles_won, 0) as tackles_won,
    coalesce(d.interceptions, 0) as interceptions,
    coalesce(d.tackles_att_3rd, 0) as tackles_att_3rd,
    coalesce(m.ball_recoveries, 0) as recoveries,
    coalesce(m.aerials_won, 0) as aerials_won,
    
    -- Playing Time / Team Impact
    coalesce(tm.plus_minus, 0) as plus_minus,
    coalesce(tm.plus_minus_per90, 0) as plus_minus_per90

FROM standard s
LEFT JOIN shooting sh USING (player_id, squad, season_id)
LEFT JOIN passing p   USING (player_id, squad, season_id)
LEFT JOIN pass_types pt USING (player_id, squad, season_id)
LEFT JOIN defense d   USING (player_id, squad, season_id)
LEFT JOIN poss        USING (player_id, squad, season_id)
LEFT JOIN misc m      USING (player_id, squad, season_id)
LEFT JOIN creation c  USING (player_id, squad, season_id)
LEFT JOIN playing_time tm USING (player_id, squad, season_id)

-- Filter out rows where the join keys might have drifted or empty names
WHERE s.player_id IS NOT NULL