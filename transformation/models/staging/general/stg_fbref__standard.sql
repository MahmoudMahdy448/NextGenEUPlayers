-- 1. CONFIGURATION
{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT 
            *, 
            '{{ season }}' as source_season_id 
        FROM {{ source('fbref_raw', 'standard_stats_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

cleaned AS (
    SELECT
        -- 1. IDENTITY (The Golden Key)
        {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
        
        -- 2. METADATA
        unnamed_1_level_0_player as player_name,
        unnamed_2_level_0_nation as nation,
        unnamed_4_level_0_squad  as squad,
        unnamed_3_level_0_pos    as primary_position,
        unnamed_5_level_0_comp   as competition,
        source_season_id         as season_id,
        
        -- 3. DEMOGRAPHICS
        {{ clean_age('unnamed_6_level_0_age') }} as age,
        {{ clean_numeric('unnamed_7_level_0_born') }} as birth_year,

        -- 4. PLAYING TIME
        {{ clean_numeric('playing_time_mp') }} as matches_played,
        {{ clean_numeric('playing_time_starts') }} as matches_started,
        {{ clean_numeric('playing_time_min') }} as minutes_played,
        TRY_CAST(playing_time_90s AS FLOAT) as minutes_90s,

        -- 5. PERFORMANCE METRICS (Using the macro to handle commas/types)
        {{ clean_numeric('performance_gls') }} as goals,
        {{ clean_numeric('performance_ast') }} as assists,
        {{ clean_numeric('performance_ga') }} as goals_assists,
        {{ clean_numeric('performance_pk') }} as penalties_made,
        {{ clean_numeric('performance_pkatt') }} as penalties_att,
        
        -- 6. EXPECTED METRICS
        TRY_CAST(expected_xg AS FLOAT) as xg,
        TRY_CAST(expected_npxg AS FLOAT) as npxg,
        TRY_CAST(expected_xag AS FLOAT) as xag,
        
        -- 7. PROGRESSION
        {{ clean_numeric('progression_prgc') }} as progressive_carries,
        {{ clean_numeric('progression_prgp') }} as progressive_passes,
        {{ clean_numeric('progression_prgr') }} as progressive_receptions

    FROM raw_union
)

SELECT * FROM cleaned
WHERE player_name != 'Player' -- Filter header rows
  AND player_name IS NOT NULL
