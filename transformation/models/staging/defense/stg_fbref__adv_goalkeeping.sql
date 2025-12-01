{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'advanced_goalkeeping_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    TRY_CAST(expected_psxg AS FLOAT) as psxg, -- Post-Shot Expected Goals (How hard were the shots?)
    TRY_CAST(expected_psxg_1 AS FLOAT) as psxg_plus_minus, -- The "Shot Stopping" Metric
    
    {{ clean_numeric('launched_cmp') }} as long_passes_completed,
    TRY_CAST(launched_cmp_1 AS FLOAT) as long_pass_completion_pct,
    
    {{ clean_numeric('crosses_stp') }} as crosses_stopped,
    TRY_CAST(crosses_stp_1 AS FLOAT) as crosses_stopped_pct,
    
    {{ clean_numeric('sweeper_opa') }} as defensive_actions_outside_pen_area

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
