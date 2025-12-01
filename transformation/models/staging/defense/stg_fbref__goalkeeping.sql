{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'goalkeeping_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('performance_ga') }} as goals_against,
    {{ clean_numeric('performance_saves') }} as saves,
    TRY_CAST(performance_save AS FLOAT) as save_pct,
    {{ clean_numeric('performance_cs') }} as clean_sheets,
    TRY_CAST(performance_cs_1 AS FLOAT) as clean_sheet_pct,
    {{ clean_numeric('penalty_kicks_pka') }} as pk_allowed,
    {{ clean_numeric('penalty_kicks_pksv') }} as pk_saved

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
