{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'possession_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('touches_touches') }} as touches,
    {{ clean_numeric('touches_att_pen') }} as touches_att_pen,
    {{ clean_numeric('takeons_succ') }} as takeons_won,
    {{ clean_numeric('takeons_att') }} as takeons_attempted,
    {{ clean_numeric('carries_prgdist') }} as carry_progressive_distance,
    {{ clean_numeric('carries_cpa') }} as carries_into_penalty_area

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
