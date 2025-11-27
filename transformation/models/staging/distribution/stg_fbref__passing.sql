{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'passing_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('total_cmp') }} as passes_completed,
    {{ clean_numeric('total_att') }} as passes_attempted,
    {{ clean_numeric('total_prgdist') }} as pass_progressive_distance,
    {{ clean_numeric('unnamed_27_level_0_kp') }} as key_passes,
    {{ clean_numeric('unnamed_29_level_0_ppa') }} as passes_into_penalty_area,
    {{ clean_numeric('unnamed_30_level_0_crspa') }} as crosses_into_penalty_area

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
