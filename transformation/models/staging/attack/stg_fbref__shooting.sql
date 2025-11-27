{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'shooting_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('standard_sh') }} as shots_total,
    {{ clean_numeric('standard_sot') }} as shots_on_target,
    TRY_CAST(standard_dist AS FLOAT) as avg_shot_distance

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
