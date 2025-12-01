{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'pass_types_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    -- High Value Creation Stats
    {{ clean_numeric('pass_types_tb') }} as through_balls,
    {{ clean_numeric('pass_types_sw') }} as switches,
    {{ clean_numeric('pass_types_crs') }} as crosses,
    {{ clean_numeric('corner_kicks_in') }} as corners_inswing,
    {{ clean_numeric('corner_kicks_out') }} as corners_outswing

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
