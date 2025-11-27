{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'defensive_actions_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('tackles_tkl') }} as tackles_total,
    {{ clean_numeric('tackles_tklw') }} as tackles_won,
    {{ clean_numeric('tackles_att_3rd') }} as tackles_att_3rd,
    {{ clean_numeric('unnamed_21_level_0_int') }} as interceptions,
    {{ clean_numeric('unnamed_23_level_0_clr') }} as clearances,
    {{ clean_numeric('blocks_sh') }} as shots_blocked

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
