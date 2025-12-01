{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'playing_time_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_1_level_0_player as player_name,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('playing_time_mp') }} as matches_played,
    {{ clean_numeric('starts_starts') }} as starts,
    {{ clean_numeric('subs_subs') }} as substitutions_on,
    {{ clean_numeric('subs_unsub') }} as unused_sub,
    
    -- Team Success (Plus/Minus)
    {{ clean_numeric('team_success_') }} as plus_minus,
    {{ clean_numeric('team_success_90') }} as plus_minus_per90,
    TRY_CAST(team_success_onoff AS FLOAT) as plus_minus_net_per_90

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
