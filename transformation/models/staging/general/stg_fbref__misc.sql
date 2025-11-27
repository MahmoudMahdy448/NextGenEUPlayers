{% set seasons = ['2023_2024', '2024_2025', '2025_2026'] %}

WITH raw_union AS (
    {% for season in seasons %}
        SELECT *, '{{ season }}' as source_season_id FROM {{ source('fbref_raw', 'miscellaneous_stats_' ~ season) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    {{ generate_player_id('unnamed_1_level_0_player', 'unnamed_7_level_0_born', 'unnamed_2_level_0_nation') }} as player_id,
    unnamed_4_level_0_squad  as squad,
    source_season_id         as season_id,
    
    {{ clean_numeric('performance_crdy') }} as yellow_cards,
    {{ clean_numeric('performance_crdr') }} as red_cards,
    {{ clean_numeric('performance_fls') }} as fouls_committed,
    {{ clean_numeric('performance_fld') }} as fouls_drawn,
    {{ clean_numeric('performance_off') }} as offsides,
    {{ clean_numeric('performance_recov') }} as ball_recoveries,
    {{ clean_numeric('aerial_duels_won') }} as aerials_won,
    {{ clean_numeric('aerial_duels_lost') }} as aerials_lost

FROM raw_union
WHERE unnamed_1_level_0_player != 'Player'
