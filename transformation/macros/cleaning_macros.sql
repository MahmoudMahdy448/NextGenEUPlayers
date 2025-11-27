{# 1. Generate Surrogate Key (The Unique ID) #}
{% macro generate_player_id(player_col, born_col, nation_col) %}
    md5(concat(
        coalesce({{ player_col }}, ''), 
        '-', 
        coalesce({{ born_col }}, ''), 
        '-', 
        coalesce({{ nation_col }}, '')
    ))
{% endmacro %}

{# 2. Clean Numeric Strings (e.g., "1,020" -> 1020) #}
{% macro clean_numeric(column_name, target_type='INTEGER') %}
    TRY_CAST(REPLACE({{ column_name }}, ',', '') AS {{ target_type }})
{% endmacro %}

{# 3. Clean Age (e.g., "23-154" -> 23) #}
{% macro clean_age(age_col) %}
    TRY_CAST(SPLIT_PART({{ age_col }}, '-', 1) AS INTEGER)
{% endmacro %}

{# 4. Safe Division (Prevent Divide by Zero) #}
{% macro safe_divide(numerator, denominator) %}
    CASE 
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN 0
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
