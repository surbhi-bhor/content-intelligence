{% macro get_allowed_languages() %}
{#
    Reads preferred_languages live from meta.user_config instead of a static
    dbt_project.yml var, so there is exactly one source of truth for allowed
    languages (the same one tmdb_op.py's discovery calls read at ingestion time).
    Previously these were two separate values that had to be kept in sync by hand
    and had already drifted once (var stuck at 2 languages after user_config
    gained a third).

    `execute` is False during dbt's parse-time compilation (no DB connection yet),
    so run_query() isn't available then - the hardcoded fallback only matters for
    that parse phase and is never used for an actual `dbt run`.
#}
    {% set fallback_languages = ['en', 'hi', 'mr'] %}

    {% if execute %}
        {% set query %}
            select config_value from meta.user_config where config_key = 'preferred_languages'
        {% endset %}
        {% set results = run_query(query) %}
        {% if results and results.rows | length > 0 %}
            {% set raw = results.rows[0][0] %}
            {% if raw is string %}
                {{ return(fromjson(raw)) }}
            {% else %}
                {{ return(raw) }}
            {% endif %}
        {% endif %}
    {% endif %}

    {{ return(fallback_languages) }}
{% endmacro %}
