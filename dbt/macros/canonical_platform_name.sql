{% macro canonical_platform_name(column) %}
    trim(
        regexp_replace(
            {{ column }},
            '\s+(Standard with Ads|Premium Plus|Basic with Ads|with Ads|Amazon Channel|Apple TV Channel|Roku Premium Channel|Premium|Essential|Basic)$',
            ''
        )
    )
{% endmacro %}
