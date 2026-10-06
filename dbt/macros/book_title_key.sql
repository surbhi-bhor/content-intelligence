{% macro book_title_key(column) %}
{#
    Matching key for "is this the same book?" across Hardcover titles and
    Open Library titles, which differ in subtitles, leading articles, case
    and punctuation ("The Palace of Illusions: A Novel" vs "Palace of
    illusions"). Main title only, bracketed notes like "[adaptation]" or
    "(Book 1)" dropped, leading article dropped, letters and digits only.
#}
    regexp_replace(
        regexp_replace(
            regexp_replace(lower(split_part({{ column }}, ':', 1)), '\([^)]*\)|\[[^]]*\]', '', 'g'),
            '^\s*(the|a|an)\s+', ''
        ),
        '[^[:alnum:]]+', '', 'g'
    )
{% endmacro %}
