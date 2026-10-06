{#
    Everything the app and dashboards display is meant to be English.
    These macros are the shared definitions; ingestion already prefers
    English names, and these act as the backstop in dbt.
#}

{% macro has_non_latin_script(column) %}
{#- True when the text contains a letter outside the Latin script (CJK,
    Cyrillic, Hangul, Devanagari, ...). Latin accents are allowed, whether
    stored as one character or as a letter plus a combining mark
    (U+0300 to U+036F), so "Emily Brontë", "Vālmīki" and "Karṇa" pass. -#}
    coalesce({{ column }} ~ '[^\x00-\x7F\u00A0-\u024F\u0300-\u036F\u1E00-\u1EFF\u2000-\u206F]', false)
{%- endmacro %}

{% macro latin_or_null(column) %}
{#- The name if it is in Latin script, otherwise NULL, so a name with no
    English form is left blank instead of shown in another script. -#}
    case when {{ has_non_latin_script(column) }} then null else {{ column }} end
{%- endmacro %}

{% macro is_non_english_subject(column) %}
{#- Open Library subject tags come from many national catalogues
    ("Littérature américaine", "Ciencia-ficción", "Cours et courtisans --
    Romans, nouvelles, etc"). Flags a tag that is in a non-Latin script,
    uses accented letters of French, Spanish, German or Polish (Sanskrit
    transliteration such as "Karṇa" or "Rāma" is allowed), or contains a
    common French, Spanish or German word. Best effort: an English tag
    containing "Brontë" is dropped too, which costs nothing. -#}
    (
        {{ has_non_latin_script(column) }}
        or normalize({{ column }}, NFC) ~ '[àâäçéèêëîïôöœùûüÿæáíóúãõąęłńźżßÀÂÄÇÉÈÊËÎÏÔÖŒÙÛÜÁÍÓÚÃÕĄĘŁŃŹŻ¿¡]'
        or lower({{ column }}) ~ '\m(et|du|des|les|pour|dans|aux|sur|und|der|das|para|con|del|romans|nouvelles|ouvrages|jeunesse|histoire|femmes|hommes|novela|novelas|ficcion|cuentos|lectures|morceaux|mythologie|psychologie|psychologues|psychohistoire|autriche|lieux|imaginaires|monde|literatura|horrorroman)\M'
    )
{%- endmacro %}
