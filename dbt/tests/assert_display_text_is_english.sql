-- Fails if any text the app or dashboards display contains a non-Latin
-- script (CJK, Cyrillic, Hangul, ...). Ingestion prefers English names and
-- staging blanks the rest, so a row here means a new source of
-- non-English text reached the marts.

with displayed as (
    select 'dim_watchable.title' as field, content_id as id, title as value from {{ ref('dim_watchable') }}
    union all select 'dim_watchable.primary_creator', content_id, primary_creator from {{ ref('dim_watchable') }}
    union all select 'dim_book.title', content_id, title from {{ ref('dim_book') }}
    union all select 'dim_book.primary_creator', content_id, primary_creator from {{ ref('dim_book') }}
    union all select 'dim_book_subject.subject_name', subject_id::text, subject_name from {{ ref('dim_book_subject') }}
    union all select 'dim_genre.genre_name', genre_id::text, genre_name from {{ ref('dim_genre') }}
    union all select 'dim_platform.platform_name', platform_id::text, platform_name from {{ ref('dim_platform') }}
)

select field, id, value
from displayed
where {{ has_non_latin_script('value') }}
