-- Hardcover's native rating scale is 1-5; rating here is normalized to the
-- 1-10 scale used everywhere else (fact_watch_history.rating, dim_*.vote_average)
-- so cross-type comparisons (taste_profile's avg_rating_by_type, /ask) don't
-- need to know per-table scale rules. native_rating preserves the original
-- 1-5 value for anywhere that wants Hardcover's own number.
{{ config(
    materialized='incremental',
    unique_key='content_id',
    on_schema_change='sync_column_types',
    post_hook="""
        delete from {{ this }} f
        where not exists (
            select 1
            from {{ ref('stg_book_ratings') }} sbr
            join {{ ref('dim_book') }} dc on dc.hardcover_id = sbr.hardcover_id
            where dc.content_id = f.content_id
        )
    """,
    indexes=[
      {'columns': ['content_id']},
      {'columns': ['interaction_date']},
      {'columns': ['rating']},
    ]
) }}

select
    dc.content_id,
    sbr.rating * 2 as rating,
    sbr.rating as native_rating,
    (sbr.rating is not null) as has_rating,
    sbr.reading_status as native_status,
    case sbr.reading_status
        when 'read' then 'consumed'
        when 'reading' then 'in_progress'
        when 'want_to_read' then 'planned'
        when 'did_not_finish' then 'abandoned'
        else 'unknown'
    end as consumption_status,
    sbr.finished_at::timestamp as interaction_date,
    sbr.ingested_at as source_ingested_at

from {{ ref('stg_book_ratings') }} sbr
join {{ ref('dim_book') }} dc
    on dc.hardcover_id = sbr.hardcover_id

{% if is_incremental() %}
-- Same watermark reasoning as fact_watch_history: raw ingested_at only
-- moves on a real change, so re-ratings and unfinished (NULL finished_at)
-- books are caught; deletes are handled by the post_hook above.
where sbr.ingested_at > (select coalesce(max(source_ingested_at), '1900-01-01') from {{ this }})
   or dc.content_id not in (select content_id from {{ this }})
{% endif %}
