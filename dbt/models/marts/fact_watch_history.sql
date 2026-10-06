{{ config(
    materialized='incremental',
    unique_key='content_id',
    on_schema_change='sync_column_types',
    post_hook="""
        delete from {{ this }} f
        where not exists (
            select 1
            from {{ ref('stg_ratings') }} sr
            join {{ ref('dim_watchable') }} dc
                on dc.tmdb_id = sr.tmdb_id
                and dc.content_type = case sr.content_type when 'show' then 'tv' else sr.content_type end
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
    dc.content_type,
    sr.rating,
    sr.has_rating,
    sr.status as native_status,
    case sr.status
        when 'completed' then 'consumed'
        when 'watching' then 'in_progress'
        when 'plantowatch' then 'planned'
        else 'unknown'
    end as consumption_status,
    sr.watched_at::timestamp as interaction_date,
    sr.ingested_at as source_ingested_at

from {{ ref('stg_ratings') }} sr
join {{ ref('dim_watchable') }} dc
    on dc.tmdb_id = sr.tmdb_id
    and dc.content_type = case sr.content_type when 'show' then 'tv' else sr.content_type end

{% if is_incremental() %}
-- raw_ratings.ingested_at only moves when a value actually changed (see the
-- UPSERT in simkl_op.py), so it catches re-ratings, status changes and
-- NULL-dated plan-to-watch rows that a watched_at watermark never would.
-- The NOT IN arm picks up ratings whose dim_watchable row only appeared
-- after the rating was ingested (TMDB details landing a run later).
-- Rows removed from Simkl are deleted by the post_hook above.
where sr.ingested_at > (select coalesce(max(source_ingested_at), '1900-01-01') from {{ this }})
   or dc.content_id not in (select content_id from {{ this }})
{% endif %}
