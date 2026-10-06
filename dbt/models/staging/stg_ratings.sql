select
    simkl_id,
    tmdb_id,
    title,
    content_type,
    status,
    rating,
    watched_at,
    (rating is not null) as has_rating,
    ingested_at
from {{ source('raw', 'raw_ratings') }}
