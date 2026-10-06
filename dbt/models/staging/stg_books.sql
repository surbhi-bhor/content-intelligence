select
    ol_key,
    hardcover_id,
    title,
    author,
    first_publish_year,
    subjects,
    ratings_average,
    ratings_count,
    page_count,
    cover_id,
    ingested_at
from {{ source('raw', 'raw_books') }}
