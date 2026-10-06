select
    ol_key,
    hardcover_id,
    title,
    -- Ingestion stores the English author name; this blanks any that slip through.
    {{ latin_or_null('author') }} as author,
    first_publish_year,
    subjects,
    ratings_average,
    ratings_count,
    page_count,
    cover_id,
    ingested_at
from {{ source('raw', 'raw_books') }}
