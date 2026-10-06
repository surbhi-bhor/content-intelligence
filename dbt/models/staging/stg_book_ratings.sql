select
    hardcover_id,
    title,
    author,
    rating,
    status as status_id,
    case status
        when '1' then 'want_to_read'
        when '2' then 'reading'
        when '3' then 'read'
        when '5' then 'did_not_finish'
        else 'unknown'
    end as reading_status,
    finished_at,
    ingested_at
from {{ source('raw', 'raw_book_ratings') }}
