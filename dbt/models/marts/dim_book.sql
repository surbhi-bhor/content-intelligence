select
    'book_' || ol_key as content_id,
    ol_key,
    hardcover_id,
    title,
    first_publish_year as release_year,
    ratings_average as vote_average,
    ratings_count as vote_count,
    author as primary_creator,
    page_count,
    case when cover_id is not null
        then 'https://covers.openlibrary.org/b/id/' || cover_id || '-M.jpg'
    end as cover_url

from {{ ref('stg_books') }}
