with open_library as (

    select
        'book_' || sb.ol_key as content_id,
        sb.ol_key,
        sb.hardcover_id,
        -- A library book keeps the title the reader knows it by: Open
        -- Library's work title can be the original-language one
        -- ("コーヒーが冷めないうちに" for Before the Coffee Gets Cold).
        coalesce(sbr.title, sb.title) as title,
        sb.first_publish_year as release_year,
        sb.ratings_average as vote_average,
        sb.ratings_count as vote_count,
        coalesce(sbr.author, sb.author) as primary_creator,
        sb.page_count,
        case when sb.cover_id is not null
            then 'https://covers.openlibrary.org/b/id/' || sb.cover_id || '-M.jpg'
        end as cover_url,
        'openlibrary' as metadata_source,
        -- One Open Library row per library book. A later enrichment run can
        -- match a different work for the same Hardcover book (search results
        -- shift), which would otherwise give one read two dim_book rows and
        -- double it in fact_reading_history. The newest match wins.
        case when sb.hardcover_id is null then 1
            else row_number() over (
                partition by sb.hardcover_id
                order by sb.ingested_at desc, sb.ol_key
            )
        end as library_match_rank

    from {{ ref('stg_books') }} sb
    left join {{ ref('stg_book_ratings') }} sbr on sbr.hardcover_id = sb.hardcover_id

),

-- Books in the Hardcover library that have no Open Library match. Without
-- these rows the read never reaches fact_reading_history, so it would be
-- missing from history, the taste profile, and the "already read" check.
-- Once a match lands, the open_library row replaces this one and
-- fact_reading_history's post_hook removes the old content_id.
hardcover_only as (

    select
        'book_hc_' || sbr.hardcover_id as content_id,
        null::text as ol_key,
        sbr.hardcover_id,
        sbr.title,
        null::integer as release_year,
        null::double precision as vote_average,
        null::integer as vote_count,
        sbr.author as primary_creator,
        null::integer as page_count,
        null::text as cover_url,
        'hardcover' as metadata_source,
        1 as library_match_rank

    from {{ ref('stg_book_ratings') }} sbr
    where not exists (
        select 1 from {{ ref('stg_books') }} sb
        where sb.hardcover_id = sbr.hardcover_id
    )

)

select
    content_id, ol_key, hardcover_id, title, release_year, vote_average,
    vote_count, primary_creator, page_count, cover_url, metadata_source
from open_library
where library_match_rank = 1

union all

select
    content_id, ol_key, hardcover_id, title, release_year, vote_average,
    vote_count, primary_creator, page_count, cover_url, metadata_source
from hardcover_only
