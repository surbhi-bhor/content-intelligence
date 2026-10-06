with book_subjects as (

    select
        dc.content_id,
        trim(genre_value) as subject_name
    from {{ ref('stg_books') }} sb
    join {{ ref('dim_book') }} dc
        on dc.ol_key = sb.ol_key
    , jsonb_array_elements_text(sb.subjects) as genre_value
    where sb.subjects is not null

)

select distinct
    bs.content_id,
    dbs.subject_id
from book_subjects bs
join {{ ref('dim_book_subject') }} dbs
    on dbs.subject_name = bs.subject_name
where bs.subject_name <> ''
