with book_subjects as (

    select genre_value as subject_name
    from {{ ref('stg_books') }}, jsonb_array_elements_text(subjects) as genre_value
    where subjects is not null

),

cleaned as (

    select distinct trim(subject_name) as subject_name
    from book_subjects
    where subject_name is not null and trim(subject_name) <> ''

)

select
    row_number() over (order by subject_name) as subject_id,
    subject_name
from cleaned
