with movie_genres as (

    select genre_value as genre_name
    from {{ ref('stg_movies') }}, jsonb_array_elements_text(genres) as genre_value
    where genres is not null

),

tv_genres as (

    select genre_value as genre_name
    from {{ ref('stg_tv') }}, jsonb_array_elements_text(genres) as genre_value
    where genres is not null

),

all_genres as (

    select genre_name from movie_genres
    union all
    select genre_name from tv_genres

),

cleaned as (

    select distinct trim(genre_name) as genre_name
    from all_genres
    where genre_name is not null and trim(genre_name) <> ''

)

select
    row_number() over (order by genre_name) as genre_id,
    genre_name
from cleaned
