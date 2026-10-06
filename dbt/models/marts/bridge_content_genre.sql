{{ config(
    indexes=[
      {'columns': ['content_id']},
      {'columns': ['genre_id']},
    ]
) }}

with movie_genres as (

    select
        dc.content_id,
        trim(genre_value) as genre_name
    from {{ ref('stg_movies') }} sm
    join {{ ref('dim_watchable') }} dc
        on dc.content_type = 'movie' and dc.tmdb_id = sm.tmdb_id
    , jsonb_array_elements_text(sm.genres) as genre_value
    where sm.genres is not null

),

tv_genres as (

    select
        dc.content_id,
        trim(genre_value) as genre_name
    from {{ ref('stg_tv') }} st
    join {{ ref('dim_watchable') }} dc
        on dc.content_type = 'tv' and dc.tmdb_id = st.tmdb_id
    , jsonb_array_elements_text(st.genres) as genre_value
    where st.genres is not null

),

all_content_genres as (

    select * from movie_genres
    union all
    select * from tv_genres

)

select distinct
    acg.content_id,
    dg.genre_id
from all_content_genres acg
join {{ ref('dim_genre') }} dg
    on dg.genre_name = acg.genre_name
where acg.genre_name <> ''
