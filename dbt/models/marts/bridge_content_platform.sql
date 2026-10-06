{{ config(
    indexes=[
      {'columns': ['content_id']},
      {'columns': ['platform_id']},
    ]
) }}

with movie_platforms as (

    select
        dc.content_id,
        {{ canonical_platform_name('platform_value') }} as platform_name
    from {{ ref('stg_movies') }} sm
    join {{ ref('dim_watchable') }} dc
        on dc.content_type = 'movie' and dc.tmdb_id = sm.tmdb_id
    , jsonb_array_elements_text(sm.platforms) as platform_value
    where sm.platforms is not null

),

tv_platforms as (

    select
        dc.content_id,
        {{ canonical_platform_name('platform_value') }} as platform_name
    from {{ ref('stg_tv') }} st
    join {{ ref('dim_watchable') }} dc
        on dc.content_type = 'tv' and dc.tmdb_id = st.tmdb_id
    , jsonb_array_elements_text(st.platforms) as platform_value
    where st.platforms is not null

),

all_content_platforms as (

    select * from movie_platforms
    union all
    select * from tv_platforms

)

select distinct
    acp.content_id,
    dp.platform_id
from all_content_platforms acp
join {{ ref('dim_platform') }} dp
    on dp.platform_name = acp.platform_name
where acp.platform_name <> ''
