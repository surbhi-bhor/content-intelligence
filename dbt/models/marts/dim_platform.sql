with movie_platforms as (

    select platform_value as platform_name
    from {{ ref('stg_movies') }}, jsonb_array_elements_text(platforms) as platform_value
    where platforms is not null

),

tv_platforms as (

    select platform_value as platform_name
    from {{ ref('stg_tv') }}, jsonb_array_elements_text(platforms) as platform_value
    where platforms is not null

),

all_platforms as (

    select platform_name from movie_platforms
    union all
    select platform_name from tv_platforms

),

canonicalized as (

    select {{ canonical_platform_name('platform_name') }} as platform_name
    from all_platforms
    where platform_name is not null and trim(platform_name) <> ''

),

cleaned as (

    select distinct platform_name
    from canonicalized
    where platform_name <> ''

)

select
    row_number() over (order by platform_name) as platform_id,
    platform_name
from cleaned
