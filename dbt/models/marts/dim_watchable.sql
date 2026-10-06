{{ config(
    indexes=[
      {'columns': ['content_type']},
      {'columns': ['original_language']},
    ]
) }}

with movies as (

    select
        'movie_' || tmdb_id as content_id,
        'movie' as content_type,
        tmdb_id,
        title,
        release_date,
        extract(year from release_date)::int as release_year,
        overview,
        popularity,
        vote_average,
        vote_count,
        original_language,
        is_allowed_language,
        director as primary_creator,
        runtime_mins,
        null::int as number_of_seasons,
        null::int as number_of_episodes,
        poster_path,
        false as is_serial_format

    from {{ ref('stg_movies') }}

),

tv as (

    select
        'tv_' || tmdb_id as content_id,
        'tv' as content_type,
        tmdb_id,
        title,
        first_air_date as release_date,
        extract(year from first_air_date)::int as release_year,
        overview,
        popularity,
        vote_average,
        vote_count,
        original_language,
        is_allowed_language,
        creator as primary_creator,
        episode_runtime_mins as runtime_mins,
        number_of_seasons,
        number_of_episodes,
        poster_path,
        -- Daily soaps, TV serials, reality and talk formats: never
        -- recommended. Total episode count alone missed them (Comedy Nights
        -- with Kapil: 191 episodes in one season). Episodes per season is
        -- the real signal: scripted series run ~8-24, these run 30 to 2,000+.
        -- Vote count exempts genuinely popular long-runners (anime such as
        -- One Piece at 51/season with 5,500+ votes); the >100/season tier
        -- also catches high-vote daily formats (Emmerdale, late-night talk).
        coalesce(
            number_of_episodes is not null and (
                (number_of_episodes > 300 and coalesce(vote_count, 0) < 100)
                or (number_of_episodes::numeric / greatest(coalesce(number_of_seasons, 1), 1) > 30
                    and coalesce(vote_count, 0) < 100)
                or (number_of_episodes::numeric / greatest(coalesce(number_of_seasons, 1), 1) > 100
                    and coalesce(vote_count, 0) < 1000)
            ),
            false
        ) as is_serial_format

    from {{ ref('stg_tv') }}

)

select * from movies
union all
select * from tv
