with latest_snapshot as (

    select
        tmdb_id,
        release_date,
        popularity,
        vote_average,
        vote_count,
        original_language,
        overview,
        row_number() over (partition by tmdb_id order by ingested_at_date desc) as rn
    from {{ source('raw', 'raw_movies') }}

),

latest_snapshot_deduped as (

    select
        tmdb_id,
        release_date,
        popularity,
        vote_average,
        vote_count,
        original_language,
        overview
    from latest_snapshot
    where rn = 1

),

details as (

    select
        tmdb_id,
        title,
        original_language,
        runtime_mins,
        budget,
        revenue,
        director,
        genres,
        platforms,
        poster_path
    from {{ source('raw', 'raw_movie_details') }}

)

select
    details.tmdb_id,
    details.title,
    details.runtime_mins,
    details.budget,
    details.revenue,
    -- Display names must be English; a name with no Latin form is blanked.
    {{ latin_or_null('details.director') }} as director,
    details.genres,
    details.platforms,
    details.poster_path,
    snapshot.release_date,
    snapshot.popularity,
    snapshot.vote_average,
    snapshot.vote_count,
    snapshot.overview,
    coalesce(details.original_language, snapshot.original_language) as original_language,
    case
        when coalesce(details.original_language, snapshot.original_language) is null then null
        when coalesce(details.original_language, snapshot.original_language) in ({{ "'" ~ get_allowed_languages() | join("', '") ~ "'" }}) then true
        else false
    end as is_allowed_language

from details
left join latest_snapshot_deduped as snapshot
    on details.tmdb_id = snapshot.tmdb_id
