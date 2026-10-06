import os
import time
import requests
import psycopg2
import psycopg2.extras
from datetime import date
from typing import Optional
from pydantic import BaseModel, ValidationError
from dagster import op, get_dagster_logger

# ── Pydantic models ──────────────────────────────────────────

class MovieRaw(BaseModel):
    tmdb_id: int
    title: str
    release_date: Optional[date] = None
    popularity: Optional[float] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    original_language: Optional[str] = None
    overview: Optional[str] = None
    genre_ids: Optional[list] = []

class MovieDetails(BaseModel):
    tmdb_id: int
    title: str
    original_language: Optional[str] = None
    runtime_mins: Optional[int] = None
    budget: Optional[int] = None
    revenue: Optional[int] = None
    director: Optional[str] = None
    genres: Optional[list] = []
    platforms: Optional[list] = []
    poster_path: Optional[str] = None

class TVRaw(BaseModel):
    tmdb_id: int
    title: str
    first_air_date: Optional[date] = None
    popularity: Optional[float] = None
    vote_average: Optional[float] = None
    vote_count: Optional[int] = None
    original_language: Optional[str] = None
    overview: Optional[str] = None
    genre_ids: Optional[list] = []

class TVDetails(BaseModel):
    tmdb_id: int
    title: str
    original_language: Optional[str] = None
    number_of_seasons: Optional[int] = None
    number_of_episodes: Optional[int] = None
    episode_runtime_mins: Optional[int] = None
    creator: Optional[str] = None
    genres: Optional[list] = []
    platforms: Optional[list] = []
    poster_path: Optional[str] = None

# ── DB connection ─────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

# ── Helpers ───────────────────────────────────────────────────

def fetch_tmdb(endpoint: str, retries: int = 5) -> dict:
    token = os.getenv("TMDB_API_KEY")
    base  = "https://api.themoviedb.org/3"
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(retries):
        try:
            r = requests.get(f"{base}{endpoint}", headers=headers, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)

def get_director(credits: dict) -> Optional[str]:
    for member in credits.get("crew", []):
        if member.get("job") == "Director":
            return member.get("name")
    return None

def get_platforms(providers: dict, region: str) -> list:
    # TMDB's watch/providers response is keyed by country - availability is
    # genuinely different per region (e.g. a Hindi film with no US
    # streaming option was on Prime Video in IN). Hardcoding "US" left 91% of
    # Marathi and 60% of Hindi titles with no platform, and recommendations
    # require one, so the regional candidate pool was nearly empty.
    results = providers.get("results", {})
    streams = results.get(region, {}).get("flatrate", [])
    return [s["provider_name"] for s in streams]

def get_creator(detail: dict) -> Optional[str]:
    creators = detail.get("created_by", [])
    return creators[0]["name"] if creators else None

def get_watch_region(cur) -> str:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'watch_region';
    """)
    row = cur.fetchone()
    return row[0] if row and row[0] else "US"

# Details (and especially streaming platforms) change after first fetch -
# titles leave and join services. Anything enriched longer ago than this is
# re-fetched, so "where to watch" can't freeze at its first-seen value.
DETAILS_REFRESH_DAYS = 30

def get_ids_to_enrich(cur, table: str, candidate_ids: list) -> tuple[list, int, int]:
    """New ids from this run plus every already-enriched id that has gone
    stale. Returns (ids, new_count, stale_count)."""
    cur.execute(f"SELECT tmdb_id FROM raw.{table};")
    already_enriched = {row[0] for row in cur.fetchall()}
    new_ids = {i for i in candidate_ids if i not in already_enriched}
    cur.execute(
        f"SELECT tmdb_id FROM raw.{table} WHERE ingested_at < NOW() - make_interval(days => %s);",
        (DETAILS_REFRESH_DAYS,),
    )
    stale_ids = {row[0] for row in cur.fetchall()}
    return sorted(new_ids | stale_ids), len(new_ids), len(stale_ids)

def get_preferred_languages(cur) -> list:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'preferred_languages';
    """)
    row = cur.fetchone()
    return row[0] if row else []

# ── Op 1: discover movies ─────────────────────────────────────

TMDB_MOVIES_MIN = 20
TMDB_SHOWS_MIN = 20

@op
def ingest_tmdb_movies(context):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0
    skipped  = 0
    lang_skipped = 0
    tmdb_ids = []
    total_fetched = 0

    preferred_languages = set(get_preferred_languages(cur))
    endpoints = [
        "/movie/popular?page=1",
        "/movie/upcoming?page=1",
        "/trending/movie/day",
    ]
    for lang in preferred_languages:
        endpoints.append(f"/discover/movie?with_original_language={lang}&sort_by=popularity.desc&page=1")

    for ep in endpoints:
        data = fetch_tmdb(ep)
        movies = data.get("results", [])
        total_fetched += len(movies)

        for m in movies:
            # /movie/popular, /movie/upcoming, and /trending/movie/day are
            # global charts, not language-scoped like the /discover calls
            # above - without this, every language TMDB's charts return
            # (French, Korean, Japanese, ...) lands in raw.raw_movies and
            # propagates all the way to marts.dim_watchable, even though
            # recommendation_op already filters to preferred_languages and
            # would never surface it. Dropping it here instead of just
            # filtering it out downstream means the curated layer only ever
            # holds languages actually watched.
            if m.get("original_language") not in preferred_languages:
                lang_skipped += 1
                continue
            try:
                movie = MovieRaw(
                    tmdb_id           = m["id"],
                    title             = m["title"],
                    release_date      = m.get("release_date") or None,
                    popularity        = m.get("popularity"),
                    vote_average      = m.get("vote_average"),
                    vote_count        = m.get("vote_count"),
                    original_language = m.get("original_language"),
                    overview          = m.get("overview"),
                    genre_ids         = m.get("genre_ids", []),
                )
            except ValidationError as e:
                log.warning(f"Validation failed for movie {m.get('id')}: {e}")
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO raw.raw_movies (
                    tmdb_id, title, release_date, popularity,
                    vote_average, vote_count, original_language,
                    overview, genre_ids, ingested_at_date, pipeline_run_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tmdb_id, ingested_at_date) DO NOTHING;
            """, (
                movie.tmdb_id,
                movie.title,
                movie.release_date,
                movie.popularity,
                movie.vote_average,
                movie.vote_count,
                movie.original_language,
                movie.overview,
                psycopg2.extras.Json(movie.genre_ids),
                date.today(),
                run_id,
            ))
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1
            tmdb_ids.append(movie.tmdb_id)

    conn.commit()
    cur.close()
    conn.close()

    # A 200 response with an empty/thin results list (bad query params,
    # quota exhaustion returning empty-but-valid JSON, etc.) doesn't raise
    # on its own - fetch_tmdb's raise_for_status only catches actual HTTP
    # failures. This catches the silent-empty case instead.
    if total_fetched == 0:
        raise Exception(
            "ingest_tmdb_movies: zero rows ingested — API may be down or "
            "returning empty response. Check TMDB connectivity."
        )
    if total_fetched < TMDB_MOVIES_MIN:
        log.warning(
            f"ingest_tmdb_movies: only {total_fetched} rows — below expected "
            f"minimum of {TMDB_MOVIES_MIN}. Possible partial response from TMDB."
        )

    log.info(f"TMDB movies: {inserted} inserted, {skipped} skipped, {lang_skipped} dropped (non-preferred language)")
    return list(set(tmdb_ids))

# ── Op 2: enrich movie details ────────────────────────────────
# No row-count guard here on purpose: this only processes new ids plus ones
# older than DETAILS_REFRESH_DAYS. Zero to enrich is a normal steady state -
# not a signal the API is down, so a zero-guard would fire false alarms.

@op
def ingest_tmdb_details(context, movie_ids: list):
    log  = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0

    region = get_watch_region(cur)
    to_enrich, new_count, stale_count = get_ids_to_enrich(cur, "raw_movie_details", movie_ids)
    log.info(f"Movie details: {new_count} new, {stale_count} stale (>{DETAILS_REFRESH_DAYS}d), region {region}")

    for tmdb_id in to_enrich:
        try:
            # One call instead of three: append_to_response embeds credits
            # and watch providers in the details payload.
            detail    = fetch_tmdb(f"/movie/{tmdb_id}?append_to_response=credits,watch/providers")
            credits   = detail.get("credits", {})
            providers = detail.get("watch/providers", {})

            md = MovieDetails(
                tmdb_id    = tmdb_id,
                title      = detail.get("title", ""),
                original_language = detail.get("original_language"),
                runtime_mins = detail.get("runtime"),
                budget     = detail.get("budget"),
                revenue    = detail.get("revenue"),
                director   = get_director(credits),
                genres     = [g["name"] for g in detail.get("genres", [])],
                platforms  = get_platforms(providers, region),
                poster_path = detail.get("poster_path"),
            )
        except (ValidationError, Exception) as e:
            log.warning(f"Detail fetch failed for {tmdb_id}: {e}")
            continue

        cur.execute("""
            INSERT INTO raw.raw_movie_details (
                tmdb_id, title, original_language, runtime_mins, budget,
                revenue, director, genres, platforms, poster_path, pipeline_run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (tmdb_id) DO UPDATE SET
                title        = EXCLUDED.title,
                original_language = EXCLUDED.original_language,
                runtime_mins = EXCLUDED.runtime_mins,
                budget       = EXCLUDED.budget,
                revenue      = EXCLUDED.revenue,
                director     = EXCLUDED.director,
                genres       = EXCLUDED.genres,
                platforms    = EXCLUDED.platforms,
                poster_path  = EXCLUDED.poster_path,
                ingested_at  = NOW(),
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """, (
            md.tmdb_id,
            md.title,
            md.original_language,
            md.runtime_mins,
            md.budget,
            md.revenue,
            md.director,
            psycopg2.extras.Json(md.genres),
            psycopg2.extras.Json(md.platforms),
            md.poster_path,
            run_id,
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"TMDB details: {inserted} enriched")

# ── Op 3: discover TV shows ─────────────────────────────────────

@op
def ingest_tmdb_shows(context):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0
    skipped  = 0
    lang_skipped = 0
    tmdb_ids = []
    total_fetched = 0

    preferred_languages = set(get_preferred_languages(cur))
    endpoints = [
        "/tv/popular?page=1",
        "/tv/top_rated?page=1",
        "/trending/tv/day",
    ]
    for lang in preferred_languages:
        endpoints.append(f"/discover/tv?with_original_language={lang}&sort_by=popularity.desc&page=1")

    for ep in endpoints:
        data = fetch_tmdb(ep)
        shows = data.get("results", [])
        total_fetched += len(shows)

        for s in shows:
            # Same reasoning as ingest_tmdb_movies - /tv/popular, /tv/top_rated,
            # and /trending/tv/day are global charts, not language-scoped.
            if s.get("original_language") not in preferred_languages:
                lang_skipped += 1
                continue
            try:
                show = TVRaw(
                    tmdb_id           = s["id"],
                    title             = s["name"],
                    first_air_date    = s.get("first_air_date") or None,
                    popularity        = s.get("popularity"),
                    vote_average      = s.get("vote_average"),
                    vote_count        = s.get("vote_count"),
                    original_language = s.get("original_language"),
                    overview          = s.get("overview"),
                    genre_ids         = s.get("genre_ids", []),
                )
            except ValidationError as e:
                log.warning(f"Validation failed for show {s.get('id')}: {e}")
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO raw.raw_tv (
                    tmdb_id, title, first_air_date, popularity,
                    vote_average, vote_count, original_language,
                    overview, genre_ids, ingested_at_date, pipeline_run_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tmdb_id, ingested_at_date) DO NOTHING;
            """, (
                show.tmdb_id,
                show.title,
                show.first_air_date,
                show.popularity,
                show.vote_average,
                show.vote_count,
                show.original_language,
                show.overview,
                psycopg2.extras.Json(show.genre_ids),
                date.today(),
                run_id,
            ))
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1
            tmdb_ids.append(show.tmdb_id)

    conn.commit()
    cur.close()
    conn.close()

    if total_fetched == 0:
        raise Exception(
            "ingest_tmdb_shows: zero rows ingested — API may be down or "
            "returning empty response. Check TMDB connectivity."
        )
    if total_fetched < TMDB_SHOWS_MIN:
        log.warning(
            f"ingest_tmdb_shows: only {total_fetched} rows — below expected "
            f"minimum of {TMDB_SHOWS_MIN}. Possible partial response from TMDB."
        )

    log.info(f"TMDB shows: {inserted} inserted, {skipped} skipped, {lang_skipped} dropped (non-preferred language)")
    return list(set(tmdb_ids))

# ── Op 4: enrich TV show details ────────────────────────────────
# Same reasoning as ingest_tmdb_details above - no guard, zero-new is normal.

@op
def ingest_tmdb_tv_details(context, show_ids: list):
    log  = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0

    region = get_watch_region(cur)
    to_enrich, new_count, stale_count = get_ids_to_enrich(cur, "raw_tv_details", show_ids)
    log.info(f"TV details: {new_count} new, {stale_count} stale (>{DETAILS_REFRESH_DAYS}d), region {region}")

    for tmdb_id in to_enrich:
        try:
            detail    = fetch_tmdb(f"/tv/{tmdb_id}?append_to_response=watch/providers")
            providers = detail.get("watch/providers", {})

            td = TVDetails(
                tmdb_id              = tmdb_id,
                title                = detail.get("name", ""),
                original_language    = detail.get("original_language"),
                number_of_seasons    = detail.get("number_of_seasons"),
                number_of_episodes   = detail.get("number_of_episodes"),
                episode_runtime_mins = (detail.get("episode_run_time") or [None])[0],
                creator              = get_creator(detail),
                genres               = [g["name"] for g in detail.get("genres", [])],
                platforms            = get_platforms(providers, region),
                poster_path          = detail.get("poster_path"),
            )
        except (ValidationError, Exception) as e:
            log.warning(f"Detail fetch failed for show {tmdb_id}: {e}")
            continue

        cur.execute("""
            INSERT INTO raw.raw_tv_details (
                tmdb_id, title, original_language, number_of_seasons, number_of_episodes,
                episode_runtime_mins, creator, genres, platforms, poster_path, pipeline_run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (tmdb_id) DO UPDATE SET
                title                = EXCLUDED.title,
                original_language    = EXCLUDED.original_language,
                number_of_seasons    = EXCLUDED.number_of_seasons,
                number_of_episodes   = EXCLUDED.number_of_episodes,
                episode_runtime_mins = EXCLUDED.episode_runtime_mins,
                creator              = EXCLUDED.creator,
                genres               = EXCLUDED.genres,
                platforms            = EXCLUDED.platforms,
                poster_path          = EXCLUDED.poster_path,
                ingested_at          = NOW(),
                pipeline_run_id      = EXCLUDED.pipeline_run_id;
        """, (
            td.tmdb_id,
            td.title,
            td.original_language,
            td.number_of_seasons,
            td.number_of_episodes,
            td.episode_runtime_mins,
            td.creator,
            psycopg2.extras.Json(td.genres),
            psycopg2.extras.Json(td.platforms),
            td.poster_path,
            run_id,
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"TMDB TV details: {inserted} enriched")