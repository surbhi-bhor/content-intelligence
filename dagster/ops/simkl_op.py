import os
import requests
import psycopg2
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ValidationError
from dagster import op, Out, Output, get_dagster_logger

from ops import NETWORK_RETRY

# ── Pydantic model ───────────────────────────────────────────

class RatingRaw(BaseModel):
    simkl_id: int
    tmdb_id: Optional[int] = None
    title: str
    content_type: str
    status: Optional[str] = None
    rating: Optional[int] = None
    watched_at: Optional[datetime] = None

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

def fetch_simkl_ratings() -> dict:
    url = "https://api.simkl.com/sync/ratings/"
    headers = {
        "Authorization": f"Bearer {os.getenv('SIMKL_ACCESS_TOKEN')}",
        "simkl-api-key": os.getenv("SIMKL_CLIENT_ID"),
    }
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

# ── Op: ingest ratings ───────────────────────────────────────

SIMKL_RATINGS_MIN = 1

@op(out={"movie_ids": Out(), "show_ids": Out()}, retry_policy=NETWORK_RETRY)
def ingest_simkl_ratings(context):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    tmdb_movie_ids = []
    tmdb_show_ids = []
    total_fetched = 0

    data = fetch_simkl_ratings() or {}

    for content_type, key in [("movie", "movies"), ("show", "shows")]:
        items = data.get(key) or []
        total_fetched += len(items)
        seen_ids = []
        validation_failed = False

        for item in items:
            media = item.get(content_type, {})
            raw_tmdb_id = media.get("ids", {}).get("tmdb")

            try:
                rating = RatingRaw(
                    simkl_id=media.get("ids", {}).get("simkl"),
                    tmdb_id=int(raw_tmdb_id) if raw_tmdb_id else None,
                    title=media.get("title", ""),
                    content_type=content_type,
                    status=item.get("status"),
                    rating=item.get("user_rating"),
                    watched_at=item.get("user_rated_at") or item.get("last_watched_at"),
                )
            except ValidationError as e:
                log.warning(f"Validation failed for {content_type} rating: {e}")
                skipped += 1
                validation_failed = True
                continue

            cur.execute("""
                INSERT INTO raw.raw_ratings (
                    simkl_id, tmdb_id, title, content_type, status, rating, watched_at, pipeline_run_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (simkl_id, content_type) DO UPDATE SET
                    tmdb_id     = EXCLUDED.tmdb_id,
                    status      = EXCLUDED.status,
                    rating      = EXCLUDED.rating,
                    watched_at  = EXCLUDED.watched_at,
                    ingested_at = CASE
                        WHEN raw_ratings.tmdb_id     IS DISTINCT FROM EXCLUDED.tmdb_id
                          OR raw_ratings.status      IS DISTINCT FROM EXCLUDED.status
                          OR raw_ratings.rating      IS DISTINCT FROM EXCLUDED.rating
                          OR raw_ratings.watched_at  IS DISTINCT FROM EXCLUDED.watched_at
                        THEN NOW()
                        ELSE raw_ratings.ingested_at
                    END,
                    pipeline_run_id = EXCLUDED.pipeline_run_id;
            """, (
                rating.simkl_id,
                rating.tmdb_id,
                rating.title,
                rating.content_type,
                rating.status,
                rating.rating,
                rating.watched_at,
                run_id,
            ))
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1
            seen_ids.append(rating.simkl_id)

            if rating.tmdb_id:
                if content_type == "movie":
                    tmdb_movie_ids.append(rating.tmdb_id)
                else:
                    tmdb_show_ids.append(rating.tmdb_id)

        # Delete reconciliation treats "not in this response" as "removed in
        # Simkl" - only safe when the response for this type is complete.
        # An empty list (auth expired, API change, missing key) would make
        # `simkl_id <> ALL('{}')` match every row and wipe the whole type;
        # a validation failure would wrongly delete that one item. Skip the
        # delete in either case - a missed delete self-heals next run, a
        # wrong one loses watch history.
        if not seen_ids:
            log.warning(f"Simkl returned no {key} — skipping delete reconciliation for {content_type}")
        elif validation_failed:
            log.warning(f"Validation failures in {key} — skipping delete reconciliation for {content_type}")
        else:
            cur.execute("""
                DELETE FROM raw.raw_ratings
                WHERE content_type = %s AND simkl_id <> ALL(%s);
            """, (content_type, seen_ids))
            if cur.rowcount:
                log.info(f"Removed {cur.rowcount} {content_type}(s) no longer in Simkl watchlist")

    conn.commit()
    cur.close()
    conn.close()

    # Counting API response size (total_fetched), not DB rows written -
    # raw_ratings only bumps ingested_at when a value actually changed
    # (see the UPSERT's CASE above), so a normal day where nothing changed
    # would show 0 recently-written rows even on a fully healthy run.
    # 0 here specifically means Simkl returned nothing at all, which the
    # task treats as a real failure (auth broke / endpoint changed), not
    # "nothing new was rated": the account always has existing rated history.
    if total_fetched == 0:
        raise Exception(
            "ingest_simkl_ratings: zero rows ingested — API may be down or "
            "returning empty response. Check Simkl connectivity."
        )
    if total_fetched < SIMKL_RATINGS_MIN:
        log.warning(
            f"ingest_simkl_ratings: only {total_fetched} rows — below expected "
            f"minimum of {SIMKL_RATINGS_MIN}. Possible partial response from Simkl."
        )

    log.info(f"Simkl ratings: {inserted} inserted/updated, {skipped} skipped")
    yield Output(list(set(tmdb_movie_ids)), "movie_ids")
    yield Output(list(set(tmdb_show_ids)), "show_ids")
