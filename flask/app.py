import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import psycopg2
from flask import Flask, render_template, request, jsonify, redirect, url_for

from agent import answer_question

app = Flask(__name__)

# Postgres and the containers run in UTC, so every stored timestamp is UTC.
# Comparisons stay in UTC; this filter only changes how times are displayed.
DISPLAY_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Kolkata"))

@app.template_filter("local")
def local_time(value, fmt="%b %d %Y %I:%M%p %Z"):
    """Render a naive UTC datetime in the display timezone, e.g. 'Oct 06 2026 10:32PM IST'."""
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(DISPLAY_TZ).strftime(fmt)

LANGUAGE_NAMES = {"en": "English", "hi": "Hindi", "mr": "Marathi"}

def runtime_label(content_type, runtime_mins, number_of_seasons, number_of_episodes):
    """One line the user can actually use to decide: runtime for a movie,
    season/episode count for a show - so a 300-episode daily soap and a
    6-episode limited series don't look like the same commitment."""
    if content_type == "movie":
        return f"{runtime_mins} min" if runtime_mins else None
    if content_type == "tv":
        parts = []
        if number_of_seasons:
            parts.append(f"{number_of_seasons} season" + ("s" if number_of_seasons != 1 else ""))
        if number_of_episodes:
            parts.append(f"{number_of_episodes} ep")
        return " · ".join(parts) if parts else None
    return None

def watch_url(content_id: str):
    """TMDB's own page for the title - always resolvable from content_id
    (movie_<id> / tv_<id>), and TMDB's page itself lists watch providers,
    so this doubles as a real 'where to watch' link without needing a
    separate affiliate/deep-link API."""
    if content_id and content_id.startswith("movie_"):
        return f"https://www.themoviedb.org/movie/{content_id[len('movie_'):]}"
    if content_id and content_id.startswith("tv_"):
        return f"https://www.themoviedb.org/tv/{content_id[len('tv_'):]}"
    return None

def book_url(content_id: str):
    """content_id for an OpenLibrary-matched book is 'book_/works/OL...W' -
    that's a real OpenLibrary work path already, no extra lookup needed.
    Hardcover-only books (no OL match, e.g. 'book_manual_17256130') have no
    known-correct URL scheme here, so they get no link rather than a guess."""
    if content_id and content_id.startswith("book_/works/"):
        return f"https://openlibrary.org{content_id[len('book_'):]}"
    return None

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

@contextmanager
def db_cursor():
    """Yields (conn, cur) and always closes the connection - Flask is a
    long-running process, so a connection left open by an exception mid-route
    leaks until Postgres hits max_connections. Callers commit explicitly;
    closing an uncommitted connection rolls it back."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            yield conn, cur
    finally:
        conn.close()

def get_taste_profile(cur):
    cur.execute("""
        SELECT top_genres_watch, top_creators_watch, avg_rating_by_type,
               total_rated, generated_at
        FROM meta.taste_profile
        ORDER BY generated_at DESC
        LIMIT 1;
    """)
    row = cur.fetchone()
    if row is None:
        return None

    top_genres, top_creators, avg_by_type, total_rated, generated_at = row
    return {
        "top_genres": (top_genres or [])[:3],
        "top_creators": (top_creators or [])[:3],
        "avg_by_type": avg_by_type or {},
        "total_rated": total_rated,
        "generated_at": generated_at,
    }

def get_daily_picks(cur):
    cur.execute("""
        SELECT
            dr.content_id, dr.rank, dw.title, dr.content_type, dw.original_language,
            dw.vote_average, dw.release_date, dw.primary_creator, dr.reason,
            primary_platform.platform_name AS platform, dr.predicted_score,
            dw.poster_path, dw.runtime_mins, dw.number_of_seasons, dw.number_of_episodes
        FROM meta.daily_recommendations_watch dr
        JOIN marts.dim_watchable dw ON dw.content_id = dr.content_id
        LEFT JOIN LATERAL (
            SELECT dp.platform_name
            FROM marts.bridge_content_platform bcp
            JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id
            WHERE bcp.content_id = dw.content_id
            ORDER BY length(dp.platform_name) ASC, dp.platform_name ASC
            LIMIT 1
        ) primary_platform ON true
        ORDER BY dr.rank;
    """)
    picks = [
        {
            "content_id": content_id,
            "rank": rank,
            "title": title,
            "content_type": content_type,
            "language": LANGUAGE_NAMES.get(language, language) if language else None,
            "vote_average": vote_average,
            "release_date": release_date,
            "creator": creator,
            "reason": reason,
            "platform": platform,
            "predicted_score": predicted_score,
            "poster_url": f"https://image.tmdb.org/t/p/w200{poster_path}" if poster_path else None,
            "runtime_mins": runtime_mins,
            "number_of_seasons": number_of_seasons,
            "number_of_episodes": number_of_episodes,
            "runtime_label": runtime_label(content_type, runtime_mins, number_of_seasons, number_of_episodes),
            "watch_url": watch_url(content_id),
        }
        for content_id, rank, title, content_type, language, vote_average, release_date, creator, reason, platform,
            predicted_score, poster_path, runtime_mins, number_of_seasons, number_of_episodes in cur.fetchall()
    ]

    cur.execute("""
        SELECT dr.content_id, dr.rank, db.title, db.vote_average, db.primary_creator,
               dr.reason, db.cover_url
        FROM meta.daily_recommendations_books dr
        JOIN marts.dim_book db ON db.content_id = dr.content_id
        ORDER BY dr.rank;
    """)
    book_picks = [
        {
            "content_id": content_id,
            "rank": rank,
            "title": title,
            "content_type": "book",
            "language": None,
            "vote_average": vote_average,
            "release_date": None,
            "creator": creator,
            "reason": reason,
            "platform": None,
            "predicted_score": None,
            "poster_url": cover_url,
            "runtime_mins": None,
            "number_of_seasons": None,
            "number_of_episodes": None,
            "book_url": book_url(content_id),
        }
        for content_id, rank, title, vote_average, creator, reason, cover_url in cur.fetchall()
    ]
    return picks, book_picks

def record_api_usage(feature: str, tokens: int):
    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO meta.api_usage (date, feature, tokens_used, call_count)
            VALUES (CURRENT_DATE, %s, %s, 1)
            ON CONFLICT (date, feature) DO UPDATE
            SET tokens_used = meta.api_usage.tokens_used + EXCLUDED.tokens_used,
                call_count = meta.api_usage.call_count + 1;
        """, (feature, tokens))
        conn.commit()

@app.route("/")
def index():
    with db_cursor() as (conn, cur):
        taste = get_taste_profile(cur)
        picks, book_picks = get_daily_picks(cur)

        cur.execute("""
            SELECT GREATEST(
                (SELECT max(generated_at) FROM meta.daily_recommendations_watch),
                (SELECT max(generated_at) FROM meta.daily_recommendations_books)
            );
        """)
        picks_generated_at = cur.fetchone()[0]

    return render_template(
        "index.html", picks=picks, book_picks=book_picks, taste=taste, picks_generated_at=picks_generated_at,
    )

@app.route("/tonight")
def tonight_redirect():
    return redirect(url_for("index"))

_LOW_VOTE_COUNT_LANGUAGES = {"hi", "mr"}
_MIN_VOTE_COUNT_DEFAULT = 10
_MIN_VOTE_COUNT_REGIONAL = 3
# Must match recommendation_op.py: the pipeline relaxes vote_average to 5.5
# for hi/mr. A flat >= 7 here meant a dismissed Hindi/Marathi pick almost
# never found a same-language replacement and fell back to another language.
_MIN_VOTE_AVERAGE_DEFAULT = 7
_MIN_VOTE_AVERAGE_REGIONAL = 5.5

def _get_excluded_genres(cur) -> list:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'excluded_genres';
    """)
    row = cur.fetchone()
    return row[0] if row else []

def _query_replacement_candidate(cur, top_genres, exclude_ids, languages, content_types):
    # Same reasoning as recommendation_op.py's get_candidates: TMDB vote
    # counts skew heavily toward English content, so a single global
    # threshold starves Hindi/Marathi candidates. Only relax it when every
    # language in this search is regional - a mixed search (fallback tier)
    # keeps the strict bar since it may include English.
    all_regional = bool(languages) and all(lang in _LOW_VOTE_COUNT_LANGUAGES for lang in languages)
    min_vote_count = _MIN_VOTE_COUNT_REGIONAL if all_regional else _MIN_VOTE_COUNT_DEFAULT
    min_vote_average = _MIN_VOTE_AVERAGE_REGIONAL if all_regional else _MIN_VOTE_AVERAGE_DEFAULT
    excluded_genres = _get_excluded_genres(cur)
    excluded_genre_sql = """
          AND NOT EXISTS (
              SELECT 1 FROM marts.bridge_content_genre ebcg
              JOIN marts.dim_genre eg ON eg.genre_id = ebcg.genre_id
              WHERE ebcg.content_id = dc.content_id AND eg.genre_name = ANY(%s)
          )""" if excluded_genres else ""
    cur.execute(f"""
        SELECT dc.content_id, dc.title, dc.content_type, dc.original_language,
               dc.vote_average, dc.release_date, dc.primary_creator,
               dc.poster_path, dc.runtime_mins, dc.number_of_seasons,
               dc.number_of_episodes,
               primary_platform.platform_name AS platform,
               (
                   SELECT COUNT(*) FROM marts.bridge_content_genre bcg
                   JOIN marts.dim_genre g ON g.genre_id = bcg.genre_id
                   WHERE bcg.content_id = dc.content_id AND g.genre_name = ANY(%s)
               ) AS genre_match_count
        FROM marts.dim_watchable dc
        LEFT JOIN LATERAL (
            SELECT dp.platform_name
            FROM marts.bridge_content_platform bcp
            JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id
            WHERE bcp.content_id = dc.content_id
            ORDER BY length(dp.platform_name) ASC, dp.platform_name ASC
            LIMIT 1
        ) primary_platform ON true
        WHERE dc.content_type = ANY(%s)
          AND dc.original_language = ANY(%s)
          AND dc.content_id NOT IN (SELECT content_id FROM marts.fact_watch_history)
          AND dc.content_id NOT IN (SELECT content_id FROM meta.not_interested)
          AND dc.content_id != ALL(%s)
          AND dc.vote_count >= %s
          AND dc.vote_average >= %s
          AND EXISTS (
              SELECT 1 FROM marts.bridge_content_platform bcp
              WHERE bcp.content_id = dc.content_id
          )
          -- Same rule as recommendation_op.py: no daily soaps, TV serials,
          -- reality or talk formats (defined once in dim_watchable).
          AND NOT dc.is_serial_format
          {excluded_genre_sql}
        ORDER BY genre_match_count DESC, dc.popularity DESC NULLS LAST
        LIMIT 1;
    """, (top_genres, content_types, languages, list(exclude_ids), min_vote_count, min_vote_average) + ((excluded_genres,) if excluded_genres else ()))
    return cur.fetchone()

def find_replacement_pick(cur, exclude_ids, target_language, target_content_type):
    """Lightweight standalone version of recommendation_op.py's candidate
    selection - Flask and Dagster are separate containers with no shared
    Python module, so the real scoring/LLM-selection pipeline can't be
    imported here. Same quality bar (vote_count/vote_average/has a
    platform), genre-preference-aware via taste_profile, but no LLM step -
    good enough for a live single-slot backfill. The weekly full
    regeneration in recommendation_op.py is unaffected and still uses the
    real algorithm.

    An earlier version searched across ALL preferred
    languages/types for the single best-scoring candidate - since English
    has a far bigger catalog than Hindi/Marathi, every dismissal kept
    getting backfilled with English TV, and 3 dismissals in a row left the
    whole picks list 5/5 English TV, violating the configured
    recommendation_language_slots (en:2, hi:2, mr:1) entirely. A
    replacement MUST preserve the language+type of the slot it's filling,
    same as the real pipeline's allocate_by_language - falling back to a
    looser search only if that exact slot genuinely has no candidates."""
    cur.execute("""
        SELECT top_genres_watch FROM meta.taste_profile
        ORDER BY generated_at DESC LIMIT 1;
    """)
    row = cur.fetchone()
    top_genres = [g["name"] for g in (row[0] or [])] if row else []

    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'recommendation_fallback_language';
    """)
    row = cur.fetchone()
    fallback_language = row[0] if row and row[0] else "en"

    all_types = ["movie", "tv"]
    tiers = [
        ([target_language], [target_content_type]),
        ([target_language], all_types),
        ([fallback_language], [target_content_type]),
        ([fallback_language], all_types),
    ]
    row = None
    for languages, content_types in tiers:
        row = _query_replacement_candidate(cur, top_genres, exclude_ids, languages, content_types)
        if row:
            break
    if row is None:
        return None

    (content_id, title, content_type, language, vote_average, release_date,
     creator, poster_path, runtime_mins, number_of_seasons, number_of_episodes,
     platform, genre_match_count) = row

    reason = (
        "Matches top genre — filling in for the dismissed pick."
        if genre_match_count > 0
        else "Filling in for the dismissed pick."
    )

    return {
        "content_id": content_id,
        "title": title,
        "content_type": content_type,
        "language": LANGUAGE_NAMES.get(language, language),
        "vote_average": vote_average,
        "release_date": release_date.isoformat() if release_date else None,
        "creator": creator,
        "reason": reason,
        "platform": platform,
        "predicted_score": None,
        "poster_url": f"https://image.tmdb.org/t/p/w200{poster_path}" if poster_path else None,
        "runtime_mins": runtime_mins,
        "number_of_seasons": number_of_seasons,
        "number_of_episodes": number_of_episodes,
        "runtime_label": runtime_label(content_type, runtime_mins, number_of_seasons, number_of_episodes),
        "watch_url": watch_url(content_id),
    }

def find_book_replacement_pick(cur, exclude_ids):
    """Books have no language and discovered candidates have no rating
    data, so this can't reuse find_replacement_pick's tiered
    language/type search at all - same reasoning as
    recommendation_op.py's select_book_picks. Subject match + author match
    against top_genres_read/top_creators_read is the available signal."""
    cur.execute("SELECT top_genres_read, top_creators_read FROM meta.taste_profile ORDER BY generated_at DESC LIMIT 1;")
    row = cur.fetchone()
    subject_names = [g["name"] for g in (row[0] or [])] if row else []
    creator_names = [c["name"] for c in (row[1] or [])] if row else []

    cur.execute("""
        SELECT dc.content_id, dc.title, dc.primary_creator, dc.vote_average, dc.cover_url,
               dc.page_count,
               array_agg(DISTINCT dbs.subject_name) FILTER (WHERE dbs.subject_name IS NOT NULL) AS subjects
        FROM marts.dim_book dc
        LEFT JOIN marts.bridge_content_book_subject bcbs ON bcbs.content_id = dc.content_id
        LEFT JOIN marts.dim_book_subject dbs ON dbs.subject_id = bcbs.subject_id
        WHERE dc.content_id NOT IN (SELECT content_id FROM marts.fact_reading_history)
          AND dc.content_id NOT IN (SELECT content_id FROM meta.not_interested)
          AND dc.content_id != ALL(%s)
        GROUP BY dc.content_id, dc.title, dc.primary_creator, dc.vote_average, dc.cover_url, dc.page_count;
    """, (list(exclude_ids),))
    rows = cur.fetchall()
    if not rows:
        return None

    def score(row):
        subjects = row[6] or []
        match_count = sum(1 for s in subjects if s in subject_names)
        creator_is_top = bool(row[2]) and row[2] in creator_names
        return (match_count + (2 if creator_is_top else 0), row[3] if row[3] is not None else 0)

    content_id, title, creator, vote_average, cover_url, page_count, subjects = max(rows, key=score)
    subjects = subjects or []
    matched = next((s for s in subjects if s in subject_names), None)
    creator_is_top = bool(creator) and creator in creator_names
    if creator_is_top and matched:
        reason = f"By {creator} — top-rated author, in {matched}."
    elif creator_is_top:
        reason = f"By {creator}, top-rated author."
    elif matched:
        reason = f"Matches favorite subject: {matched}."
    elif creator:
        reason = f"By {creator} — worth discovering."
    else:
        reason = "A new title worth discovering."

    return {
        "content_id": content_id,
        "title": title,
        "content_type": "book",
        "language": None,
        "vote_average": vote_average,
        "release_date": None,
        "creator": creator,
        "reason": reason,
        "platform": None,
        "predicted_score": None,
        "poster_url": cover_url,
        "runtime_mins": None,
        "number_of_seasons": None,
        "number_of_episodes": None,
        "book_url": book_url(content_id),
    }

@app.route("/not-interested", methods=["POST"])
def not_interested():
    data = request.get_json(silent=True) or {}
    content_id = (data.get("content_id") or "").strip()
    if not content_id:
        return jsonify({"error": "Missing content_id."}), 400

    with db_cursor() as (conn, cur):
        cur.execute("""
            INSERT INTO meta.not_interested (content_id, marked_at)
            VALUES (%s, NOW())
            ON CONFLICT (content_id) DO NOTHING;
        """, (content_id,))

        # Dismissed pick's type comes from its content_id prefix, not from
        # whether a lookup row happens to exist - a stale/raced content_id (e.g.
        # already removed by an earlier dismiss) used to silently fall through
        # to "must be a book" and backfill a book into the movie/TV grid
        # (a dismissed movie pick once returned a book replacement
        # that the client then rendered into the picks grid, since the client
        # trusts isPickCard from the dismissed DOM element, not the replacement
        # it got back).
        is_book = content_id.startswith("book_")
        if is_book:
            cur.execute("SELECT rank FROM meta.daily_recommendations_books WHERE content_id = %s;", (content_id,))
            book_row = cur.fetchone()
            rank = book_row[0] if book_row else None
            dismissed_type, dismissed_lang = "book", None
        else:
            cur.execute("""
                SELECT dr.rank, dr.content_type, dw.original_language
                FROM meta.daily_recommendations_watch dr
                JOIN marts.dim_watchable dw ON dw.content_id = dr.content_id
                WHERE dr.content_id = %s;
            """, (content_id,))
            row = cur.fetchone()
            if row is None:
                # Pick already gone (double-click, stale tab) - still commit the
                # not_interested insert above so the dismissal sticks.
                conn.commit()
                return jsonify({"ok": True, "replacement": None})
            rank, dismissed_type, dismissed_lang = row

        if is_book:
            cur.execute("DELETE FROM meta.daily_recommendations_books WHERE content_id = %s;", (content_id,))
            cur.execute("SELECT content_id FROM meta.daily_recommendations_books;")
        else:
            cur.execute("DELETE FROM meta.daily_recommendations_watch WHERE content_id = %s;", (content_id,))
            cur.execute("SELECT content_id FROM meta.daily_recommendations_watch;")
        shown_ids = [r[0] for r in cur.fetchall()] + [content_id]

        if dismissed_type == "book":
            replacement = find_book_replacement_pick(cur, shown_ids)
        else:
            replacement = find_replacement_pick(cur, shown_ids, dismissed_lang, dismissed_type)
        if replacement and rank is not None:
            if dismissed_type == "book":
                cur.execute("""
                    INSERT INTO meta.daily_recommendations_books (content_id, title, rank, reason)
                    VALUES (%s, %s, %s, %s);
                """, (replacement["content_id"], replacement["title"], rank, replacement["reason"]))
            else:
                cur.execute("""
                    INSERT INTO meta.daily_recommendations_watch
                        (content_id, title, content_type, rank, reason, predicted_score)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (
                    replacement["content_id"], replacement["title"], replacement["content_type"],
                    rank, replacement["reason"], replacement["predicted_score"],
                ))
            replacement["rank"] = rank

        conn.commit()
        return jsonify({"ok": True, "replacement": replacement})

@app.route("/usage")
def usage():
    # No external quota anymore (fully local Ollama, no paid API) - this just
    # shows call volume/response-token size for observability, not a limit.
    with db_cursor() as (conn, cur):
        cur.execute("""
            SELECT feature, tokens_used, call_count FROM meta.api_usage
            WHERE date = CURRENT_DATE;
        """)
        rows = cur.fetchall()

    features = {
        feature: {"tokens_used": tokens_used, "call_count": call_count}
        for feature, tokens_used, call_count in rows
    }
    return jsonify({"date": time.strftime("%Y-%m-%d"), "backend": "ollama", "features": features})

# ── /health ───────────────────────────────────────────────────
# Read-only status page - no writes. Reuses the same MAX(generated_at) read
# for both "last pipeline run" and the staleness check so it's one query,
# not two round-trips for related facts.
@app.route("/health")
def health():
    with db_cursor() as (conn, cur):

        cur.execute("""
            SELECT GREATEST(
                (SELECT max(generated_at) FROM meta.daily_recommendations_watch),
                (SELECT max(generated_at) FROM meta.daily_recommendations_books)
            );
        """)
        last_run = cur.fetchone()[0]

        # Not date-filtered: the pipeline DELETEs and re-INSERTs this table in
        # full on every run (daily or weekly), so a plain COUNT(*) already is
        # "current picks" - a generated_at::date = CURRENT_DATE filter only
        # made sense back when the job ran every day; on the current weekly
        # (Friday) cadence it would read zero picks on the other six days and
        # falsely flag the app as red.
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM meta.daily_recommendations_watch)
              + (SELECT COUNT(*) FROM meta.daily_recommendations_books);
        """)
        picks_today = cur.fetchone()[0]

        # Latest failed Dagster run (written by dagster/sensors.py). Only counts
        # against status while it's newer than the last successful picks - a
        # failure followed by a good run is history, not a current problem.
        cur.execute("""
            SELECT job_name, failed_at, run_id
            FROM meta.pipeline_alerts
            ORDER BY failed_at DESC
            LIMIT 1;
        """)
        last_failure = cur.fetchone()

        table_counts = {}
        for schema, table in [
            ("raw", "raw_movies"), ("raw", "raw_ratings"), ("raw", "raw_books"),
            ("marts", "fact_watch_history"), ("marts", "dim_watchable"),
            ("meta", "daily_recommendations_watch"), ("meta", "daily_recommendations_books"), ("meta", "taste_profile"),
        ]:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
            table_counts[f"{schema}.{table}"] = cur.fetchone()[0]

    watchlist_rows = table_counts["marts.fact_watch_history"]
    # 8-day window, not 24h: the pipeline runs weekly (Friday noon IST),
    # not nightly, so a 24-hour staleness check would show yellow on
    # every single day except run day itself. 8 days = one week plus a
    # day of slack for a run that lands a bit late.
    is_stale = last_run is not None and (datetime.now() - last_run) > timedelta(days=8)

    failure_is_current = last_failure is not None and (last_run is None or last_failure[1] > last_run)

    if picks_today > 0 and watchlist_rows > 0 and not failure_is_current:
        status = "yellow" if is_stale else "green"
    else:
        status = "red"

    ai_model = os.getenv("OLLAMA_ASK_MODEL", "llama3.2:3b")

    return render_template(
        "health.html",
        last_run=last_run,
        picks_today=picks_today,
        table_counts=table_counts,
        ai_backend=f"Ollama (local) — {ai_model}",
        status=status,
        last_failure=last_failure,
        failure_is_current=failure_is_current,
    )

# ── /ask ──────────────────────────────────────────────────────
# flask/agent.py handles everything: generating SQL, verifying it by actually
# running it against the read-only ask_readonly role (retrying on failure -
# best-of-N against a free verifier), and phrasing the answer from real rows
# only. No keyword detection, no manual SQL templates here.

_TEXT_NORMALIZE_MAP = str.maketrans({
    "‑": "-", "–": "-", "—": "-",   # non-breaking hyphen, en dash, em dash
    "‘": "'", "’": "'",                   # curly single quotes
    "“": '"', "”": '"',                   # curly double quotes
    " ": " ", " ": " ",                   # narrow/non-breaking space
})

def normalize_answer_text(text: str) -> str:
    """The model is told to write plain text only, but it
    doesn't reliably obey that (curly quotes, narrow hyphens, markdown bold
    still leak through) - same lesson as everywhere else this session: don't
    trust prompt-only compliance for anything that actually matters, fix it
    deterministically instead."""
    text = text.translate(_TEXT_NORMALIZE_MAP)
    text = text.replace("**", "").replace("`", "")
    return text

@app.route("/ask", methods=["POST"])
def ask_submit():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()

    if not question:
        return jsonify({"error": "Ask something first."}), 400
    if len(question) > 500:
        return jsonify({"error": "That question's too long — try rephrasing shorter."}), 400

    start = time.time()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        result = answer_question(question)
        answer = normalize_answer_text(result["answer"])
        response_time = round(time.time() - start, 2)
        record_api_usage("ask", result["tokens"])
        print(f"[{timestamp}] Q: {question}")
        print(f'[ASK] Q: "{question}" | Grounded: {result["grounded"]} | '
              f'Tokens: {result["tokens"]:,} | Time: {response_time}s')
        return jsonify({
            "answer": answer,
            "type": result.get("type", "prose"),
            "data": result.get("data", {}),
            "response_time": response_time,
            "backend": "ollama",
            "source": "personal data",
        })
    except Exception as e:
        response_time = round(time.time() - start, 2)
        print(f"[{timestamp}] Q: {question}")
        print(f"[{timestamp}] ERROR ({response_time}s) [ollama]: {e}")
        return jsonify({
            "answer": "Could not process that. Try rephrasing.",
            "type": "prose",
            "data": {},
            "response_time": response_time,
            "backend": "ollama",
            "source": "personal data",
        })

if __name__ == "__main__":
    # Werkzeug's debugger allows arbitrary code execution from the browser -
    # never on by default on a 0.0.0.0 bind. Opt in with FLASK_DEBUG=1. The
    # auto-reloader is separate and harmless, so it stays on for the
    # bind-mounted ./flask dev loop.
    app.run(
        host="0.0.0.0", port=5000, threaded=True,
        debug=os.getenv("FLASK_DEBUG") == "1", use_reloader=True,
    )
