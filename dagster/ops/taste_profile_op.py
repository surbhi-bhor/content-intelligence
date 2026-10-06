import os
import psycopg2
import psycopg2.extras
from dagster import op, get_dagster_logger

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

def rows_to_signal(rows):
    return [{"name": name, "avg_rating": round(float(avg), 2), "count": count} for name, avg, count in rows]

def get_genre_signal(cur, domain, direction):
    # Watch and read are now fully separate tables (fact_watch_history/
    # dim_genre for movies+TV, fact_reading_history/dim_book_subject for
    # books) - no shared content_type filter needed since each fact table
    # only ever holds its own domain's rows.
    is_book = domain == "read"
    fact_table = "marts.fact_reading_history" if is_book else "marts.fact_watch_history"
    bridge_table = "marts.bridge_content_book_subject" if is_book else "marts.bridge_content_genre"
    dim_table = "marts.dim_book_subject" if is_book else "marts.dim_genre"
    id_col = "subject_id" if is_book else "genre_id"
    name_col = "subject_name" if is_book else "genre_name"

    # Books: generic Open Library tags ("Fiction", "New York Times
    # bestseller") sit on most books and crowded out every real subject, and
    # a library of a few dozen books rarely has 5 per subject, so books use
    # specific subjects only and a lower floor.
    generic_filter = "AND NOT g.is_generic" if is_book else ""
    min_count = 2 if is_book else 5

    order = "DESC" if direction == "top" else "ASC"
    rating_cmp = ">= 7" if direction == "top" else "<= 6"
    cur.execute(f"""
        SELECT g.{name_col}, avg(fw.rating), count(*)
        FROM {fact_table} fw
        JOIN {bridge_table} bcg ON bcg.content_id = fw.content_id
        JOIN {dim_table} g ON g.{id_col} = bcg.{id_col}
        WHERE fw.has_rating {generic_filter}
        GROUP BY g.{name_col}
        HAVING count(*) >= {min_count} AND avg(fw.rating) {rating_cmp}
        ORDER BY avg(fw.rating) {order}
        LIMIT 10;
    """)
    return rows_to_signal(cur.fetchall())

def get_creator_signal(cur, domain, direction):
    is_book = domain == "read"
    fact_table = "marts.fact_reading_history" if is_book else "marts.fact_watch_history"
    dim_table = "marts.dim_book" if is_book else "marts.dim_watchable"

    order = "DESC" if direction == "top" else "ASC"
    rating_cmp = ">= 7" if direction == "top" else "<= 6"
    cur.execute(f"""
        SELECT dc.primary_creator, avg(fw.rating), count(*)
        FROM {fact_table} fw
        JOIN {dim_table} dc ON dc.content_id = fw.content_id
        WHERE fw.has_rating AND dc.primary_creator IS NOT NULL
        GROUP BY dc.primary_creator
        HAVING count(*) >= 2 AND avg(fw.rating) {rating_cmp}
        ORDER BY avg(fw.rating) {order}
        LIMIT 10;
    """)
    return rows_to_signal(cur.fetchall())

def build_section_text(label, top_genres, bottom_genres, top_creators, bottom_creators):
    lines = [f"--- {label} taste ---"]

    if top_genres:
        parts = [f"{g['name']} ({g['avg_rating']} avg, {g['count']} rated)" for g in top_genres]
        lines.append("Favorite genres: " + ", ".join(parts) + ".")

    if bottom_genres:
        parts = [f"{g['name']} ({g['avg_rating']} avg, {g['count']} rated)" for g in bottom_genres]
        lines.append("Genres to avoid: " + ", ".join(parts) + ".")

    if top_creators:
        parts = [f"{c['name']} ({c['avg_rating']} avg, {c['count']} rated)" for c in top_creators]
        lines.append("Favorite creators: " + ", ".join(parts) + ".")

    if bottom_creators:
        parts = [f"{c['name']} ({c['avg_rating']} avg, {c['count']} rated)" for c in bottom_creators]
        lines.append("Creators to avoid: " + ", ".join(parts) + ".")

    return "\n".join(lines)

def build_summary_text(watch, read, avg_by_type, total_rated):
    lines = [f"Based on {total_rated} rated titles."]

    type_parts = [f"{t}: {round(a, 2)} avg" for t, a in avg_by_type.items()]
    if type_parts:
        lines.append("Average rating by content type — " + ", ".join(type_parts) + ".")

    lines.append("")
    lines.append(build_section_text(
        "Movies & TV", watch["top_genres"], watch["bottom_genres"], watch["top_creators"], watch["bottom_creators"]
    ))
    lines.append("")
    lines.append(build_section_text(
        "Book", read["top_genres"], read["bottom_genres"], read["top_creators"], read["bottom_creators"]
    ))

    return "\n".join(lines)

# ── Op: build taste profile ─────────────────────────────────────

@op
def build_taste_profile(context, start=None):
    log = get_dagster_logger()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT (SELECT count(*) FROM marts.fact_watch_history WHERE has_rating)
             + (SELECT count(*) FROM marts.fact_reading_history WHERE has_rating);
    """)
    total_rated = cur.fetchone()[0]

    watch = {
        "top_genres": get_genre_signal(cur, "watch", "top"),
        "bottom_genres": get_genre_signal(cur, "watch", "bottom"),
        "top_creators": get_creator_signal(cur, "watch", "top"),
        "bottom_creators": get_creator_signal(cur, "watch", "bottom"),
    }
    read = {
        "top_genres": get_genre_signal(cur, "read", "top"),
        "bottom_genres": get_genre_signal(cur, "read", "bottom"),
        "top_creators": get_creator_signal(cur, "read", "top"),
        "bottom_creators": get_creator_signal(cur, "read", "bottom"),
    }

    cur.execute("""
        SELECT content_type, avg(rating)
        FROM marts.fact_watch_history
        WHERE has_rating
        GROUP BY content_type
        UNION ALL
        SELECT 'book', avg(rating)
        FROM marts.fact_reading_history
        WHERE has_rating;
    """)
    avg_by_type = {row[0]: round(float(row[1]), 2) for row in cur.fetchall() if row[1] is not None}

    summary_text = build_summary_text(watch, read, avg_by_type, total_rated)

    cur.execute("""
        INSERT INTO meta.taste_profile (
            summary_text,
            top_genres_watch, bottom_genres_watch, top_creators_watch, bottom_creators_watch,
            top_genres_read, bottom_genres_read, top_creators_read, bottom_creators_read,
            avg_rating_by_type, total_rated
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """, (
        summary_text,
        psycopg2.extras.Json(watch["top_genres"]),
        psycopg2.extras.Json(watch["bottom_genres"]),
        psycopg2.extras.Json(watch["top_creators"]),
        psycopg2.extras.Json(watch["bottom_creators"]),
        psycopg2.extras.Json(read["top_genres"]),
        psycopg2.extras.Json(read["bottom_genres"]),
        psycopg2.extras.Json(read["top_creators"]),
        psycopg2.extras.Json(read["bottom_creators"]),
        psycopg2.extras.Json(avg_by_type),
        total_rated,
    ))

    conn.commit()
    cur.close()
    conn.close()
    log.info(
        f"Taste profile built: {total_rated} rated titles, "
        f"{len(watch['top_genres'])} top watch genres, {len(read['top_genres'])} top read genres"
    )
    return summary_text
