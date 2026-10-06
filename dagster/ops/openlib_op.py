import os
import requests
import psycopg2
import psycopg2.extras
from typing import Optional
from pydantic import BaseModel, ValidationError
from dagster import op, get_dagster_logger

# ── Pydantic model ───────────────────────────────────────────

class BookMeta(BaseModel):
    ol_key: str
    hardcover_id: str
    title: str
    author: Optional[str] = None
    first_publish_year: Optional[int] = None
    subjects: Optional[list] = []
    ratings_average: Optional[float] = None
    ratings_count: Optional[int] = None
    page_count: Optional[int] = None
    cover_id: Optional[int] = None

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

def search_openlibrary(title: str, author: Optional[str]) -> Optional[dict]:
    main_title = title.split(":")[0].strip()
    url = "https://openlibrary.org/search.json"
    params = {
        "title": main_title,
        "limit": 5,
        "fields": "key,title,author_name,first_publish_year,subject,ratings_average,ratings_count,number_of_pages_median,cover_i",
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    docs = r.json().get("docs", [])

    if not author:
        return docs[0] if docs else None

    author_norm = author.strip().lower()
    for doc in docs:
        candidates = [a.strip().lower() for a in doc.get("author_name", [])]
        if any(author_norm in c or c in author_norm for c in candidates):
            return doc
    return None

# ── Op: enrich book metadata ────────────────────────────────────
# No row-count guard here on purpose: this only re-enriches books already
# in raw_book_ratings, one OpenLibrary search per title. Once the backlog
# is caught up, 0 newly-inserted/updated rows is the normal steady state
# (search matches found, but nothing about them changed), not a signal
# OpenLibrary is down - a zero-guard here would false-alarm constantly.

@op
def enrich_book_metadata(context, start=None):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    cur.execute("SELECT hardcover_id, title, author FROM raw.raw_book_ratings;")
    books = cur.fetchall()

    for hardcover_id, title, author in books:
        try:
            doc = search_openlibrary(title, author)
        except requests.exceptions.RequestException as e:
            log.warning(f"OpenLibrary request failed for '{title}': {e}")
            skipped += 1
            continue

        if doc is None:
            log.warning(f"No confident OpenLibrary match for '{title}' by {author}")
            skipped += 1
            continue

        try:
            meta = BookMeta(
                ol_key=doc["key"],
                hardcover_id=hardcover_id,
                title=doc.get("title", title),
                author=author,
                first_publish_year=doc.get("first_publish_year"),
                subjects=doc.get("subject", [])[:20],
                ratings_average=doc.get("ratings_average"),
                ratings_count=doc.get("ratings_count"),
                page_count=doc.get("number_of_pages_median"),
                cover_id=doc.get("cover_i"),
            )
        except (ValidationError, KeyError) as e:
            log.warning(f"Validation failed for '{title}': {e}")
            skipped += 1
            continue

        cur.execute("""
            INSERT INTO raw.raw_books (
                ol_key, hardcover_id, title, author, first_publish_year,
                subjects, ratings_average, ratings_count, page_count, cover_id, pipeline_run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ol_key) DO UPDATE SET
                hardcover_id        = EXCLUDED.hardcover_id,
                first_publish_year = EXCLUDED.first_publish_year,
                subjects            = EXCLUDED.subjects,
                ratings_average     = EXCLUDED.ratings_average,
                ratings_count       = EXCLUDED.ratings_count,
                page_count          = EXCLUDED.page_count,
                cover_id            = EXCLUDED.cover_id,
                ingested_at         = CASE
                    WHEN raw_books.hardcover_id       IS DISTINCT FROM EXCLUDED.hardcover_id
                      OR raw_books.first_publish_year IS DISTINCT FROM EXCLUDED.first_publish_year
                      OR raw_books.subjects            IS DISTINCT FROM EXCLUDED.subjects
                      OR raw_books.ratings_average     IS DISTINCT FROM EXCLUDED.ratings_average
                      OR raw_books.ratings_count       IS DISTINCT FROM EXCLUDED.ratings_count
                      OR raw_books.page_count          IS DISTINCT FROM EXCLUDED.page_count
                      OR raw_books.cover_id            IS DISTINCT FROM EXCLUDED.cover_id
                    THEN NOW()
                    ELSE raw_books.ingested_at
                END,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """, (
            meta.ol_key,
            meta.hardcover_id,
            meta.title,
            meta.author,
            meta.first_publish_year,
            psycopg2.extras.Json(meta.subjects),
            meta.ratings_average,
            meta.ratings_count,
            meta.page_count,
            meta.cover_id,
            run_id,
        ))
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"OpenLibrary enrichment: {inserted} inserted/updated, {skipped} skipped")
    return inserted

# ── Op: discover new (unread) books via OpenLibrary Subjects API ───────
# enrich_book_metadata above only ever enriches books already in the
# user's own Hardcover library (raw.raw_book_ratings) - there was no
# mechanism anywhere in this pipeline for surfacing a book the user hasn't
# already added themselves, unlike movies/TV which get a real discover/
# popular/trending pool from TMDB independent of watch history. This op
# is that missing discovery source for books, using OpenLibrary's public
# Subjects API (no auth, no key) against the reader's own top-rated real
# subjects from meta.taste_profile.

DISCOVERY_MIN_EDITION_COUNT = 3
REQUIRED_BOOK_LANGUAGE = "eng"  # this reader only reads English

def _is_english(doc: dict) -> bool:
    """OpenLibrary's 'language' field lists every edition's language for a
    work, not the language of the title actually returned - a foreign
    original work with an English translation edition still passes the
    language check while returning its original-language title (confirmed
    live: 'Krew elfów', a Polish title, passed language-only filtering
    since an English edition of that work exists). Requiring the returned
    title to be ASCII catches that class of miss without extra API calls -
    imperfect (rejects a genuine English title with an accented loanword,
    rare) but the honest cheap fix. A work with no language data is
    excluded too (can't confirm it's readable)."""
    title = doc.get("title") or ""
    return REQUIRED_BOOK_LANGUAGE in (doc.get("language") or []) and title.isascii()

def get_top_read_subjects(cur, limit=5) -> list:
    cur.execute("""
        SELECT top_genres_read FROM meta.taste_profile
        ORDER BY generated_at DESC LIMIT 1;
    """)
    row = cur.fetchone()
    if not row or not row[0]:
        return []
    return [g["name"] for g in row[0][:limit]]

def get_top_read_creators(cur, limit=5) -> list:
    cur.execute("""
        SELECT top_creators_read FROM meta.taste_profile
        ORDER BY generated_at DESC LIMIT 1;
    """)
    row = cur.fetchone()
    if not row or not row[0]:
        return []
    return [c["name"] for c in row[0][:limit]]

def get_known_ol_keys(cur) -> set:
    cur.execute("SELECT ol_key FROM raw.raw_books;")
    return {r[0] for r in cur.fetchall()}

_SEARCH_FIELDS = (
    "key,title,author_name,first_publish_year,subject,language,"
    "ratings_average,ratings_count,number_of_pages_median,"
    "cover_i,edition_count"
)

def fetch_subject_works(subject_name: str, limit: int = 20) -> list:
    """search.json's subject= facet (not the old /subjects/{slug}.json
    endpoint) - the old endpoint returns no per-work language data at all,
    making an English-only filter impossible there. search.json returns it,
    same shape as fetch_author_works below, so both discovery paths share
    one filtering/insertion pipeline."""
    url = "https://openlibrary.org/search.json"
    params = {"subject": subject_name, "limit": limit, "fields": _SEARCH_FIELDS}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("docs", [])

def fetch_author_works(author_name: str, limit: int = 20) -> list:
    """Unlike the old subjects endpoint, search.json returns real
    ratings_average/ratings_count per book - an author-driven discovery
    candidate gets a genuine quality signal, not just edition_count."""
    url = "https://openlibrary.org/search.json"
    params = {"author": author_name, "limit": limit, "fields": _SEARCH_FIELDS}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("docs", [])

DISCOVERY_MIN_RATINGS_COUNT = 5

def _insert_discovered_book(cur, ol_key, title, author, first_publish_year,
                             subjects, ratings_average, ratings_count,
                             page_count, cover_id, run_id) -> bool:
    try:
        meta = BookMeta(
            ol_key=ol_key, hardcover_id="", title=title, author=author,
            first_publish_year=first_publish_year, subjects=subjects,
            ratings_average=ratings_average, ratings_count=ratings_count,
            page_count=page_count, cover_id=cover_id,
        )
    except ValidationError as e:
        get_dagster_logger().warning(f"Validation failed for discovered book {ol_key!r}: {e}")
        return False

    cur.execute("""
        INSERT INTO raw.raw_books (
            ol_key, hardcover_id, title, author, first_publish_year,
            subjects, ratings_average, ratings_count, page_count, cover_id, pipeline_run_id
        ) VALUES (%s, NULL, %s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ol_key) DO NOTHING;
    """, (
        meta.ol_key, meta.title, meta.author, meta.first_publish_year,
        psycopg2.extras.Json(meta.subjects), meta.ratings_average,
        meta.ratings_count, meta.page_count, meta.cover_id, run_id,
    ))
    return bool(cur.rowcount)

def _process_discovery_docs(cur, docs, known_keys, run_id, fallback_subjects=None) -> tuple:
    """Shared by both discovery paths (subject-search and author-search now
    return the same search.json doc shape). English-only first (this reader
    only reads English), then a quality bar: real
    ratings_count when search.json provides it, edition_count as a fallback
    proxy when it doesn't."""
    inserted = skipped = 0
    for doc in docs:
        ol_key = doc.get("key")
        if not ol_key or ol_key in known_keys:
            continue
        if not _is_english(doc):
            skipped += 1
            continue

        ratings_count = doc.get("ratings_count")
        edition_count = doc.get("edition_count") or 0
        if ratings_count is not None:
            if ratings_count < DISCOVERY_MIN_RATINGS_COUNT:
                skipped += 1
                continue
        elif edition_count < DISCOVERY_MIN_EDITION_COUNT:
            skipped += 1
            continue

        author_names = doc.get("author_name") or []
        author = author_names[0] if author_names else None
        subjects = doc.get("subject", [])[:20] or (fallback_subjects or [])

        if _insert_discovered_book(
            cur, ol_key, doc.get("title", ""), author, doc.get("first_publish_year"),
            subjects, doc.get("ratings_average"), ratings_count,
            doc.get("number_of_pages_median"), doc.get("cover_i"), run_id,
        ):
            known_keys.add(ol_key)
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped

OPENLIB_DISCOVERY_MIN = 5

@op
def discover_openlibrary_books(context, start=None):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur = conn.cursor()

    subjects = get_top_read_subjects(cur)
    creators = get_top_read_creators(cur)
    if not subjects and not creators:
        log.warning("No top read subjects/creators in taste_profile yet — run build_taste_profile first.")
        cur.close()
        conn.close()
        return 0

    known_keys = get_known_ol_keys(cur)
    inserted = 0
    skipped = 0
    total_fetched = 0

    for subject_name in subjects:
        try:
            docs = fetch_subject_works(subject_name)
        except requests.exceptions.RequestException as e:
            log.warning(f"OpenLibrary subject search failed for '{subject_name}': {e}")
            continue
        total_fetched += len(docs)
        d_inserted, d_skipped = _process_discovery_docs(cur, docs, known_keys, run_id, fallback_subjects=[subject_name])
        inserted += d_inserted
        skipped += d_skipped

    # Author-driven discovery - books by authors this reader has already
    # rated highly (meta.taste_profile.top_creators_read), same "creator
    # match" signal movies/TV already use.
    for author_name in creators:
        try:
            docs = fetch_author_works(author_name)
        except requests.exceptions.RequestException as e:
            log.warning(f"OpenLibrary author search failed for '{author_name}': {e}")
            continue
        total_fetched += len(docs)
        d_inserted, d_skipped = _process_discovery_docs(cur, docs, known_keys, run_id)
        inserted += d_inserted
        skipped += d_skipped

    conn.commit()
    cur.close()
    conn.close()

    # Guards on total_fetched (raw search-API results), not `inserted` -
    # 0 newly-inserted books is a normal, common outcome (everything found
    # was already known/non-English/below the quality bar), not a failure.
    # 0 fetched across every subject/author search, on the other hand, means
    # OpenLibrary itself returned nothing at all - a real connectivity signal.
    if total_fetched == 0:
        raise Exception(
            "discover_openlibrary_books: zero rows ingested — API may be down or "
            "returning empty response. Check OpenLibrary connectivity."
        )
    if total_fetched < OPENLIB_DISCOVERY_MIN:
        log.warning(
            f"discover_openlibrary_books: only {total_fetched} rows — below expected "
            f"minimum of {OPENLIB_DISCOVERY_MIN}. Possible partial response from OpenLibrary."
        )

    log.info(f"OpenLibrary discovery: {inserted} new unread English books found, {skipped} skipped (already known, non-English, or below quality bar)")
    return inserted
