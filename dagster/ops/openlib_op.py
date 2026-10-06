import difflib
import os
import re
import unicodedata
import requests
import psycopg2
import psycopg2.extras
from typing import Optional
from pydantic import BaseModel, ValidationError
from dagster import op, get_dagster_logger

from ops import NETWORK_RETRY

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

AUTHOR_MATCH_RATIO = 0.85

def normalize_name(name: str) -> str:
    """Accent-free, lowercase, letters and spaces only: 'Elif Şafak' and
    'Elif Shafak' become 'elif safak' and 'elif shafak'."""
    decomposed = unicodedata.normalize("NFKD", name)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(re.sub(r"[^\w\s]", " ", stripped.lower()).split())

def author_matches(author: str, candidates: list) -> bool:
    """Substring either way ('J.K. Rowling' vs 'J. K. Rowling'), or close
    spelling for transliterated names ('Elif Shafak' vs 'Elif Şafak')."""
    target = normalize_name(author)
    for candidate in candidates:
        c = normalize_name(candidate)
        if not c:
            continue
        if target in c or c in target:
            return True
        if difflib.SequenceMatcher(None, target, c).ratio() >= AUTHOR_MATCH_RATIO:
            return True
    return False

def search_openlibrary(title: str, author: Optional[str]) -> Optional[dict]:
    """Title search first, then a free-text title + author search. The
    title search alone missed books Open Library does have: a translated
    title indexed under the original-script author name ('Before the Coffee
    Gets Cold' under 川口俊和), or a title the title index doesn't match
    at all ('What You Are Looking for Is in the Library')."""
    main_title = title.split(":")[0].strip()
    url = "https://openlibrary.org/search.json"
    fields = "key,title,author_name,first_publish_year,subject,ratings_average,ratings_count,number_of_pages_median,cover_i"
    searches = [{"title": main_title}]
    if author:
        searches.append({"q": f"{main_title} {author}"})

    for params in searches:
        r = requests.get(url, params={**params, "limit": 5, "fields": fields}, timeout=10)
        r.raise_for_status()
        docs = r.json().get("docs", [])
        if not author:
            return docs[0] if docs else None
        for doc in docs:
            if author_matches(author, doc.get("author_name", [])):
                return doc
    return None

# ── Op: enrich book metadata ────────────────────────────────────
# No row-count guard here on purpose: this only re-enriches books already
# in raw_book_ratings, one OpenLibrary search per title. Once the backlog
# is caught up, 0 newly-inserted/updated rows is the normal steady state
# (search matches found, but nothing about them changed), not a signal
# OpenLibrary is down - a zero-guard here would false-alarm constantly.

@op(retry_policy=NETWORK_RETRY)
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
                author              = EXCLUDED.author,
                first_publish_year = EXCLUDED.first_publish_year,
                subjects            = EXCLUDED.subjects,
                ratings_average     = EXCLUDED.ratings_average,
                ratings_count       = EXCLUDED.ratings_count,
                page_count          = EXCLUDED.page_count,
                cover_id            = EXCLUDED.cover_id,
                ingested_at         = CASE
                    WHEN raw_books.hardcover_id       IS DISTINCT FROM EXCLUDED.hardcover_id
                      OR raw_books.author             IS DISTINCT FROM EXCLUDED.author
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

# ── Op: discover new (unread) books via OpenLibrary search ───────
# enrich_book_metadata above only ever enriches books already in the
# user's own Hardcover library (raw.raw_book_ratings) - there was no
# mechanism anywhere in this pipeline for surfacing a book the user hasn't
# already added themselves, unlike movies/TV which get a real discover/
# popular/trending pool from TMDB independent of watch history. This op
# is that missing discovery source for books, using OpenLibrary's public
# search API (no auth, no key), seeded from the subjects and authors of
# the books the reader rated highest.

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
    title = english_title(doc) or doc.get("title") or ""
    return REQUIRED_BOOK_LANGUAGE in (doc.get("language") or []) and title.isascii()

def english_title(doc: dict) -> Optional[str]:
    """Title of the work's English edition, which search.json returns under
    editions when asked with lang=en. A work's own title is often the
    original-language one ('... Trotzdem Ja zum Leben sagen' for Man's
    Search for Meaning), which reads wrong on the picks page and slips past
    the "already read" title check."""
    for edition in (doc.get("editions") or {}).get("docs") or []:
        if REQUIRED_BOOK_LANGUAGE in (edition.get("language") or []) and edition.get("title"):
            return edition["title"]
    return None

DISCOVERY_LIKED_MIN_RATING = 8  # out of 10, i.e. 4+ stars on Hardcover

def get_top_read_subjects(cur, limit=8) -> list:
    """Specific subjects (never generic tags like 'Fiction') of the books
    rated 8+ out of 10, most common first, so discovery searches for books
    like the ones actually loved. meta.taste_profile's top_genres_read
    needs 5+ books per subject, which a few dozen books rarely reach,
    leaving only generic tags to search."""
    cur.execute("""
        SELECT s.subject_name
        FROM marts.fact_reading_history f
        JOIN marts.bridge_content_book_subject b ON b.content_id = f.content_id
        JOIN marts.dim_book_subject s ON s.subject_id = b.subject_id
        WHERE f.has_rating AND f.rating >= %s AND NOT s.is_generic
        GROUP BY s.subject_name
        ORDER BY count(*) DESC, avg(f.rating) DESC, s.subject_name
        LIMIT %s;
    """, (DISCOVERY_LIKED_MIN_RATING, limit))
    return [r[0] for r in cur.fetchall()]

def get_top_read_creators(cur, limit=10) -> list:
    """Authors of books rated 8+ out of 10, highest average first. One
    loved book is enough; the taste profile's creator list needs two."""
    cur.execute("""
        SELECT d.primary_creator
        FROM marts.fact_reading_history f
        JOIN marts.dim_book d ON d.content_id = f.content_id
        WHERE f.has_rating AND f.rating >= %s AND d.primary_creator IS NOT NULL
        GROUP BY d.primary_creator
        ORDER BY avg(f.rating) DESC, count(*) DESC, d.primary_creator
        LIMIT %s;
    """, (DISCOVERY_LIKED_MIN_RATING, limit))
    return [r[0] for r in cur.fetchall()]

def get_known_ol_keys(cur) -> set:
    """Keys of books in the reader's own library, never touched by
    discovery. Earlier discoveries are not in this set, so a later run can
    refresh their title (see _insert_discovered_book)."""
    cur.execute("SELECT ol_key FROM raw.raw_books WHERE hardcover_id IS NOT NULL;")
    return {r[0] for r in cur.fetchall()}

_SEARCH_FIELDS = (
    "key,title,author_name,first_publish_year,subject,language,"
    "ratings_average,ratings_count,number_of_pages_median,"
    "cover_i,edition_count,editions,editions.title,editions.language"
)

def fetch_subject_works(subject_name: str, limit: int = 20) -> list:
    """search.json's subject= facet (not the old /subjects/{slug}.json
    endpoint) - the old endpoint returns no per-work language data at all,
    making an English-only filter impossible there. search.json returns it,
    same shape as fetch_author_works below, so both discovery paths share
    one filtering/insertion pipeline."""
    url = "https://openlibrary.org/search.json"
    params = {"subject": subject_name, "limit": limit, "fields": _SEARCH_FIELDS, "lang": "en"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("docs", [])

def fetch_author_works(author_name: str, limit: int = 20) -> list:
    """Unlike the old subjects endpoint, search.json returns real
    ratings_average/ratings_count per book - an author-driven discovery
    candidate gets a genuine quality signal, not just edition_count."""
    url = "https://openlibrary.org/search.json"
    params = {"author": author_name, "limit": limit, "fields": _SEARCH_FIELDS, "lang": "en"}
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
        ON CONFLICT (ol_key) DO UPDATE SET
            title = EXCLUDED.title,
            ingested_at = NOW(),
            pipeline_run_id = EXCLUDED.pipeline_run_id
        WHERE raw_books.hardcover_id IS NULL
          AND raw_books.title IS DISTINCT FROM EXCLUDED.title;
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
            cur, ol_key, english_title(doc) or doc.get("title", ""), author, doc.get("first_publish_year"),
            subjects, doc.get("ratings_average"), ratings_count,
            doc.get("number_of_pages_median"), doc.get("cover_i"), run_id,
        ):
            known_keys.add(ol_key)
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped

OPENLIB_DISCOVERY_MIN = 5

@op(retry_policy=NETWORK_RETRY)
def discover_openlibrary_books(context, start=None):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur = conn.cursor()

    subjects = get_top_read_subjects(cur)
    creators = get_top_read_creators(cur)
    if not subjects and not creators:
        log.warning("No highly rated books in marts.fact_reading_history yet; nothing to base discovery on.")
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
    # rated highly (get_top_read_creators above), same "creator
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

    log.info(f"OpenLibrary discovery: {inserted} unread English books added or retitled, {skipped} skipped (unchanged, non-English, or below quality bar)")
    return inserted
