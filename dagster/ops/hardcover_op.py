import os
import requests
import psycopg2
from datetime import date
from typing import Optional
from pydantic import BaseModel, ValidationError
from dagster import op, get_dagster_logger

# ── Pydantic model ───────────────────────────────────────────

class BookRating(BaseModel):
    hardcover_id: str
    title: str
    author: Optional[str] = None
    isbn_13: Optional[str] = None
    isbn_10: Optional[str] = None
    rating: Optional[float] = None
    status: Optional[str] = None
    finished_at: Optional[date] = None

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

def get_isbns(book: dict) -> tuple[Optional[str], Optional[str]]:
    editions = book.get("editions", [])
    isbn_13 = next((e["isbn_13"] for e in editions if e.get("isbn_13")), None)
    isbn_10 = next((e["isbn_10"] for e in editions if e.get("isbn_10")), None)
    return isbn_13, isbn_10

def fetch_hardcover(query: str) -> dict:
    token = os.getenv("HARDCOVER_API_TOKEN")
    url = "https://api.hardcover.app/v1/graphql"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json={"query": query}, timeout=10)
    r.raise_for_status()
    return r.json()

# ── Op: ingest book ratings ────────────────────────────────────
# Every library entry, rated or not. A rated-only filter used to drop books
# marked read but never rated (2 of 29 in this library) - the pipeline then
# didn't know they'd been read and could recommend them back. Unrated rows
# carry rating NULL; downstream taste signals already filter on has_rating.

HARDCOVER_QUERY = """
{
  me {
    user_books {
      id
      rating
      status_id
      user_book_reads {
        finished_at
      }
      book {
        title
        contributions {
          author {
            name
          }
        }
        editions {
          isbn_10
          isbn_13
        }
      }
    }
  }
}
"""

HARDCOVER_BOOKS_MIN = 1

@op
def ingest_hardcover_books(context):
    log = get_dagster_logger()
    run_id = context.run_id
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    data = fetch_hardcover(HARDCOVER_QUERY)
    # GraphQL errors (e.g. a schema change on Hardcover's side) come back as
    # HTTP 200 with "errors" and no "data", which used to surface as an
    # unhelpful AttributeError on None.
    if data.get("errors"):
        raise Exception(f"ingest_hardcover_books: GraphQL error from Hardcover: {data['errors']}")
    me = (data.get("data") or {}).get("me") or [{}]
    user_books = me[0].get("user_books", [])
    total_fetched = len(user_books)

    for b in user_books:
        try:
            book = b.get("book", {})
            contributions = book.get("contributions", [])
            author = contributions[0]["author"]["name"] if contributions else None
            isbn_13, isbn_10 = get_isbns(book)

            reads = b.get("user_book_reads", [])
            finish_dates = [r["finished_at"] for r in reads if r.get("finished_at")]
            finished_at = max(finish_dates) if finish_dates else None

            rating = BookRating(
                hardcover_id=str(b["id"]),
                title=book.get("title", ""),
                author=author,
                isbn_13=isbn_13,
                isbn_10=isbn_10,
                rating=b.get("rating"),
                status=str(b.get("status_id")),
                finished_at=finished_at,
            )
        except (ValidationError, KeyError, IndexError) as e:
            log.warning(f"Validation failed for book {b.get('id')}: {e}")
            skipped += 1
            continue

        cur.execute("""
            INSERT INTO raw.raw_book_ratings (
                hardcover_id, title, author, isbn_13, isbn_10, rating, status, finished_at, pipeline_run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (hardcover_id) DO UPDATE SET
                isbn_13     = EXCLUDED.isbn_13,
                isbn_10     = EXCLUDED.isbn_10,
                rating      = EXCLUDED.rating,
                status      = EXCLUDED.status,
                finished_at = EXCLUDED.finished_at,
                ingested_at = CASE
                    WHEN raw_book_ratings.isbn_13     IS DISTINCT FROM EXCLUDED.isbn_13
                      OR raw_book_ratings.isbn_10     IS DISTINCT FROM EXCLUDED.isbn_10
                      OR raw_book_ratings.rating      IS DISTINCT FROM EXCLUDED.rating
                      OR raw_book_ratings.status      IS DISTINCT FROM EXCLUDED.status
                      OR raw_book_ratings.finished_at IS DISTINCT FROM EXCLUDED.finished_at
                    THEN NOW()
                    ELSE raw_book_ratings.ingested_at
                END,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """, (
            rating.hardcover_id,
            rating.title,
            rating.author,
            rating.isbn_13,
            rating.isbn_10,
            rating.rating,
            rating.status,
            rating.finished_at,
            run_id,
        ))
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cur.close()
    conn.close()

    # Same reasoning as simkl_op - raw_book_ratings' ingested_at only bumps
    # on an actual change (see the UPSERT's CASE above), so this checks the
    # API response size, not DB write activity. 0 means the GraphQL query
    # returned no library books at all - a real failure signal (auth/endpoint),
    # since the library is never empty.
    if total_fetched == 0:
        raise Exception(
            "ingest_hardcover_books: zero rows ingested — API may be down or "
            "returning empty response. Check Hardcover connectivity."
        )
    if total_fetched < HARDCOVER_BOOKS_MIN:
        log.warning(
            f"ingest_hardcover_books: only {total_fetched} rows — below expected "
            f"minimum of {HARDCOVER_BOOKS_MIN}. Possible partial response from Hardcover."
        )

    log.info(f"Hardcover books: {inserted} inserted/updated, {skipped} skipped")
    return inserted
