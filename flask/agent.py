import os
import re
import logging
from decimal import Decimal

import psycopg2
from langchain_community.utilities import SQLDatabase
from langchain_ollama import ChatOllama

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("agent")

OLLAMA_MODEL = os.getenv("OLLAMA_ASK_MODEL", "llama3.2:3b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
MAX_SQL_ATTEMPTS = 2
MAX_ROWS_TO_MODEL = 30

INCLUDE_TABLES = [
    "dim_watchable", "fact_watch_history", "dim_genre",
    "bridge_content_genre", "dim_platform", "bridge_content_platform",
    "dim_book", "fact_reading_history", "dim_book_subject", "bridge_content_book_subject",
    "taste_profile", "daily_recommendations_watch", "daily_recommendations_books", "user_config",
]

# Two autonomous designs were tried and both failed at this model size (3B,
# CPU-only, no paid API): a plain generate-then-validate single-shot text-to-SQL
# path (unreliable SQL) and a LangChain create_sql_agent
# tool-calling loop (llama3.2:3b answered with zero tool calls,
# fully fabricated titles never in this account's watch history - same failure
# mode qwen2.5-coder:3b had before it). The fix isn't a bigger model, it's the
# same recipe a real local-3B agentic project (github.com/itsbhoomika/
# pipeline-optimizer) used to make a 3B model reliable: schema grounding, a
# worked example to imitate, and best-of-N against a FREE verifier (a real
# Postgres query costs nothing to try, unlike their BigQuery dry-runs) -
# never trust one shot, cheaply verify every candidate before using it.

# Condensed 2026-08-27 to cut prompt-processing latency on CPU-only Ollama
# (this rules block was ~55% of a ~2870-token prompt, and prefill on this
# hardware runs as low as ~20 tok/s under host CPU contention). Every
# constraint below is kept; only the prose explaining WHY
# each rule exists was cut - that reasoning lives in git history, not needed
# by the model at inference time.
SCHEMA_RULES = """SQL RULES:
- Only filter on what the question actually asks — don't copy a date range,
  rating check, or language code from the examples onto an unrelated question.
- Always schema-qualify tables: marts.dim_watchable, meta.taste_profile, etc.
- Use ILIKE with wildcards for text/title search.
- LIMIT 10 by default unless a specific number is asked for. EXCEPTION: for
  a "how many"/count question, do NOT write COUNT() and do NOT add a LIMIT —
  return every matching row (id or title column only); the app counts rows
  in Python.
- Ranking by AVERAGE requires a HAVING COUNT(*) floor so one-off ratings
  can't win uncontested: >= 5 for movie/TV genre, >= 2 for book subject and
  for creator (same floors as meta.taste_profile; a few dozen books and
  directors rarely reach 5).
- Book subjects: always filter NOT dbs.is_generic. Generic tags ('Fiction',
  'New York Times bestseller') are on most books and say nothing about them.
- marts.dim_genre holds only movie/TV genres. Book subject tags live in
  marts.dim_book_subject / marts.bridge_content_book_subject (see the Books
  rule below).
- dim_genre.genre_name is stored Title Case ('Comedy', 'Science Fiction').
  ALWAYS compare it with ILIKE, never =, e.g. dg.genre_name ILIKE 'comedy' —
  an exact = match against a guessed case silently returns zero rows.
- "Haven't watched"/"unwatched": LEFT JOIN marts.dim_watchable to
  marts.fact_watch_history ON content_id, WHERE fact_watch_history.content_id IS NULL.
- CRITICAL: "x.col IS NULL" on a joined table only makes sense after LEFT/
  RIGHT/FULL JOIN to that table — a plain (inner) JOIN paired with IS NULL is
  a contradiction that always returns zero rows. Double-check the join type
  matches every time you write an IS NULL condition on a joined column.
- "Watched"/"have I watched"/"read"/"did I watch": fact_watch_history has rows
  that are NOT actually watched yet — consumption_status is 'consumed'
  (really finished), 'in_progress' (currently watching), or 'planned'
  (wishlist, never started). Any "watched"/"have I watched" style question
  MUST add fact_watch_history.consumption_status = 'consumed' — otherwise
  wishlist and still-watching items get counted as if they were finished.
  No rating condition beyond that unless the question explicitly asks about
  rated vs unrated items.
- Genre filters go through marts.bridge_content_genre -> marts.dim_genre (no
  genre column on dim_watchable). Platform filters go through
  marts.bridge_content_platform -> marts.dim_platform (no platform column on
  dim_watchable).
- For platform queries always use ILIKE wildcard, never an exact = match.
  Platform names in the database are TMDB's full official names, which
  differ from the colloquial names people actually type.
  Always write: WHERE dp.platform_name ILIKE '%term%'
  Never write:  WHERE dp.platform_name = 'Netflix'
  Common mappings — user says -> DB contains:
    Netflix -> Netflix (exact match happens to work, but use ILIKE anyway)
    Prime / Amazon Prime -> Amazon Prime Video
    Apple TV / Apple -> Apple TV
    Max / HBO Max -> HBO Max
    Hulu -> Hulu
    Disney Plus / Disney -> Disney Plus
    Peacock -> Peacock
- fact_watch_history.rating is the user's OWN rating; dim_watchable.vote_average is
  the TMDB community rating — never confuse the two.
- dim_watchable.original_language stores ISO 639-1 codes ('en', 'hi', 'mr',
  ...), not language names — always translate a language name to its code.
- For "what should I watch"/"what's on [platform]"/discovery questions about
  unwatched content, always add dc.vote_count >= 10 to exclude near-unvoted
  catalog entries.
- Date math: NOW() - INTERVAL '7 days', not application-level date logic. A
  calendar month name ("in August") is NEVER the rolling-window pattern —
  it means EXTRACT(MONTH FROM fw.interaction_date) = <month number>, no year
  restriction unless a year is explicitly stated.
- Taste profile: SELECT ... FROM meta.taste_profile ORDER BY generated_at
  DESC LIMIT 1 — always the latest row.
- Books are a separate domain, not a content_type value: marts.fact_reading_history
  and marts.dim_book have NO content_type column (every row in them is a book by
  definition) — never write content_type = 'book'. Book "genre" is a subject tag
  via marts.bridge_content_book_subject -> marts.dim_book_subject
  (subject_name), NOT marts.dim_genre (movie/TV genres only).
  dim_book.primary_creator is the author.
- Output ONLY the SQL query. No markdown fences, no explanation, no comments.
  End with a semicolon. Exactly one SELECT (or WITH ... SELECT) statement."""

WORKED_EXAMPLE = """Question: "What did I watch last week?"
SQL:
SELECT dc.title, dc.content_type, fw.rating, fw.interaction_date
FROM marts.fact_watch_history fw
JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
WHERE fw.interaction_date >= NOW() - INTERVAL '7 days'
  AND fw.consumption_status = 'consumed'
ORDER BY fw.interaction_date DESC
LIMIT 10;

Question: "Hindi movies I haven't watched yet"
SQL:
SELECT dc.title, dc.content_type
FROM marts.dim_watchable dc
LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id
WHERE dc.original_language = 'hi'
  AND dc.content_type = 'movie'
  AND fw.content_id IS NULL
LIMIT 10;

Question: "How many movies have I watched?"
SQL:
SELECT dc.content_id
FROM marts.fact_watch_history fw
JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
WHERE fw.content_type = 'movie'
  AND fw.consumption_status = 'consumed';

Question: "How many movies did I watch in August?"
SQL:
SELECT dc.content_id
FROM marts.fact_watch_history fw
JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
WHERE fw.content_type = 'movie'
  AND fw.consumption_status = 'consumed'
  AND EXTRACT(MONTH FROM fw.interaction_date) = 8;

Question: "What is my highest rated genre?"
SQL:
SELECT dg.genre_name, AVG(fw.rating) AS avg_rating, COUNT(*) AS n
FROM marts.fact_watch_history fw
JOIN marts.bridge_content_genre bcg ON bcg.content_id = fw.content_id
JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
WHERE fw.content_type IN ('movie', 'tv')
  AND fw.consumption_status = 'consumed'
GROUP BY dg.genre_name
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC
LIMIT 10;

Question: "Hindi thriller I haven't seen"
SQL:
SELECT dc.title, dc.content_type
FROM marts.dim_watchable dc
JOIN marts.bridge_content_genre bcg ON bcg.content_id = dc.content_id
JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id
WHERE dc.original_language = 'hi'
  AND dg.genre_name ILIKE 'Thriller'
  AND dc.vote_count >= 10
  AND fw.content_id IS NULL
LIMIT 10;

Question: "What's on Netflix that I haven't watched?"
SQL:
SELECT dc.title, dc.content_type
FROM marts.dim_watchable dc
JOIN marts.bridge_content_platform bcp ON bcp.content_id = dc.content_id
JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id
LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id
WHERE dp.platform_name ILIKE '%netflix%'
  AND dc.vote_count >= 10
  AND fw.content_id IS NULL
LIMIT 10;

Question: "What genre do I watch the most but rate the lowest?"
SQL:
SELECT dg.genre_name, COUNT(*) AS n, AVG(fw.rating) AS avg_rating
FROM marts.fact_watch_history fw
JOIN marts.bridge_content_genre bcg ON bcg.content_id = fw.content_id
JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
WHERE fw.content_type IN ('movie', 'tv')
  AND fw.consumption_status = 'consumed'
GROUP BY dg.genre_name
HAVING COUNT(*) >= 5
ORDER BY n DESC
LIMIT 10;
-- "most" is the actual ranking criterion (ORDER BY the count), not rating —
-- rating is only reported alongside for context, never used to sort here.

Question: "How does my average movie rating compare to my average TV rating?"
SQL:
SELECT fw.content_type, AVG(fw.rating) AS avg_rating, COUNT(*) AS n
FROM marts.fact_watch_history fw
WHERE fw.content_type IN ('movie', 'tv')
  AND fw.consumption_status = 'consumed'
GROUP BY fw.content_type
ORDER BY fw.content_type;
-- A "compare A to B" question is not a ranking (no ORDER BY ... DESC LIMIT
-- 1) — group by the thing being compared and return every group's row so
-- both sides of the comparison are in the result, not just a single winner.

Question: "What books have I read?"
SQL:
SELECT db.title, fw.rating
FROM marts.fact_reading_history fw
JOIN marts.dim_book db ON db.content_id = fw.content_id
WHERE fw.consumption_status = 'consumed'
ORDER BY fw.interaction_date DESC
LIMIT 10;

Question: "What is my highest rated book subject?"
SQL:
SELECT dbs.subject_name, AVG(fw.rating) AS avg_rating, COUNT(*) AS n
FROM marts.fact_reading_history fw
JOIN marts.bridge_content_book_subject bcbs ON bcbs.content_id = fw.content_id
JOIN marts.dim_book_subject dbs ON dbs.subject_id = bcbs.subject_id
WHERE fw.consumption_status = 'consumed'
  AND NOT dbs.is_generic
GROUP BY dbs.subject_name
HAVING COUNT(*) >= 2
ORDER BY avg_rating DESC
LIMIT 10;
-- No content_type filter — fact_reading_history only ever holds books."""

ANSWER_STYLE = """Tone: texting a friend, not writing a report. 1-2 short sentences
max — state the fact and stop. Use "you" (never "the user"), contractions
("you've", "don't"), no bullet points or headers.
BANNED: any commentary, editorializing, or filler that isn't a fact from the
data — no "it's clear you...", no "you have a strong affection for...", no
"that's pretty solid", no explaining what the number means or how the user
should feel about it. State the number/title and stop.
Never wrap a title in quotation marks — write Friends, not "Friends".
Never invent a unit that isn't a column in the data (no "episodes" unless a
column literally says episodes).
Plain text only — no markdown (no **bold**, no _italics_, no backticks), no
special unicode punctuation (use a plain hyphen and straight quotes, not
curly quotes or narrow no-break spaces).
Never invent a title, rating, or fact that isn't in the data given to you.
If the data is empty, say plainly that nothing matched — never fill the gap
with outside knowledge about movies, shows, or books.
If the question asks about an attribute (genre, platform, language, etc.)
that isn't one of the columns in the data below, do not guess or classify
items yourself (e.g. never say a title "could be classified as" a genre
that wasn't in the query result) — only state what the columns actually
show.

Examples of the tone wanted:
"Drama's your most-watched genre — 69 watched, 7.56 avg rating."
"Yep, 3 unwatched Marathi movies: Aarpar, The Lane, Zombivli."
"Nothing matched — no unwatched horror movies on your list."
NOT wanted: "It's clear you have a strong affection for this genre, even
though the ratings aren't always glowing, which shows a real dedication.\""""

_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|CALL)\b",
    re.IGNORECASE,
)

def _build_db():
    return SQLDatabase.from_uri(
        os.getenv("ASK_DATABASE_URL"),
        include_tables=INCLUDE_TABLES,
        sample_rows_in_table_info=1,
    )

def _ask_conn():
    dsn = os.getenv("ASK_DATABASE_URL").replace("postgresql+psycopg2://", "postgresql://")
    return psycopg2.connect(dsn)

# Built on first use rather than at import: the schema introspection needs a
# live database, and importing this module (e.g. from unit tests of the SQL
# guards and repairs below) shouldn't.
_schema_info_cache = None
_answer_llm_cache = None

def _schema_info() -> str:
    global _schema_info_cache
    if _schema_info_cache is None:
        _schema_info_cache = _build_db().get_table_info(INCLUDE_TABLES)
    return _schema_info_cache

def _answer_llm():
    global _answer_llm_cache
    if _answer_llm_cache is None:
        _answer_llm_cache = ChatOllama(model=OLLAMA_MODEL, temperature=0.0, base_url=OLLAMA_BASE_URL)
    return _answer_llm_cache

log.info(f"[agent] backend: ollama ({OLLAMA_MODEL}), best-of-{MAX_SQL_ATTEMPTS} SQL generation")

def _extract_sql(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("sql"):
            text = text[3:]
    return text.strip().rstrip(";").strip()

def _is_safe_select(sql: str) -> bool:
    if not sql:
        return False
    if _FORBIDDEN_KEYWORDS.search(sql):
        return False
    if sql.count(";") > 0:
        return False  # only a trailing one was allowed, already stripped
    first_word = sql.strip().split(None, 1)[0].upper()
    return first_word in ("SELECT", "WITH")

_CONTENT_ID_IS_NULL = re.compile(r"content_id\s+IS\s+NULL", re.IGNORECASE)

def _has_impossible_null_check(sql: str) -> bool:
    """content_id is never a nullable column on its own - it only reads as
    NULL after a LEFT/RIGHT/FULL JOIN failed to match. The model
    twice wrote a plain (inner) JOIN paired with 'content_id IS NULL',
    a self-contradiction that always silently returns zero rows regardless
    of real data - worse, best-of-N then fell through to a later attempt
    that answered a completely different question (watched, not unwatched)
    using real data, which read as a confident correct answer despite being
    the wrong semantic set entirely. Reject this shape before even running
    it, rather than let it burn an attempt on a guaranteed-empty query."""
    if not _CONTENT_ID_IS_NULL.search(sql):
        return False
    upper = sql.upper()
    return not any(j in upper for j in ("LEFT JOIN", "RIGHT JOIN", "FULL JOIN"))

_MONTH_NAMES = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b",
    re.IGNORECASE,
)

def _has_wrong_date_window(sql: str, question: str) -> bool:
    """Seen twice, even after adding both a rule and a matching
    worked example: the model keeps reusing the 'last 7 days' rolling-window
    pattern (from the 'what did I watch last week' example) for a question
    that names a specific calendar month instead - 'how many movies did I
    watch in August' returned 0 three separate times because the SQL was
    silently checking the last 7 days, not August at all. Prompt-only fixes
    didn't stick, so this rejects the mismatched shape outright: if the
    question names a month and the SQL has no month-based date filter, the
    rolling-window pattern is almost certainly present by mistake."""
    if not _MONTH_NAMES.search(question):
        return False
    upper = sql.upper()
    has_month_filter = "EXTRACT(MONTH" in upper or "DATE_TRUNC" in upper
    return not has_month_filter

_CONTENT_TYPE_WORDS = {
    "movie": re.compile(r"\bmovies?\b", re.IGNORECASE),
    # Bug: "movies on Apple TV+" matched \btv\b against the
    # platform name itself, not a real content-type mention - the guard then
    # rejected every attempt for lacking a content_type='tv' filter nobody
    # actually asked for. Negative lookbehind excludes "Apple TV" specifically
    # (the one platform name observed to collide), not "tv" in general.
    "tv": re.compile(r"(?<!apple )\btv\b|\bseries\b|\bshows?\b", re.IGNORECASE),
    "book": re.compile(r"\bbooks?\b|\bread\b", re.IGNORECASE),
}

_LANGUAGE_NAME_WORDS = re.compile(r"\bhindi\b|\benglish\b|\bmarathi\b", re.IGNORECASE)

_LANGUAGE_VALUE = r"(?:=\s*'[^']*'|IN\s*\([^)]*\))"
# The filter can be the FIRST WHERE condition (no leading AND, but a
# trailing AND before the next condition) or a later one (leading AND, no
# trailing AND needed). The model puts it in either
# position depending on the question, so both must be handled or the
# leftover becomes invalid SQL ("WHERE AND ..." or a dangling AND).
_LANGUAGE_FILTER_LEADING = re.compile(
    rf"\bWHERE\s+\w+\.original_language\s+{_LANGUAGE_VALUE}\s*\bAND\b", re.IGNORECASE
)
_LANGUAGE_FILTER_TRAILING = re.compile(
    rf"\s*\bAND\s+\w+\.original_language\s+{_LANGUAGE_VALUE}", re.IGNORECASE
)

def _repair_unrequested_language_filter(sql: str, question: str) -> str:
    """Even with a targeted reminder telling the model not
    to, it keeps copying original_language from the nearest worked example
    onto questions that never named a language - and rejecting those
    candidates outright (via _has_unrequested_language_filter below) just
    burned every attempt with the same mistake, going from 'wrong answer'
    to 'no answer at all'. Same lesson as _repair_missing_genre_join: a
    purely mechanical, deterministic defect is cheaper to patch in code
    than to keep spending retries hoping the model self-corrects."""
    if _LANGUAGE_NAME_WORDS.search(question):
        return sql
    sql = _LANGUAGE_FILTER_LEADING.sub("WHERE", sql)
    sql = _LANGUAGE_FILTER_TRAILING.sub("", sql)
    return sql

def _has_unrequested_language_filter(sql: str, question: str) -> bool:
    """Bug: 'What horror movies haven't I seen?' - the model
    copied original_language = 'hi' from the 'Hindi thriller' worked example
    even though the question never named a language, silently narrowing
    results to Hindi-only. SCHEMA_RULES' first rule already says not to copy
    unmentioned filters from the examples; the model doesn't reliably obey
    it. Unlike the missing-filter guards above, this is the one confirmed
    'added something nobody asked for' case - narrowly scoped to language
    specifically since that's the only bleed pattern actually observed."""
    if _LANGUAGE_NAME_WORDS.search(question):
        return False
    return "ORIGINAL_LANGUAGE" in sql.upper()

def _has_missing_content_type_filter(sql: str, question: str) -> bool:
    """Bug: 'how many movies did I watch in August' - the model
    fixed the date-window bug (EXTRACT(MONTH...) present) but silently
    dropped content_type = 'movie' entirely, using bare COUNT(dc.content_id)
    over EVERY row in that month (movies, tv, books alike) - 102 instead of
    the real 68. The worked example for this exact question (SCHEMA_RULES /
    WORKED_EXAMPLE) already includes the content_type filter, but the model
    doesn't reliably keep it once it's also juggling a date filter. If the
    question names one specific content type and the SQL has no
    content_type filter naming it at all, reject outright rather than trust
    a count/list that silently spans all three types."""
    upper = sql.upper()
    for kind, pattern in _CONTENT_TYPE_WORDS.items():
        if kind == "book":
            continue  # books have no content_type column - fact_reading_history/dim_book ARE the filter
        if pattern.search(question) and "CONTENT_TYPE" in upper:
            if f"'{kind.upper()}'" not in upper:
                return True
        elif pattern.search(question) and "CONTENT_TYPE" not in upper:
            return True
    return False

_GENRE_WORDS = {
    "comedy": re.compile(r"\bcomed(y|ies)\b", re.IGNORECASE),
    "horror": re.compile(r"\bhorror\b", re.IGNORECASE),
    "thriller": re.compile(r"\bthrillers?\b", re.IGNORECASE),
    "romance": re.compile(r"\bromance\b|\bromantic\b", re.IGNORECASE),
    "action": re.compile(r"\baction\b", re.IGNORECASE),
    "drama": re.compile(r"\bdramas?\b", re.IGNORECASE),
    "documentary": re.compile(r"\bdocumentar(y|ies)\b", re.IGNORECASE),
    "animation": re.compile(r"\banimat(ion|ed)\b", re.IGNORECASE),
    "fantasy": re.compile(r"\bfantasy\b", re.IGNORECASE),
    "crime": re.compile(r"\bcrime\b", re.IGNORECASE),
    "mystery": re.compile(r"\bmyster(y|ies)\b", re.IGNORECASE),
    "history": re.compile(r"\bhistor(y|ical)\b", re.IGNORECASE),
    "war": re.compile(r"\bwar\b", re.IGNORECASE),
    "western": re.compile(r"\bwestern\b", re.IGNORECASE),
    "science fiction": re.compile(r"\bsci-?fi\b|\bscience fiction\b", re.IGNORECASE),
    "family": re.compile(r"\bfamily\b", re.IGNORECASE),
    "music": re.compile(r"\bmusical?\b", re.IGNORECASE),
}

def _has_missing_genre_filter(sql: str, question: str) -> bool:
    """Bug: 'Hindi comedies I haven't seen on Netflix' - the model
    dropped the genre filter entirely (no join to bridge_content_genre/
    dim_genre at all), so the platform+language+unwatched filters matched
    every Hindi movie on Netflix regardless of genre - action titles (War 2,
    Border 2) came back labeled as comedy suggestions. Same shape as the
    content_type bug above: if the question names a specific genre and the
    SQL has no dim_genre reference naming it, reject outright. Skipped for
    book questions - books use dim_book_subject, not dim_genre, and casual
    genre words ("fantasy") don't map cleanly onto real subject tags."""
    if _CONTENT_TYPE_WORDS["book"].search(question):
        return False
    upper = sql.upper()
    for name, pattern in _GENRE_WORDS.items():
        if not pattern.search(question):
            continue
        if "DIM_GENRE" not in upper and "GENRE_NAME" not in upper:
            return True
        key = "SCI" if name == "science fiction" else name.upper()
        if key not in upper:
            return True
    return False

_COMPOUND_RANKING = re.compile(r"\b(most|least)\b.*\brate\w*\b", re.IGNORECASE)
_COMPARISON_WORDS = re.compile(r"\bcompare[sd]?\b|\bcompared to\b|\bversus\b|\bvs\.?\b", re.IGNORECASE)
_WATCHED_PHRASE = re.compile(r"\bwatched\b|\bdid i (watch|read)\b|\bhave i (watched|read)\b|\bread\b", re.IGNORECASE)
_UNWATCHED_PHRASE = re.compile(r"haven.t|unwatched|not watched|not seen|n.t seen", re.IGNORECASE)

def _has_incomplete_comparison_filter(sql: str, question: str) -> bool:
    """Bug: 'How does my average movie rating compare to my
    average TV rating?' - the model wrote WHERE fw.content_type = 'movie'
    only (a single equality), silently dropping TV entirely - the query
    correctly ran and returned real movie data, so nothing else here caught
    it, but the answer had no TV row to compare against at all. If the
    question is a comparison AND names two or more content types, every one
    of those types' literal must appear in the SQL, or the candidate is
    thrown out rather than trusted with half the comparison missing."""
    if not _COMPARISON_WORDS.search(question):
        return False
    matched = [kind for kind, pattern in _CONTENT_TYPE_WORDS.items() if pattern.search(question)]
    if len(matched) < 2:
        return False
    upper = sql.upper()
    return not all(f"'{kind.upper()}'" in upper for kind in matched)

def _has_missing_consumed_filter(sql: str, question: str) -> bool:
    """fact_watch_history rows aren't all actually watched - consumption_status
    is 'consumed' (finished), 'in_progress' (currently watching), or
    'planned' (wishlist, never started). Spotted in Metabase: 'What
    You Watch On' and every other 'watched' stat was silently counting all
    three, inflating platform/genre/type counts with wishlist and
    still-watching rows. Same fix here: if the question asks what was
    watched/read and the SQL has no consumption_status filter at all,
    reject the candidate rather than trust an inflated count."""
    if not _WATCHED_PHRASE.search(question) or _UNWATCHED_PHRASE.search(question):
        return False
    return "CONSUMPTION_STATUS" not in sql.upper()

def _generate_sql(question: str, attempt: int, tokens: list) -> str | None:
    # The "how many movies did I watch in August" worked
    # example already sits in WORKED_EXAMPLE, but buried mid-block with the
    # Netflix example last - the 3B model attends most to whatever sits
    # closest to the "SQL:" cue and kept reusing the rolling-window pattern
    # anyway (3/3 attempts, every time, all rejected by
    # _has_wrong_date_window). Restating the rule at the highest-recency
    # position, right next to the question itself, fixes what a buried
    # worked example alone did not.
    # Same recency-attention problem showed up a second way: once the model
    # gets the month filter right, it separately drops content_type = 'movie'
    # (or 'tv'/'book') entirely, silently counting/listing all three types
    # together (102 vs the real 68 movies-only for "how many
    # movies did I watch in August"). Both nudges stack in the same reminder
    # block since a question can trigger either or both.
    reminder_lines = []
    if _MONTH_NAMES.search(question):
        reminder_lines.append(
            "This question names a calendar month. You MUST filter with "
            "EXTRACT(MONTH FROM fw.interaction_date) = <month number>. Do NOT "
            "use NOW() - INTERVAL '7 days' or any other rolling-window pattern."
        )
    for kind, pattern in _CONTENT_TYPE_WORDS.items():
        if not pattern.search(question):
            continue
        if kind == "book":
            reminder_lines.append(
                "This question is about books. Use marts.fact_reading_history and "
                "marts.dim_book — these tables ONLY ever contain books, so there is "
                "no content_type column and no content_type filter to add."
            )
        else:
            reminder_lines.append(
                f"This question is specifically about {kind}s. You MUST include "
                f"a content_type = '{kind}' filter — do not count or list other "
                f"content types alongside it."
            )
        break
    for genre, pattern in _GENRE_WORDS.items():
        if pattern.search(question):
            language_note = ""
            if not _LANGUAGE_NAME_WORDS.search(question):
                language_note = (
                    " This question does NOT name a language — do NOT add "
                    "original_language just because the closest worked "
                    "example above happens to have one."
                )
            reminder_lines.append(
                f"This question names the genre '{genre}'. You MUST add both "
                f"of these two lines, not just one: "
                f"'JOIN marts.bridge_content_genre bcg ON bcg.content_id = "
                f"dc.content_id' AND 'JOIN marts.dim_genre dg ON dg.genre_id "
                f"= bcg.genre_id', THEN filter with dg.genre_name ILIKE "
                f"'{genre}' (ILIKE, never =). A WHERE condition on "
                f"dg.genre_name with no JOIN that defines dg is invalid SQL "
                f"and will fail.{language_note}"
            )
            break
    if _WATCHED_PHRASE.search(question) and not _UNWATCHED_PHRASE.search(question):
        reminder_lines.append(
            "This question asks what you've WATCHED/READ (finished), not "
            "what's on your wishlist or still in progress. You MUST add "
            "fw.consumption_status = 'consumed' — presence in fact_watch_history "
            "alone is NOT enough, it also has 'planned' and 'in_progress' rows."
        )
    if _COMPOUND_RANKING.search(question):
        reminder_lines.append(
            "This question ranks by TWO things at once (e.g. most watched "
            "but lowest rated). Sort ONLY by the most/least (COUNT) side as "
            "the real ranking key — ORDER BY n DESC (or ASC for least). "
            "Include AVG(rating) as a second selected column for context, "
            "but do NOT sort by it."
        )
    elif _COMPARISON_WORDS.search(question):
        matched_types = [k for k, p in _CONTENT_TYPE_WORDS.items() if p.search(question)]
        type_note = ""
        if len(matched_types) >= 2:
            in_list = ", ".join(f"'{k}'" for k in matched_types)
            type_note = (
                f" This compares {' and '.join(matched_types)} — the WHERE "
                f"clause MUST be content_type IN ({in_list}), never a single "
                f"content_type = '...' equality, or one side goes missing."
            )
        reminder_lines.append(
            "This is a 'compare A to B' question, not a ranking. Do NOT use "
            "ORDER BY ... LIMIT 1 to pick a single winner — GROUP BY the "
            "thing being compared and return one row per group so every "
            "side of the comparison is in the result." + type_note
        )
    reminder = ("\nReminder:\n" + "\n".join(reminder_lines) + "\n") if reminder_lines else ""
    prompt = f"""You write PostgreSQL queries against this schema:

{_schema_info()}

{SCHEMA_RULES}

{WORKED_EXAMPLE}
{reminder}
Question: "{question}"
SQL:"""
    # num_predict caps generation length - a SQL query is normally under 150
    # tokens, an uncapped call risks the model rambling past the query with
    # commentary and just burning time for nothing usable.
    llm = ChatOllama(
        model=OLLAMA_MODEL, temperature=0.4, base_url=OLLAMA_BASE_URL,
        seed=42 + attempt, num_predict=180,
    )
    response = llm.invoke(prompt)
    usage = getattr(response, "usage_metadata", None) or {}
    tokens.append((usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0))
    return _extract_sql(response.content)

def _resolve_count(columns: list, rows: list) -> int:
    """The model is told to avoid writing COUNT() and return raw matching rows
    instead (so Python can count them), but it doesn't always follow that -
    sometimes it writes a working COUNT() anyway. Handle both shapes: a
    single row with a single numeric column IS the model's own aggregate
    (trust it, it's the only thing the query computed); anything else is a
    raw per-match row set, so the real count is simply how many rows came
    back."""
    if len(rows) == 1 and len(columns) == 1 and isinstance(rows[0][0], (int, float)):
        return int(rows[0][0])
    return len(rows)

_DC_ALIAS = re.compile(r"\bdim_watchable\s+dc\b", re.IGNORECASE)

def _repair_missing_genre_join(sql: str) -> str:
    """Even with an explicit reminder, the model reliably
    writes 'AND dg.genre_name ILIKE ...' in the WHERE clause while forgetting
    the two JOIN lines that define dg - 3 out of 3 attempts, every time, for
    any question that also needed a platform join (genre + platform + LEFT
    JOIN unwatched was apparently one join too many to track at once). This
    is a purely mechanical, deterministic defect - not a semantic judgment
    call - so it's cheaper and more reliable to patch it in code than to
    keep spending retries hoping the model remembers on its own."""
    upper = sql.upper()
    if "GENRE_NAME" not in upper or "DIM_GENRE" in upper:
        return sql
    if not _DC_ALIAS.search(sql):
        return sql  # unknown alias for dim_watchable, can't safely repair
    where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
    if not where_match:
        return sql
    insertion = (
        "JOIN marts.bridge_content_genre bcg ON bcg.content_id = dc.content_id\n"
        "JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id\n"
    )
    return sql[:where_match.start()] + insertion + sql[where_match.start():]

_BRIDGE_PLATFORM_ALIAS = re.compile(r"bridge_content_platform\s+(\w+)", re.IGNORECASE)

def _repair_missing_platform_join(sql: str) -> str:
    """100% reproducible on 'Netflix'/'Prime' questions: the
    model writes 'WHERE dp.platform_name = ...' but never emits the JOIN(s)
    that define dp. Two distinct shapes observed, not one: sometimes BOTH
    joins are missing entirely (total omission); sometimes the model already
    wrote 'JOIN marts.bridge_content_platform bcp ON ...' correctly and only
    forgot the second dim_platform join. Blindly inserting both lines for the
    second shape re-declares the bcp alias the model already used, failing
    with 'table name "bcp" specified more than once'. This
    exact regression appeared the first time this repair function shipped.
    Handling the two shapes separately instead of assuming total omission."""
    upper = sql.upper()
    if "PLATFORM_NAME" not in upper or "DIM_PLATFORM" in upper:
        return sql
    where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
    if not where_match:
        return sql

    bridge_alias_match = _BRIDGE_PLATFORM_ALIAS.search(sql)
    if bridge_alias_match:
        # Bridge join already present under whatever alias the model chose -
        # only the dim_platform join is missing. Reuse that alias, don't
        # touch the bridge join at all.
        bridge_alias = bridge_alias_match.group(1)
        insertion = f"JOIN marts.dim_platform dp ON dp.platform_id = {bridge_alias}.platform_id\n"
        return sql[:where_match.start()] + insertion + sql[where_match.start():]

    # Neither join present at all - need dim_watchable's alias to hang the
    # bridge join off of.
    if not _DC_ALIAS.search(sql):
        return sql  # unknown alias for dim_watchable, can't safely repair
    insertion = (
        "JOIN marts.bridge_content_platform bcp ON bcp.content_id = dc.content_id\n"
        "JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id\n"
    )
    return sql[:where_match.start()] + insertion + sql[where_match.start():]

def _has_missing_platform_join(sql: str) -> bool:
    """Belt-and-suspenders, same pattern as the genre guard: if the repair
    above couldn't fix it (unknown alias, or platform_name referenced in a
    shape it doesn't recognize), reject outright rather than run a query
    guaranteed to error on a missing table reference."""
    upper = sql.upper()
    return "PLATFORM_NAME" in upper and "DIM_PLATFORM" not in upper

_AVG_ALIAS = re.compile(r"\bAVG\s*\([^)]*\)\s+(?:AS\s+)?(\w+)", re.IGNORECASE)
_HAVING_COUNT_FLOOR = re.compile(r"\bHAVING\s+COUNT\s*\(\s*\*\s*\)\s*>=\s*(\d+)\s*(?=ORDER\b)", re.IGNORECASE)

def _repair_missing_avg_floor(sql: str) -> str:
    """Bug: "highest rated genre" let a one-off rating win at 10/10. The
    SCHEMA_RULES floor (>= 5 for movie/TV genre, >= 2 for book subject and
    creator, matching meta.taste_profile) is in the prompt, but the model
    either drops the HAVING or copies the wrong floor. Mechanical and deterministic, same as the
    join repairs: for a single GROUP BY query ordered by an average, add the
    floor if there's no HAVING, or raise a plain `HAVING COUNT(*) >= k` that
    is below it. Any other HAVING shape is left alone."""
    upper = sql.upper()
    if upper.count("GROUP BY") != 1 or upper.count("ORDER BY") != 1:
        return sql
    if not re.search(r"\bAVG\s*\(", sql, re.IGNORECASE):
        return sql
    group_match = re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE)
    order_match = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    if not group_match or not order_match or order_match.start() < group_match.start():
        return sql
    having_match = _HAVING_COUNT_FLOOR.search(sql)
    if "HAVING" in upper and not having_match:
        return sql
    order_clause = sql[order_match.end():]
    avg_aliases = _AVG_ALIAS.findall(sql)
    orders_by_avg = re.search(r"\bAVG\s*\(", order_clause, re.IGNORECASE) or any(
        re.search(rf"\b{re.escape(a)}\b", order_clause, re.IGNORECASE) for a in avg_aliases
    )
    if not orders_by_avg:
        return sql
    group_end = having_match.start() if having_match else order_match.start()
    group_clause = sql[group_match.end():group_end]
    group_upper = group_clause.upper()
    floor = 2 if "PRIMARY_CREATOR" in group_upper or "SUBJECT_NAME" in group_upper else 5
    if having_match:
        if int(having_match.group(1)) >= floor:
            return sql
        return sql[:having_match.start(1)] + str(floor) + sql[having_match.end(1):]
    return sql[:order_match.start()] + f"HAVING COUNT(*) >= {floor}\n" + sql[order_match.start():]

_BOOK_SUBJECT_ALIAS = re.compile(r"\bmarts\.dim_book_subject\s+(?:AS\s+)?(\w+)", re.IGNORECASE)

def _repair_missing_generic_subject_filter(sql: str) -> str:
    """Generic Open Library tags ('Fiction', 'New York Times bestseller')
    sit on most books, so a subject ranking without the is_generic filter
    answers "Fiction" while the taste profile, which skips them, says
    something specific. Adds NOT <alias>.is_generic to a single-WHERE (or
    WHERE-less) query that groups by subject; anything else is left alone."""
    alias_match = _BOOK_SUBJECT_ALIAS.search(sql)
    if not alias_match or "IS_GENERIC" in sql.upper():
        return sql
    alias = alias_match.group(1)
    if alias.upper() in ("ON", "WHERE", "JOIN", "GROUP", "ORDER", "LIMIT"):
        return sql
    upper = sql.upper()
    if upper.count("GROUP BY") != 1 or "SUBJECT_NAME" not in upper[upper.index("GROUP BY"):]:
        return sql
    where_count = len(re.findall(r"\bWHERE\b", sql, re.IGNORECASE))
    if where_count == 1:
        return re.sub(r"\bWHERE\b", f"WHERE NOT {alias}.is_generic AND", sql, count=1, flags=re.IGNORECASE)
    if where_count == 0:
        group_match = re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE)
        return sql[:group_match.start()] + f"WHERE NOT {alias}.is_generic\n" + sql[group_match.start():]
    return sql

def _repair_missing_content_type_filter(sql: str, question: str) -> str:
    """Bug: 'movies on Apple TV+' - all 3 attempts dropped
    content_type = 'movie' entirely while also juggling a platform join, same
    'too many things to track at once' pattern already seen with genre+platform
    together. Only handles the 'missing entirely' case (no content_type
    reference at all) - a WRONG existing literal (says 'tv' when the question
    asked about movies) is a different, riskier kind of edit (which occurrence,
    which alias) and isn't the failure actually observed, so that case is left
    to the existing reject-guard unchanged. Books are skipped - fact_reading_
    history/dim_book have no content_type column, nothing to insert.
    Checks specifically for a content_type FILTER (content_type = / IN (...)),
    not just the column name appearing anywhere. The model
    often SELECTs dc.content_type as a plain display column with no WHERE
    filter on it at all; a bare substring check can't tell a selected column
    apart from an actual filter and wrongly skips the repair."""
    upper = sql.upper()
    if re.search(r"CONTENT_TYPE\s*(=|IN\s*\()", upper):
        return sql  # an actual filter already exists - either correct, or the wrong-literal case this doesn't touch
    if not _DC_ALIAS.search(sql):
        return sql  # unknown alias for dim_watchable, can't safely repair
    kind = None
    for candidate, pattern in _CONTENT_TYPE_WORDS.items():
        if candidate == "book":
            continue
        if pattern.search(question):
            kind = candidate
            break
    if kind is None:
        return sql
    where_match = re.search(r"\bWHERE\b", sql, re.IGNORECASE)
    if not where_match:
        return sql
    insertion = f"dc.content_type = '{kind}' AND "
    insert_at = where_match.end()
    return sql[:insert_at] + " " + insertion + sql[insert_at:].lstrip()

def _try_execute(sql: str, question: str):
    """Free verifier - costs nothing to try against local Postgres, unlike a
    billed warehouse. Returns (columns, rows, total_count) on success, None on
    any failure (bad SQL, wrong table, timeout - all just mean 'discard this
    candidate'). Fetches the FULL result set (this dataset is a few hundred
    rows at most)."""
    if not _is_safe_select(sql):
        return None
    sql = _repair_missing_genre_join(sql)
    sql = _repair_missing_platform_join(sql)
    sql = _repair_missing_avg_floor(sql)
    sql = _repair_missing_generic_subject_filter(sql)
    sql = _repair_missing_content_type_filter(sql, question)
    sql = _repair_unrequested_language_filter(sql, question)
    if _has_missing_platform_join(sql):
        log.info("[ask] candidate SQL failed, discarding: platform_name referenced but dim_platform join missing (repair could not fix)")
        return None
    if _has_impossible_null_check(sql):
        log.info("[ask] candidate SQL failed, discarding: content_id IS NULL after a plain JOIN (always empty)")
        return None
    if _has_wrong_date_window(sql, question):
        log.info("[ask] candidate SQL failed, discarding: question names a month but SQL uses a rolling window instead")
        return None
    if _has_missing_content_type_filter(sql, question):
        log.info("[ask] candidate SQL failed, discarding: question names a specific content type but SQL has no matching content_type filter")
        return None
    if _has_missing_genre_filter(sql, question):
        log.info("[ask] candidate SQL failed, discarding: question names a specific genre but SQL has no matching dim_genre filter")
        return None
    if _has_incomplete_comparison_filter(sql, question):
        log.info("[ask] candidate SQL failed, discarding: comparison question names two content types but SQL only filters to one")
        return None
    if _has_unrequested_language_filter(sql, question):
        log.info("[ask] candidate SQL failed, discarding: SQL filters by language but question never named one")
        return None
    if _has_missing_consumed_filter(sql, question):
        log.info("[ask] candidate SQL failed, discarding: question asks what was watched/read but SQL has no consumption_status filter")
        return None
    conn = None
    try:
        conn = _ask_conn()
        cur = conn.cursor()
        cur.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        # Postgres AVG() on an integer/numeric rating column returns full
        # precision (e.g. 7.5606060606060606), and this leaked
        # straight into answers verbatim. Round before it ever reaches
        # top_fact/data_block construction, not just at display time.
        # psycopg2 maps Postgres NUMERIC (what AVG() on an int column
        # returns) to Python Decimal, not float, which surfaced a second
        # time ("8.3125" instead of "8.31"): the isinstance(v, float) check
        # alone silently let every Decimal value through unrounded. Round
        # both types, converting Decimal to float so downstream formatting/
        # JSON serialization treats it the same as any other numeric value.
        rows = [
            tuple(
                round(float(v), 2) if isinstance(v, (float, Decimal)) else v
                for v in row
            )
            for row in rows
        ]
        return columns, rows, _resolve_count(columns, rows), sql
    except Exception as e:
        log.info(f"[ask] candidate SQL failed, discarding: {e}")
        return None
    finally:
        if conn:
            conn.close()

_NEGATIVE_ANSWER = re.compile(
    r"\bnothing matched\b|\bnothing found\b|\bcouldn.?t find\b|\bno matches?\b|"
    r"\bdon.?t have any\b|\bdidn.?t (find|match)\b",
    re.IGNORECASE,
)

def _guard_against_negation(text: str, columns: list, rows: list, total_count: int) -> str:
    """Given real, non-empty row data, the answer-generation
    LLM sometimes still writes a 'nothing matched' style sentence anyway -
    contradicting the very data it was handed (e.g. 1 real horror movie in
    the rows, answer text claiming zero). Since real rows are already
    verified ground truth by this point, trust them over the model's prose:
    swap in a plain deterministic sentence rather than show a caption that
    flatly contradicts the data underneath it."""
    if not rows or not _NEGATIVE_ANSWER.search(text):
        return text
    log.info("[ask] answer text contradicted non-empty rows, using deterministic fallback")
    if len(columns) == 1:
        return f"You've got {total_count} matching item{'s' if total_count != 1 else ''}."
    names = ", ".join(str(row[0]) for row in rows[:5])
    more = f", and {total_count - 5} more" if total_count > 5 else ""
    return f"Found {total_count}: {names}{more}."

def _generate_answer(question: str, columns: list, rows: list, total_count: int, sql: str, tokens: list) -> str:
    if not rows:
        data_block = "(no rows returned)"
    elif len(columns) == 1:
        # A single-column result is structurally a count query (Example 4's
        # own contract: "how many" -> just an id column, nothing to list).
        # Two different prompt wordings both failed to reliably stop the
        # model from reducing a genuine multi-column LIST to just this same
        # number instead of describing the rows - so the model isn't asked
        # to make that call at all here; the single-column shape already
        # settles it. Skips the LLM call entirely: total_count is exact
        # (Python len() or the model's own verified aggregate), nothing left
        # for language generation to add.
        return f"You've got {total_count} matching item{'s' if total_count != 1 else ''}."
    elif "GROUP BY" in sql.upper() and not _COMPARISON_WORDS.search(question):
        # A GROUP BY query (Example 5's ranking shape - "highest rated genre",
        # etc.) is already correctly sorted by the SQL's own ORDER BY; row 0
        # IS the answer. 100% reproducible even at
        # temperature=0: asking the model to read the table and identify the
        # top row itself, even with an explicit "the first row is correct"
        # instruction, made it pick a different row every single time
        # (fixated on whichever column had the largest raw number - a COUNT
        # column, not the actual ranking column). Same fix as the count bug:
        # stop asking the model to do the comparison, state the already-
        # correct answer as a given fact and let it only phrase around that.
        # A "compare A to B" GROUP BY (no ranking, no ORDER BY ... LIMIT 1) is
        # explicitly excluded here - treating row 0 as "the" answer would
        # silently drop every other group from the response. It falls through
        # to the generic multi-row branch below instead, which describes all
        # rows rather than crowning a single winner.
        top_fact = "; ".join(f"{_humanize_column(col, sql)} = {val}" for col, val in zip(columns, rows[0]))
        rest = rows[1:MAX_ROWS_TO_MODEL]
        context_block = ""
        if rest:
            header = " | ".join(columns)
            body = "\n".join(" | ".join(str(v) for v in row) for row in rest)
            context_block = f"\n\nOther rows for optional supporting context only (NOT the top result, do not present any of these as the answer):\n{header}\n{body}"

        prompt = f"""You are a personal content assistant.
FACT (already correctly computed by a sorted query — do not recalculate,
do not pick a different row, do not use any other column to compare): {top_fact}

Write a natural answer to the question below that states this fact. Never
mention a different item as if it were the answer.{context_block}

{ANSWER_STYLE}

Question: "{question}"

Answer:"""
        response = _answer_llm().invoke(prompt)
        usage = getattr(response, "usage_metadata", None) or {}
        tokens.append((usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0))
        return _guard_against_negation(response.content.strip(), columns, rows, total_count)
    else:
        sample = rows[:MAX_ROWS_TO_MODEL]
        header = " | ".join(columns)
        body = "\n".join(" | ".join(str(v) for v in row) for row in sample)
        data_block = f"{header}\n{body}"
        if total_count > len(sample):
            data_block += f"\n(showing first {len(sample)} of {total_count} total matches)"

    prompt = f"""You are a personal content assistant. Answer the question using
ONLY the data below — it came from a real query against this user's actual
watch history and content catalog. Describe or list the actual rows shown —
never invent a title, rating, or fact that isn't in them.

{ANSWER_STYLE}

Question: "{question}"

Data:
{data_block}

Answer:"""
    response = _answer_llm().invoke(prompt)
    usage = getattr(response, "usage_metadata", None) or {}
    tokens.append((usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0))
    return _guard_against_negation(response.content.strip(), columns, rows, total_count)

_COLUMN_LABELS = {"avg_rating": "Avg Rating", "vote_average": "Rating"}

def _humanize_column(col: str, sql: str = "") -> str:
    # Bug: "highest rated book subject" was answered as "watched
    # 3 times" - the count column was always labelled "Watched", and the
    # answer model phrases around that label. Books are "Read".
    if col == "n":
        return "Read" if "FACT_READING_HISTORY" in sql.upper() else "Watched"
    return _COLUMN_LABELS.get(col, col.replace("_", " ").title())

_ITEM_LABELS = {"movie": "movies", "tv": "tv shows", "book": "books"}

def _derive_item_label(question: str) -> str:
    for kind, pattern in _CONTENT_TYPE_WORDS.items():
        if pattern.search(question):
            return _ITEM_LABELS[kind]
    return "items"

def _classify_result(question: str, columns: list, rows: list, total_count: int, sql: str) -> tuple[str, dict]:
    """Best-effort structured-data extraction for the frontend's answer
    cards - purely additive. _generate_answer's plain-text sentence is
    always computed independently of this function and is always safe to
    show on its own, so a bug or blind spot here can never produce a broken
    answer - at worst it falls back to type 'prose', which the frontend
    renders as today's flat text card. A genuinely new question shape just
    doesn't get the fancy card until this function is taught that shape."""
    try:
        if not rows:
            return "empty", {}
        if len(columns) == 1:
            return "count", {"value": total_count, "label": _derive_item_label(question)}
        upper = sql.upper()
        is_group_by = "GROUP BY" in upper
        is_comparison = bool(_COMPARISON_WORDS.search(question))
        if is_group_by and is_comparison and 2 <= len(rows) <= 4:
            numeric_idx = next(
                (i for i, v in enumerate(rows[0]) if i > 0 and isinstance(v, (int, float))), None
            )
            if numeric_idx is not None:
                groups = [{"name": str(row[0]), "value": row[numeric_idx]} for row in rows]
                return "compare", {"groups": groups}
        elif is_group_by and not is_comparison:
            figures = [
                {"label": _humanize_column(col, sql), "value": val}
                for col, val in zip(columns[1:], rows[0][1:])
                if isinstance(val, (int, float))
            ]
            if figures:
                return "rank", {"name": str(rows[0][0]), "figures": figures}
        elif not is_group_by and isinstance(rows[0][0], str):
            items = [str(row[0]) for row in rows[:20]]
            return "list", {"count": total_count, "items": items}
    except Exception as e:
        log.info(f"[ask] structured classification skipped, falling back to prose: {e}")
    return "prose", {}

_CONTENT_KEYWORDS = re.compile(
    r"\b(watch(?:ed)?|rate[ds]?|read|books?|movies?|shows?|tv|series|genre|"
    r"platform|recommend|suggest|seen|finish(?:ed)?|start(?:ed)?|hindi|"
    r"english|marathi|netflix|prime|apple|hulu|max|disney|peacock|language|"
    r"year|director|authors?|creators?|ratings?|scores?|history|picks?|"
    r"today|week|month|last|best|worst|highest|lowest|average|count|many|"
    r"list)\b",
    re.IGNORECASE,
)

def _is_gibberish(question: str) -> bool:
    """Gibberish input ('asdkjaslkdjaslkdj gibberish nonsense')
    doesn't reliably fail to produce SQL - the model sometimes generates a
    syntactically valid, executable query anyway (e.g. a generic 'list some
    content' shape with no real connection to the input), which then returns
    real DB rows that get described as if they answered the nonsense question.
    The rows aren't fabricated, but presenting them as an answer to gibberish
    is. Checked before any SQL generation is attempted at all - cheaper and
    more honest than trying to catch it after the fact.
    Deliberately excludes generic interrogatives (what/which/when/how/why/
    who) from the keyword set - every one of those appears in off-topic
    questions too ('what is the capital of France' contains 'what'), so they
    don't actually signal the question is about this app's content/watch
    data; only the domain-specific words below do."""
    return not _CONTENT_KEYWORDS.search(question)

def answer_question(question: str) -> dict:
    """Best-of-N: generate a SQL candidate, verify it for free by actually
    running it. A query that errors gets discarded immediately. A query that
    runs but returns zero rows is kept only as a fallback - a later, better-
    worded attempt might hit real rows (e.g. the model matching a language
    name like 'Hindi' against the actual code 'hi' on one attempt but not
    another). Only settles for an empty result if every attempt came up empty.
    Never falls through to a model guess - if every candidate fails outright,
    say so honestly instead."""
    if _is_gibberish(question):
        log.info(f"[ask] gibberish detected, no SQL attempted: {question!r}")
        return {
            "answer": (
                "That doesn't look like a question I can answer. Try asking "
                "something like: what did I watch last week, Hindi movies I "
                "haven't seen yet, or my highest rated genre."
            ),
            "type": "prose",
            "data": {},
            "tokens": 0,
            "grounded": False,
        }
    tokens = []
    empty_result = None
    for attempt in range(MAX_SQL_ATTEMPTS):
        sql = _generate_sql(question, attempt, tokens)
        result = _try_execute(sql, question)
        if result is None:
            continue
        columns, rows, total_count, sql = result
        if rows:
            answer = _generate_answer(question, columns, rows, total_count, sql, tokens)
            kind, data = _classify_result(question, columns, rows, total_count, sql)
            return {"answer": answer, "type": kind, "data": data, "tokens": sum(tokens), "grounded": True}
        empty_result = (columns, rows, total_count, sql)

    if empty_result is not None:
        columns, rows, total_count, sql = empty_result
        answer = _generate_answer(question, columns, rows, total_count, sql, tokens)
        kind, data = _classify_result(question, columns, rows, total_count, sql)
        return {"answer": answer, "type": kind, "data": data, "tokens": sum(tokens), "grounded": True}

    return {
        "answer": "Couldn't find a reliable way to answer that from your data. Try rephrasing.",
        "type": "prose",
        "data": {},
        "tokens": sum(tokens),
        "grounded": False,
    }
