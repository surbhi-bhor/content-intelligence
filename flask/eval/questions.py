"""Evaluation set for /ask: 20 questions, each with a hand-written gold query.

None of these questions appear in the agent's worked examples (agent.py
WORKED_EXAMPLE), so a pass means the model generalised rather than copied.

Gold queries return the full correct answer with no LIMIT. Each case names
how the agent's result is compared with it (see run_ask_eval.py):

  count   the agent's count equals the number of gold rows
  top     the agent's first row contains the gold first row's first value.
          A second gold column is the ranking value: any gold row tied with
          the first on it is also accepted.
  set     the agent's rows and the gold first-column values match exactly
  subset  the agent returned at least one row, and every row it returned
          contains a gold first-column value (for open-ended lists such as
          "unwatched X", where any 10 correct titles are a right answer)
"""

CASES = [
    # ── Counting ─────────────────────────────────────────────────
    {
        "id": "count_tv_watched",
        "question": "How many TV shows have I finished?",
        "match": "count",
        "gold": """
            SELECT content_id FROM marts.fact_watch_history
            WHERE content_type = 'tv' AND consumption_status = 'consumed'
        """,
    },
    {
        "id": "count_books_read",
        "question": "How many books have I read?",
        "match": "count",
        "gold": """
            SELECT content_id FROM marts.fact_reading_history
            WHERE consumption_status = 'consumed'
        """,
    },
    {
        "id": "count_hindi_movies_watched",
        "question": "How many Hindi movies have I watched?",
        "match": "count",
        "gold": """
            SELECT fw.content_id FROM marts.fact_watch_history fw
            JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
            WHERE fw.content_type = 'movie' AND fw.consumption_status = 'consumed'
              AND dc.original_language = 'hi'
        """,
    },
    {
        "id": "count_planned",
        "question": "How many titles are on my watchlist that I haven't started?",
        "match": "count",
        "gold": """
            SELECT content_id FROM marts.fact_watch_history
            WHERE consumption_status = 'planned'
        """,
    },
    {
        "id": "count_comedy_watched",
        "question": "How many comedies have I watched?",
        "match": "count",
        "gold": """
            SELECT DISTINCT fw.content_id FROM marts.fact_watch_history fw
            JOIN marts.bridge_content_genre bcg ON bcg.content_id = fw.content_id
            JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
            WHERE fw.consumption_status = 'consumed' AND dg.genre_name ILIKE 'comedy'
        """,
    },

    # ── Rankings and single facts ────────────────────────────────
    {
        "id": "top_rated_movie",
        "question": "What is my highest rated movie?",
        "match": "top",
        "gold": """
            SELECT dc.title, fw.rating FROM marts.fact_watch_history fw
            JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
            WHERE fw.content_type = 'movie' AND fw.consumption_status = 'consumed'
              AND fw.rating IS NOT NULL
            ORDER BY fw.rating DESC
        """,
    },
    {
        "id": "most_watched_genre",
        "question": "Which genre have I watched the most?",
        "match": "top",
        "gold": """
            SELECT dg.genre_name, COUNT(*) FROM marts.fact_watch_history fw
            JOIN marts.bridge_content_genre bcg ON bcg.content_id = fw.content_id
            JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
            WHERE fw.consumption_status = 'consumed'
            GROUP BY dg.genre_name ORDER BY COUNT(*) DESC
        """,
    },
    {
        "id": "lowest_rated_genre",
        "question": "Which genre do I rate the lowest?",
        "match": "top",
        "gold": """
            SELECT dg.genre_name, ROUND(AVG(fw.rating), 2) FROM marts.fact_watch_history fw
            JOIN marts.bridge_content_genre bcg ON bcg.content_id = fw.content_id
            JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
            WHERE fw.content_type IN ('movie', 'tv') AND fw.consumption_status = 'consumed'
            GROUP BY dg.genre_name HAVING COUNT(*) >= 5 ORDER BY AVG(fw.rating) ASC
        """,
    },
    {
        "id": "top_director",
        "question": "Which director's movies do I rate highest?",
        "match": "top",
        "gold": """
            SELECT dc.primary_creator, ROUND(AVG(fw.rating), 2) FROM marts.fact_watch_history fw
            JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
            WHERE fw.content_type = 'movie' AND fw.consumption_status = 'consumed'
              AND dc.primary_creator IS NOT NULL
            GROUP BY dc.primary_creator HAVING COUNT(*) >= 2 ORDER BY AVG(fw.rating) DESC
        """,
    },
    {
        "id": "most_read_author",
        "question": "Which author have I read the most books by?",
        "match": "top",
        "gold": """
            SELECT db.primary_creator, COUNT(*) FROM marts.fact_reading_history fr
            JOIN marts.dim_book db ON db.content_id = fr.content_id
            WHERE fr.consumption_status = 'consumed' AND db.primary_creator IS NOT NULL
            GROUP BY db.primary_creator ORDER BY COUNT(*) DESC
        """,
    },
    {
        "id": "longest_book",
        "question": "What is the longest book I've read?",
        "match": "top",
        "gold": """
            SELECT db.title, db.page_count FROM marts.fact_reading_history fr
            JOIN marts.dim_book db ON db.content_id = fr.content_id
            WHERE fr.consumption_status = 'consumed' AND db.page_count IS NOT NULL
            ORDER BY db.page_count DESC
        """,
    },
    {
        "id": "top_platform",
        "question": "Which streaming platform has the most of what I've watched?",
        "match": "top",
        "gold": """
            SELECT dp.platform_name, COUNT(*) FROM marts.fact_watch_history fw
            JOIN marts.bridge_content_platform bcp ON bcp.content_id = fw.content_id
            JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id
            WHERE fw.consumption_status = 'consumed'
            GROUP BY dp.platform_name ORDER BY COUNT(*) DESC
        """,
    },

    # ── Comparisons ──────────────────────────────────────────────
    {
        "id": "compare_movie_tv_count",
        "question": "Have I watched more movies or more TV shows?",
        "match": "set",
        "gold": """
            SELECT content_type FROM marts.fact_watch_history
            WHERE content_type IN ('movie', 'tv') AND consumption_status = 'consumed'
            GROUP BY content_type
        """,
    },

    # ── Lists of watched or read items ───────────────────────────
    {
        "id": "rated_ten",
        "question": "Which titles did I rate 10?",
        "match": "subset",
        "gold": """
            SELECT dc.title FROM marts.fact_watch_history fw
            JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
            WHERE fw.rating = 10
        """,
    },
    {
        "id": "in_progress_tv",
        "question": "Which TV shows am I currently watching?",
        "match": "subset",
        "gold": """
            SELECT dc.title FROM marts.fact_watch_history fw
            JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
            WHERE fw.content_type = 'tv' AND fw.consumption_status = 'in_progress'
        """,
    },
    {
        "id": "marathi_watched",
        "question": "Which Marathi titles have I watched?",
        "match": "subset",
        "gold": """
            SELECT dc.title FROM marts.fact_watch_history fw
            JOIN marts.dim_watchable dc ON dc.content_id = fw.content_id
            WHERE dc.original_language = 'mr' AND fw.consumption_status = 'consumed'
        """,
    },
    {
        "id": "low_rated_books",
        "question": "Which books did I rate 5 or lower?",
        "match": "subset",
        "gold": """
            SELECT db.title FROM marts.fact_reading_history fr
            JOIN marts.dim_book db ON db.content_id = fr.content_id
            WHERE fr.rating <= 5
        """,
    },

    # ── Discovery over unwatched catalogue ───────────────────────
    {
        "id": "unwatched_english_comedy",
        "question": "English comedies I haven't seen yet",
        "match": "subset",
        "gold": """
            SELECT dc.title FROM marts.dim_watchable dc
            JOIN marts.bridge_content_genre bcg ON bcg.content_id = dc.content_id
            JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id
            LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id
            WHERE dc.original_language = 'en' AND dg.genre_name ILIKE 'comedy'
              AND fw.content_id IS NULL
        """,
    },
    {
        "id": "unwatched_prime_tv",
        "question": "TV shows on Prime that I haven't watched",
        "match": "subset",
        "gold": """
            SELECT dc.title FROM marts.dim_watchable dc
            JOIN marts.bridge_content_platform bcp ON bcp.content_id = dc.content_id
            JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id
            LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id
            WHERE dc.content_type = 'tv' AND dp.platform_name ILIKE '%amazon prime%'
              AND fw.content_id IS NULL
        """,
    },

    # ── Weekly picks ─────────────────────────────────────────────
    {
        "id": "current_book_picks",
        "question": "What books are recommended for me this week?",
        "match": "subset",
        "gold": """
            SELECT title FROM meta.daily_recommendations_books
            WHERE generated_at = (SELECT MAX(generated_at) FROM meta.daily_recommendations_books)
        """,
    },
]
