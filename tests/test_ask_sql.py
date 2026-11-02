import pytest

import agent

# ── Safety: only a single read-only statement may run ────────────

@pytest.mark.parametrize("sql", [
    "SELECT * FROM marts.dim_watchable",
    "WITH x AS (SELECT 1) SELECT * FROM x",
])
def test_safe_select_accepts_reads(sql):
    assert agent._is_safe_select(sql)


@pytest.mark.parametrize("sql", [
    "DELETE FROM meta.user_config",
    "SELECT 1; DROP TABLE marts.dim_watchable",
    "UPDATE marts.dim_watchable SET title = 'x'",
    "INSERT INTO meta.not_interested VALUES ('x')",
    "",
])
def test_safe_select_rejects_writes_and_multiple_statements(sql):
    assert not agent._is_safe_select(sql)


def test_extract_sql_strips_code_fences():
    assert agent._extract_sql("```sql\nSELECT 1;\n```") == "SELECT 1"


# ── Repair: minimum-count floor when ranking by an average ───────

SUBJECT = (
    "SELECT dbs.subject_name, AVG(fw.rating) AS avg_rating FROM marts.fact_reading_history fw "
    "JOIN marts.bridge_content_book_subject b ON b.content_id = fw.content_id "
    "JOIN marts.dim_book_subject dbs ON dbs.subject_id = b.subject_id "
    "GROUP BY dbs.subject_name {having}ORDER BY avg_rating DESC LIMIT 10"
)
CREATOR = (
    "SELECT dw.primary_creator, AVG(f.rating) a FROM marts.fact_watch_history f "
    "JOIN marts.dim_watchable dw USING (content_id) GROUP BY dw.primary_creator {having}ORDER BY a DESC"
)


GENRE = (
    "SELECT dg.genre_name, AVG(f.rating) AS avg_rating FROM marts.fact_watch_history f "
    "JOIN marts.bridge_content_genre b ON b.content_id = f.content_id "
    "JOIN marts.dim_genre dg ON dg.genre_id = b.genre_id "
    "GROUP BY dg.genre_name {having}ORDER BY avg_rating DESC LIMIT 10"
)


@pytest.mark.parametrize("sql, expected", [
    (GENRE.format(having=""), "HAVING COUNT(*) >= 5"),                         # missing floor added
    (GENRE.format(having="HAVING COUNT(*) >= 2 "), "HAVING COUNT(*) >= 5"),    # creator floor copied: raised
    (GENRE.format(having="HAVING COUNT(*) >= 8 "), "HAVING COUNT(*) >= 8"),    # stricter floor kept
    (SUBJECT.format(having=""), "HAVING COUNT(*) >= 2"),                       # book subjects use 2
    (SUBJECT.format(having="HAVING COUNT(*) >= 1 "), "HAVING COUNT(*) >= 2"),  # too low: raised
    (CREATOR.format(having=""), "HAVING COUNT(*) >= 2"),                       # creators use 2
])
def test_average_rankings_get_the_right_floor(sql, expected):
    repaired = agent._repair_missing_avg_floor(sql)
    assert expected in repaired
    assert repaired.upper().count("HAVING") == 1


def test_floor_repair_leaves_other_queries_alone():
    count_query = "SELECT g, COUNT(*) n FROM t GROUP BY g ORDER BY n DESC"
    assert agent._repair_missing_avg_floor(count_query) == count_query


def test_generic_book_subjects_are_filtered_out():
    repaired = agent._repair_missing_generic_subject_filter(SUBJECT.format(having=""))
    assert "WHERE NOT dbs.is_generic\nGROUP BY" in repaired

    with_where = SUBJECT.format(having="").replace("GROUP BY", "WHERE fw.has_rating GROUP BY")
    assert "WHERE NOT dbs.is_generic AND fw.has_rating" in agent._repair_missing_generic_subject_filter(with_where)

    already = with_where.replace("fw.has_rating", "NOT dbs.is_generic")
    assert agent._repair_missing_generic_subject_filter(already) == already


# ── Repair: genre filter written without its joins ───────────────

def test_missing_genre_join_is_added():
    sql = (
        "SELECT dc.title FROM marts.dim_watchable dc "
        "WHERE dg.genre_name ILIKE 'comedy'"
    )
    repaired = agent._repair_missing_genre_join(sql)
    assert "JOIN marts.bridge_content_genre bcg" in repaired
    assert "JOIN marts.dim_genre dg" in repaired
    assert repaired.index("JOIN marts.dim_genre") < repaired.index("WHERE")


# ── Answer shaping ───────────────────────────────────────────────

def test_count_column_label_depends_on_domain():
    assert agent._humanize_column("n", "SELECT ... FROM marts.fact_reading_history") == "Read"
    assert agent._humanize_column("n", "SELECT ... FROM marts.fact_watch_history") == "Watched"


def test_gibberish_is_rejected_before_any_sql():
    assert agent._is_gibberish("asdkjaslkd qwpoeiqwe")
    assert not agent._is_gibberish("what is my highest rated genre?")


# ── Repairs and guards found by the eval set ─────────────────────
# Each SQL string below is what llama3.2:3b actually wrote during an eval run.

GENRE_NO_ORDER = (
    "SELECT dg.genre_name, AVG(fw.rating) AS avg_rating, COUNT(*) AS n FROM marts.fact_watch_history fw "
    "JOIN marts.bridge_content_genre bcg ON bcg.content_id = fw.content_id "
    "JOIN marts.dim_genre dg ON dg.genre_id = bcg.genre_id "
    "WHERE fw.consumption_status = 'consumed' GROUP BY dg.genre_name HAVING COUNT(*) >= 5"
)


def test_ranking_by_rating_gets_sorted_on_the_average():
    fixed = agent._repair_missing_order_by(GENRE_NO_ORDER, "Which genre do I rate the lowest?")
    assert fixed.endswith("ORDER BY AVG(fw.rating) ASC")


def test_ranking_by_volume_gets_sorted_on_the_count():
    fixed = agent._repair_missing_order_by(GENRE_NO_ORDER, "Which genre have I watched the most?")
    assert fixed.endswith("ORDER BY COUNT(*) DESC NULLS LAST")


def test_order_by_repair_keeps_limit_last_and_skips_non_rankings():
    sql = GENRE_NO_ORDER + " LIMIT 10"
    assert agent._repair_missing_order_by(sql, "Which genre have I watched the most?").endswith(
        "ORDER BY COUNT(*) DESC NULLS LAST LIMIT 10"
    )
    assert agent._repair_missing_order_by(sql, "Show my genres") == sql


def test_desc_sort_puts_unrated_titles_last():
    sql = "SELECT dc.title, fw.rating FROM marts.fact_watch_history fw ORDER BY fw.rating DESC LIMIT 10"
    assert "ORDER BY fw.rating DESC NULLS LAST LIMIT 10" in agent._repair_desc_nulls_first(sql)
    already = "SELECT 1 ORDER BY x DESC NULLS LAST"
    assert agent._repair_desc_nulls_first(already) == already


def test_unwatched_question_swaps_consumed_filter_for_missing_row():
    sql = (
        "SELECT dc.title FROM marts.dim_watchable dc "
        "LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id "
        "WHERE dp.platform_name ILIKE '%prime%' AND fw.consumption_status = 'consumed'"
    )
    fixed = agent._repair_consumed_on_unwatched(sql, "TV shows on Prime that I haven't watched")
    assert "fw.content_id IS NULL" in fixed and "'consumed'" not in fixed


@pytest.mark.parametrize("question, sql, expected", [
    ("How many titles are on my watchlist that I haven't started?",
     "SELECT COUNT(fw.content_id) FROM marts.fact_watch_history fw WHERE fw.consumption_status = 'in_progress'",
     "fw.consumption_status = 'planned'"),
    ("Which TV shows am I currently watching?",
     "SELECT dc.title FROM marts.fact_watch_history fw WHERE fw.consumption_status IN ('in_progress', 'planned')",
     "fw.consumption_status = 'in_progress'"),
])
def test_status_words_map_to_one_status(question, sql, expected):
    assert expected in agent._repair_status_words(sql, question)


def test_watchlist_question_is_not_treated_as_unwatched():
    sql = (
        "SELECT dc.title FROM marts.dim_watchable dc "
        "LEFT JOIN marts.fact_watch_history fw ON fw.content_id = dc.content_id "
        "WHERE fw.consumption_status = 'consumed'"
    )
    question = "What's on my watchlist that I haven't started?"
    assert agent._repair_consumed_on_unwatched(sql, question) == sql


@pytest.mark.parametrize("sql, expected", [
    ("SELECT db.primary_creator, COUNT(*) FROM marts.fact_reading_history fr "
     "JOIN marts.dim_book db ON db.content_id = fr.content_id GROUP BY db.primary_creator",
     "JOIN marts.dim_book db ON db.content_id = fr.content_id WHERE fr.consumption_status = 'consumed' GROUP BY"),
    ("SELECT db.title FROM marts.fact_reading_history fr JOIN marts.dim_book db "
     "ON db.content_id = fr.content_id WHERE fr.rating > 8",
     "WHERE fr.consumption_status = 'consumed' AND fr.rating > 8"),
    ("SELECT title FROM marts.fact_reading_history JOIN marts.dim_book USING (content_id)",
     "WHERE fact_reading_history.consumption_status = 'consumed'"),
])
def test_missing_consumed_filter_is_added(sql, expected):
    assert expected in agent._repair_missing_consumed_filter(sql, "Which author have I read the most books by?")


def test_group_by_content_type_answers_a_two_type_comparison():
    sql = (
        "SELECT fw.content_type, COUNT(*) FROM marts.fact_watch_history fw "
        "WHERE fw.consumption_status = 'consumed' GROUP BY fw.content_type"
    )
    question = "Have I watched more movies or more TV shows?"
    assert not agent._has_missing_content_type_filter(sql, question)
    assert not agent._has_incomplete_comparison_filter(sql, question)


def test_length_and_picks_questions_need_the_right_column_and_table():
    by_date = "SELECT db.title FROM marts.fact_reading_history fw JOIN marts.dim_book db USING (content_id) ORDER BY fw.interaction_date DESC"
    assert agent._has_wrong_length_column(by_date, "What is the longest book I've read?")
    assert not agent._has_wrong_length_column(by_date.replace("fw.interaction_date", "db.page_count"), "longest book?")
    assert agent._has_missing_picks_table(by_date, "What books are recommended for me this week?")
    assert not agent._has_missing_picks_table("SELECT title FROM meta.daily_recommendations_books", "my book picks")
