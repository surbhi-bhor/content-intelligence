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
