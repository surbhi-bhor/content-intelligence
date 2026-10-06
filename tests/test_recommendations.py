import logging

from dagster import build_op_context

from conftest import FakeConnection, FakeCursor
from ops import recommendation_op as rec

log = logging.getLogger("test")


def candidate(cid, lang="en", ctype="movie", creator=None, genres=(), popularity=0.0):
    # Same tuple shape get_candidates() returns.
    return (cid, cid.upper(), ctype, creator, lang, list(genres), popularity, 7.0)


GENRE_AVG = {"Drama": 8.0, "Comedy": 6.0}
CREATOR_AVG = {"Nolan": 9.0}


def test_predicted_score_averages_genre_and_creator():
    c = candidate("movie_1", creator="Nolan", genres=["Drama"])
    assert rec.compute_predicted_score(c, GENRE_AVG, CREATOR_AVG) == 8.5


def test_predicted_score_is_none_without_signal():
    c = candidate("movie_1", genres=["Horror"])
    assert rec.compute_predicted_score(c, GENRE_AVG, CREATOR_AVG) is None


def test_model_picks_outside_shortlist_are_replaced():
    shortlist = [candidate(f"movie_{i}", genres=["Drama"]) for i in range(12)]
    raw_ids = ["movie_0", "movie_999", "movie_0", "movie_1"]  # invalid + duplicate
    picks = rec.validate_model_picks(raw_ids, shortlist, GENRE_AVG, CREATOR_AVG, log)
    ids = [p["content_id"] for p in picks]
    shortlist_ids = {c[0] for c in shortlist}
    assert len(ids) == rec.DAILY_PICKS_COUNT
    assert len(set(ids)) == len(ids)
    assert set(ids) <= shortlist_ids
    assert "movie_999" not in ids


def test_model_output_wrapped_in_dict_is_handled():
    shortlist = [candidate("movie_1"), candidate("movie_2")]
    picks = rec.validate_model_picks([{"content_id": "movie_2"}], shortlist, {}, {}, log)
    assert picks[0]["content_id"] == "movie_2"


def test_language_allocation_respects_slots_and_fallback():
    candidates = (
        [candidate(f"en_{i}", "en") for i in range(6)]
        + [candidate("hi_0", "hi")]  # only one Hindi title available
        + [candidate(f"mr_{i}", "mr") for i in range(3)]
    )
    slots = {"en": 2, "hi": 2, "mr": 1}
    picks = rec.allocate_by_language([], candidates, slots, {}, {}, fallback_lang="en")
    langs = [next(c[4] for c in candidates if c[0] == p["content_id"]) for p in picks]
    assert len(picks) == 5
    assert langs.count("hi") == 1      # took the only Hindi title
    assert langs.count("mr") == 1
    assert langs.count("en") == 3      # 2 slots + 1 fallback for the Hindi shortfall


def test_empty_slots_allocate_nothing():
    # generate_recommendations treats an empty result as "keep the picks";
    # this pins the behaviour that guard depends on.
    assert rec.allocate_by_language([], [candidate("en_0")], {}, {}, {}) == []


def test_missing_taste_profile_returns_full_tuple():
    summary, genres, creators, genre_avg, creator_avg = rec.get_latest_watch_profile(FakeCursor())
    assert summary is None
    assert (genres, creators, genre_avg, creator_avg) == ([], [], {}, {})


# ── Books ────────────────────────────────────────────────────────

def book(cid, author, score=None, signals=0, similar=None, shared=0, votes=0, author_avg=None):
    # Same dict shape get_book_candidates() returns.
    return {
        "content_id": cid, "title": cid.title(), "primary_creator": author,
        "predicted_score": score, "signal_count": signals,
        "similar_to_content_id": similar, "similar_to_title": similar.title() if similar else None,
        "similar_to_rating": 10.0 if similar else None,
        "shared_subject_count": shared, "shared_subjects": ["Indic Mythology", "Siva (Hindu deity)"][:shared],
        "author_avg_rating": author_avg, "author_read_count": 2 if author_avg else 0, "vote_count": votes,
    }


def test_book_with_both_signals_ranks_first():
    subject_only = book("b1", "A", score=10.0, signals=1, similar="liked", shared=3)
    both = book("b2", "B", score=8.5, signals=2, similar="liked", shared=2, author_avg=7.0)
    no_signal = book("b3", "C", votes=500)
    picks = rec.select_book_picks([no_signal, subject_only, both], count=3)
    assert [p["content_id"] for p in picks] == ["b2", "b1", "b3"]


def test_book_picks_vary_author_and_source_book():
    pool = [
        book("b1", "Haig", score=10.0, signals=1, similar="midnight", shared=3),
        book("b2", "Haig", score=10.0, signals=1, similar="midnight", shared=3),
        book("b3", "Jordan", score=10.0, signals=1, similar="midnight", shared=2),
        book("b4", "Pratchett", score=10.0, signals=1, similar="midnight", shared=2),
        book("b5", "Kane", score=9.0, signals=1, similar="palace", shared=2),
    ]
    picks = rec.select_book_picks(pool, count=3)
    # One per author, at most two "like midnight".
    assert [p["content_id"] for p in picks] == ["b1", "b3", "b5"]


def test_book_caps_relax_when_the_pool_is_thin():
    pool = [book("b1", "Haig", score=9.0, signals=1), book("b2", "Haig", score=8.0, signals=1)]
    assert len(rec.select_book_picks(pool, count=5)) == 2


def test_book_reason_names_the_read_it_is_based_on():
    c = book("b1", "Amish", score=9.5, signals=2, similar="meluha", shared=2, author_avg=9.0)
    assert rec.build_book_reason(c) == (
        "By Amish, whose books you rate 9 avg, and like Meluha (Indic Mythology, Siva (Hindu deity))."
    )
    assert rec.build_book_reason(book("b2", "X", score=10.0, signals=1, similar="meluha", shared=2)) == (
        "Like Meluha, which you rated 10: Indic Mythology, Siva (Hindu deity)."
    )


def test_book_picks_are_written_even_without_a_taste_profile(monkeypatch):
    cursor = FakeCursor()  # no taste profile row: the movie side exits early
    monkeypatch.setattr(rec, "get_conn", lambda: FakeConnection(cursor))
    monkeypatch.setattr(rec, "get_book_candidates", lambda cur: [book("b1", "Haig", score=9.0, signals=1)])

    assert rec.generate_recommendations(build_op_context()) == 0
    assert "DELETE FROM meta.daily_recommendations_books;" in cursor.executed
    assert any(sql.startswith("INSERT INTO meta.daily_recommendations_books") for sql in cursor.executed)
