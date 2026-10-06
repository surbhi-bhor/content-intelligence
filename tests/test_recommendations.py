import logging

from conftest import FakeCursor
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
