import logging

import pytest
import requests
from dagster import build_op_context

from conftest import FakeConnection, FakeCursor
from ops import hardcover_op, openlib_op, simkl_op, tmdb_op

log = logging.getLogger("test")


# ── Retries ──────────────────────────────────────────────────────

@pytest.mark.parametrize("op_def", [
    tmdb_op.ingest_tmdb_movies, tmdb_op.ingest_tmdb_shows,
    tmdb_op.ingest_tmdb_details, tmdb_op.ingest_tmdb_tv_details,
    simkl_op.ingest_simkl_ratings, hardcover_op.ingest_hardcover_books,
    openlib_op.enrich_book_metadata, openlib_op.discover_openlibrary_books,
])
def test_ops_that_call_an_api_retry(op_def):
    assert op_def.retry_policy is not None
    assert op_def.retry_policy.max_retries >= 1


# ── TMDB ─────────────────────────────────────────────────────────

PROVIDERS = {
    "results": {
        "US": {"flatrate": [{"provider_name": "Hulu"}]},
        "IN": {"flatrate": [{"provider_name": "JioHotstar"}, {"provider_name": "Zee5"}]},
    }
}


def test_platforms_come_from_the_configured_region():
    assert tmdb_op.get_platforms(PROVIDERS, "IN") == ["JioHotstar", "Zee5"]
    assert tmdb_op.get_platforms(PROVIDERS, "US") == ["Hulu"]
    assert tmdb_op.get_platforms(PROVIDERS, "GB") == []


def test_one_failing_discovery_endpoint_does_not_stop_the_rest(monkeypatch):
    def fake_fetch(endpoint, retries=6):
        if "upcoming" in endpoint:
            raise requests.exceptions.SSLError("simulated dropped connection")
        return {"results": [{"id": 1}]}

    monkeypatch.setattr(tmdb_op, "fetch_tmdb", fake_fetch)
    pages = tmdb_op.fetch_discovery_endpoints(
        ["/movie/popular", "/movie/upcoming", "/trending/movie/day"], log
    )
    assert len(pages) == 2


# ── Simkl ────────────────────────────────────────────────────────

@pytest.mark.parametrize("payload", [{}, None, {"movies": []}, {"movies": [], "shows": []}])
def test_empty_simkl_response_never_deletes_history(monkeypatch, payload):
    cursor = FakeCursor()
    monkeypatch.setattr(simkl_op, "get_conn", lambda: FakeConnection(cursor))
    monkeypatch.setattr(simkl_op, "fetch_simkl_ratings", lambda: payload)

    with pytest.raises(Exception, match="zero rows"):
        list(simkl_op.ingest_simkl_ratings(build_op_context()))

    assert not any(sql.startswith("DELETE") for sql in cursor.executed)


def test_simkl_validation_failure_skips_delete(monkeypatch):
    payload = {
        "movies": [
            {"movie": {"title": "Ok", "ids": {"simkl": 1, "tmdb": 10}}, "status": "completed"},
            {"movie": {"title": "Broken", "ids": {}}, "status": "completed"},  # no simkl id
        ]
    }
    cursor = FakeCursor()
    monkeypatch.setattr(simkl_op, "get_conn", lambda: FakeConnection(cursor))
    monkeypatch.setattr(simkl_op, "fetch_simkl_ratings", lambda: payload)

    list(simkl_op.ingest_simkl_ratings(build_op_context()))

    assert not any(sql.startswith("DELETE") for sql in cursor.executed)


# ── Hardcover ────────────────────────────────────────────────────

def test_hardcover_graphql_error_raises_a_clear_message(monkeypatch):
    monkeypatch.setattr(hardcover_op, "get_conn", lambda: FakeConnection(FakeCursor()))
    monkeypatch.setattr(
        hardcover_op, "fetch_hardcover", lambda q: {"errors": [{"message": "field not found"}]}
    )
    with pytest.raises(Exception, match="GraphQL error from Hardcover"):
        hardcover_op.ingest_hardcover_books(build_op_context())


def test_hardcover_author_skips_translators_and_illustrators():
    book = {"contributions": [
        {"contribution": "Translator", "author": {"name": "Alison Watts"}},
        {"contribution": "Illustrator", "author": {"name": "Rohan Eason"}},
        {"contribution": "Author", "author": {"name": "Michiko Aoyama"}},
    ]}
    assert hardcover_op.get_author(book) == "Michiko Aoyama"
    assert hardcover_op.get_author({"contributions": [{"contribution": None, "author": {"name": "Amish"}}]}) == "Amish"
    assert hardcover_op.get_author({"contributions": []}) is None


# ── Open Library ─────────────────────────────────────────────────

def test_author_match_ignores_accents_and_transliteration():
    assert openlib_op.author_matches("Elif Shafak", ["Elif Şafak"])
    assert openlib_op.author_matches("J.K. Rowling", ["J. K. Rowling"])
    assert not openlib_op.author_matches("Alice Feeney", ["Drew Daywalt"])


class FakeResponse:
    def __init__(self, docs):
        self._docs = docs

    def raise_for_status(self):
        pass

    def json(self):
        return {"docs": self._docs}


def test_openlibrary_falls_back_to_title_and_author_search(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        if "title" in params:
            return FakeResponse([{"key": "/works/OL1W", "author_name": ["川口俊和"]}])
        return FakeResponse([{"key": "/works/OL2W", "author_name": ["川口俊和", "Toshikazu Kawaguchi"]}])

    monkeypatch.setattr(openlib_op.requests, "get", fake_get)
    doc = openlib_op.search_openlibrary("Before the Coffee Gets Cold", "Toshikazu Kawaguchi")
    assert doc["key"] == "/works/OL2W"
    assert calls[1]["q"] == "Before the Coffee Gets Cold Toshikazu Kawaguchi"


def test_discovered_book_uses_its_english_edition_title():
    doc = {
        "title": "... Trotzdem Ja zum Leben sagen",
        "language": ["ger", "eng"],
        "editions": {"docs": [{"title": "Man's Search for Meaning", "language": ["eng"]}]},
    }
    assert openlib_op.english_title(doc) == "Man's Search for Meaning"
    assert openlib_op._is_english(doc)
    assert openlib_op.english_title({"title": "Krew elfów", "language": ["pol", "eng"]}) is None
    assert not openlib_op._is_english({"title": "Krew elfów", "language": ["pol", "eng"]})

