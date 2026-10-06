import logging

import pytest
import requests
from dagster import build_op_context

from conftest import FakeConnection, FakeCursor
from ops import hardcover_op, simkl_op, tmdb_op

log = logging.getLogger("test")


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
