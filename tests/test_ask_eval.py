import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "flask", "eval"))

from questions import CASES  # noqa: E402
from run_ask_eval import score  # noqa: E402


def test_eval_set_has_twenty_unique_read_only_cases():
    assert len(CASES) == 20
    assert len({c["id"] for c in CASES}) == 20
    for case in CASES:
        assert case["match"] in {"count", "top", "set", "subset"}
        assert case["gold"].strip().upper().startswith("SELECT")


def test_count_compares_row_totals():
    assert score("count", [("a",), ("b",)], [("a",), ("b",)], 2)[0]
    assert not score("count", [("a",), ("b",)], [("a",)], 1)[0]


def test_top_accepts_any_tied_leader():
    gold = [("Dune", 10), ("Heat", 10), ("Up", 9)]
    assert score("top", gold, [("Heat", 10)], 1)[0]
    assert not score("top", gold, [("Up", 9)], 1)[0]


def test_top_without_ranking_column_needs_exact_leader():
    gold = [("Drama",), ("Comedy",)]
    assert score("top", gold, [("drama", 42)], 1)[0]
    assert not score("top", gold, [("Comedy", 40)], 1)[0]


@pytest.mark.parametrize("rows, expected", [
    ([("Dune", "movie"), ("Heat", "movie")], True),
    ([("Dune", "movie"), ("Invented Title", "movie")], False),
    ([], False),
])
def test_subset_rejects_titles_outside_gold(rows, expected):
    gold = [("Dune",), ("Heat",), ("Up",)]
    assert score("subset", gold, rows, len(rows))[0] is expected


def test_set_needs_both_sides_of_a_comparison():
    gold = [("movie",), ("tv",)]
    assert score("set", gold, [("movie", 120), ("tv", 40)], 2)[0]
    assert not score("set", gold, [("movie", 120)], 1)[0]
