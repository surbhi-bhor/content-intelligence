"""Score /ask against the gold queries in questions.py.

Runs inside the Flask container, which already has the database role,
Ollama, and the agent's dependencies:

    docker compose exec flask python eval/run_ask_eval.py
    docker compose exec flask python eval/run_ask_eval.py --only top_rated_movie

Only the SQL half of the agent is scored (agent.find_result); the wording of
the final answer is not. Results are printed as a table and written to
eval/results.json.
"""
import argparse
import json
import os
import sys
import time
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import agent  # noqa: E402
from questions import CASES  # noqa: E402

RESULTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json")


def _norm(value) -> str:
    if isinstance(value, (float, Decimal)):
        return f"{float(value):.2f}"
    return str(value).strip().lower()


def _run_gold(sql: str) -> list:
    conn = agent._ask_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _contains(row, value) -> bool:
    return _norm(value) in {_norm(cell) for cell in row}


def score(match: str, gold: list, rows: list, total_count: int) -> tuple[bool, str]:
    if match == "count":
        return total_count == len(gold), f"got {total_count}, expected {len(gold)}"

    if not rows:
        return False, "no rows returned"
    gold_values = [g[0] for g in gold]

    if match == "top":
        if not gold:
            return False, "gold query returned no rows"
        if len(gold[0]) < 2:
            accepted = [gold[0][0]]
        else:
            accepted = [g[0] for g in gold if _norm(g[1]) == _norm(gold[0][1])]
        ok = any(_contains(rows[0], v) for v in accepted)
        return ok, f"got {rows[0]!r}, expected one of {accepted[:5]!r}"

    unmatched = [r for r in rows if not any(_contains(r, v) for v in gold_values)]
    if match == "subset":
        return not unmatched, f"{len(unmatched)} of {len(rows)} rows not in gold"
    if match == "set":
        missing = [v for v in gold_values if not any(_contains(r, v) for r in rows)]
        return not unmatched and not missing, f"missing {missing!r}, extra {len(unmatched)}"
    raise ValueError(f"unknown match mode {match!r}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", help="case ids to run")
    args = parser.parse_args()
    cases = [c for c in CASES if not args.only or c["id"] in args.only]

    results = []
    for case in cases:
        gold = _run_gold(case["gold"])
        tokens = []
        started = time.monotonic()
        found = agent.find_result(case["question"], tokens)
        elapsed = time.monotonic() - started
        if found is None:
            ok, detail, sql = False, "every SQL candidate failed", None
        else:
            columns, rows, total_count, sql = found
            ok, detail = score(case["match"], gold, rows, total_count)
        results.append({
            "id": case["id"], "question": case["question"], "match": case["match"],
            "passed": ok, "detail": detail, "sql": sql,
            "seconds": round(elapsed, 1), "tokens": sum(tokens),
        })
        print(f"{'PASS' if ok else 'FAIL'}  {case['id']:<28} {elapsed:6.1f}s  {detail}", flush=True)

    passed = sum(r["passed"] for r in results)
    print(f"\n{passed}/{len(results)} passed ({passed / len(results):.0%})")
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "model": agent.OLLAMA_MODEL,
            "run_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "passed": passed, "total": len(results), "cases": results,
        }, f, indent=2, default=str)


if __name__ == "__main__":
    main()
