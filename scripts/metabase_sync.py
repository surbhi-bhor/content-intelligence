"""Export the Metabase dashboard to JSON in the repo, or recreate it from that JSON.

Metabase keeps questions and dashboards in its own app database, not in
files, so a fresh install starts empty. This script round-trips them through
metabase/<dashboard>.json:

    python scripts/metabase_sync.py export                # Metabase -> repo
    python scripts/metabase_sync.py import                # repo -> Metabase
    python scripts/metabase_sync.py import --collection "Imported"

Reads MB_URL (default http://localhost:4000) and either MB_API_KEY or
MB_EMAIL + MB_PASSWORD from the environment or .env.

Internal ids differ between installs, so the export stores the database by
name and every field reference by schema/table/column, and the import maps
them back to the ids of the target Metabase.
"""
import argparse
import json
import os
import re
import sys

import requests
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(ROOT, "metabase")
DEFAULT_DASHBOARD = "My Content Analytics"

CARD_KEYS = ("name", "description", "display", "type", "dataset_query", "visualization_settings")
DASHCARD_KEYS = ("row", "col", "size_x", "size_y", "visualization_settings")


def _session() -> tuple[requests.Session, str]:
    load_dotenv(os.path.join(ROOT, ".env"))
    url = os.getenv("MB_URL", "http://localhost:4000").rstrip("/")
    s = requests.Session()
    if os.getenv("MB_API_KEY"):
        s.headers["x-api-key"] = os.environ["MB_API_KEY"]
    elif os.getenv("MB_EMAIL") and os.getenv("MB_PASSWORD"):
        r = s.post(f"{url}/api/session", json={"username": os.environ["MB_EMAIL"], "password": os.environ["MB_PASSWORD"]})
        r.raise_for_status()
        s.headers["X-Metabase-Session"] = r.json()["id"]
    else:
        sys.exit("Set MB_API_KEY, or MB_EMAIL and MB_PASSWORD, in .env")
    return s, url


def _get(s, url, path):
    r = s.get(f"{url}/api{path}")
    r.raise_for_status()
    return r.json()


def _send(s, method, url, path, body):
    r = s.request(method, f"{url}/api{path}", json=body)
    if not r.ok:
        sys.exit(f"{method} {path} failed: {r.status_code} {r.text[:500]}")
    return r.json()


def _map_fields(node, fn):
    """Apply fn to the id in every field reference. Field refs are lists like
    ["field", {opts}, 317] (MBQL 5) or ["field", 317, {opts}] (legacy)."""
    if isinstance(node, list):
        if len(node) == 3 and node[0] == "field":
            return [
                "field",
                *(fn(x) if isinstance(x, (int, dict)) and _is_field_id(x) else _map_fields(x, fn) for x in node[1:]),
            ]
        return [_map_fields(x, fn) for x in node]
    if isinstance(node, dict):
        return {k: _map_fields(v, fn) for k, v in node.items() if k != "lib/uuid"}
    return node


def _is_field_id(x) -> bool:
    return isinstance(x, int) or (isinstance(x, dict) and set(x) == {"schema", "table", "column"})


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


# ── Export ──────────────────────────────────────────────────────

def export(dashboard_name: str):
    s, url = _session()
    matches = [d for d in _get(s, url, "/dashboard/") if d["name"] == dashboard_name and not d.get("archived")]
    if not matches:
        sys.exit(f"No dashboard named {dashboard_name!r}")
    dash = _get(s, url, f"/dashboard/{matches[0]['id']}")

    field_cache = {}

    def field_ref(field_id):
        if field_id not in field_cache:
            f = _get(s, url, f"/field/{field_id}")
            field_cache[field_id] = {"schema": f["table"]["schema"], "table": f["table"]["name"], "column": f["name"]}
        return field_cache[field_id]

    db_names = {db["id"]: db["name"] for db in _get(s, url, "/database")["data"]}
    cards = {}
    for dc in dash["dashcards"]:
        if dc["card_id"] and dc["card_id"] not in cards:
            card = _get(s, url, f"/card/{dc['card_id']}")
            exported = {k: card.get(k) for k in CARD_KEYS}
            exported["dataset_query"] = _map_fields(card["dataset_query"], field_ref)
            exported["dataset_query"]["database"] = db_names[card["dataset_query"]["database"]]
            cards[dc["card_id"]] = exported

    tab_index = {t["id"]: i for i, t in enumerate(sorted(dash["tabs"], key=lambda t: t["position"]))}
    card_index = {cid: i for i, cid in enumerate(cards)}
    out = {
        "name": dash["name"],
        "description": dash.get("description"),
        "parameters": dash["parameters"],
        "tabs": [t["name"] for t in sorted(dash["tabs"], key=lambda t: t["position"])],
        "cards": list(cards.values()),
        "dashcards": [
            {
                **{k: dc.get(k) for k in DASHCARD_KEYS},
                "tab": tab_index.get(dc["dashboard_tab_id"]),
                "card": card_index.get(dc["card_id"]),
                "parameter_mappings": [
                    {"parameter_id": m["parameter_id"], "target": m["target"]} for m in dc["parameter_mappings"]
                ],
            }
            for dc in sorted(dash["dashcards"], key=lambda d: (tab_index.get(d["dashboard_tab_id"], 0), d["row"], d["col"]))
        ],
    }
    os.makedirs(EXPORT_DIR, exist_ok=True)
    path = os.path.join(EXPORT_DIR, f"{_slug(dash['name'])}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Exported {len(out['cards'])} questions and {len(out['dashcards'])} dashboard cards to {os.path.relpath(path, ROOT)}")


# ── Import ──────────────────────────────────────────────────────

def import_(dashboard_name: str, collection: str | None):
    with open(os.path.join(EXPORT_DIR, f"{_slug(dashboard_name)}.json"), encoding="utf-8") as f:
        spec = json.load(f)
    s, url = _session()

    collection_id = None
    if collection:
        collection_id = _send(s, "POST", url, "/collection", {"name": collection})["id"]

    dbs = {db["name"]: db["id"] for db in _get(s, url, "/database")["data"]}
    fields_by_db = {}

    def resolve(card):
        db_name = card["dataset_query"]["database"]
        if db_name not in dbs:
            sys.exit(f"Metabase has no database named {db_name!r}. Add the Postgres connection under that name first.")
        db_id = dbs[db_name]
        if db_id not in fields_by_db:
            meta = _get(s, url, f"/database/{db_id}/metadata")
            fields_by_db[db_id] = {
                (t["schema"], t["name"], fl["name"]): fl["id"] for t in meta["tables"] for fl in t["fields"]
            }

        def field_id(ref):
            key = (ref["schema"], ref["table"], ref["column"])
            if key not in fields_by_db[db_id]:
                sys.exit(f"Field {'.'.join(key)} not found. Run dbt and sync the database schema in Metabase first.")
            return fields_by_db[db_id][key]

        query = _map_fields(card["dataset_query"], field_id)
        query["database"] = db_id
        return query

    card_ids = []
    for card in spec["cards"]:
        body = {**card, "dataset_query": resolve(card), "collection_id": collection_id}
        card_ids.append(_send(s, "POST", url, "/card", body)["id"])

    dash = _send(s, "POST", url, "/dashboard", {
        "name": spec["name"], "description": spec["description"],
        "parameters": spec["parameters"], "collection_id": collection_id,
    })
    tabs = [{"id": -(i + 1), "name": name} for i, name in enumerate(spec["tabs"])]
    dashcards = []
    for i, dc in enumerate(spec["dashcards"]):
        card_id = card_ids[dc["card"]] if dc["card"] is not None else None
        dashcards.append({
            **{k: dc[k] for k in DASHCARD_KEYS},
            "id": -(i + 1),
            "card_id": card_id,
            "dashboard_tab_id": -(dc["tab"] + 1) if dc["tab"] is not None and tabs else None,
            "parameter_mappings": [{**m, "card_id": card_id} for m in dc["parameter_mappings"]],
            "series": [],
        })
    _send(s, "PUT", url, f"/dashboard/{dash['id']}", {"tabs": tabs, "dashcards": dashcards})
    print(f"Created dashboard {spec['name']!r} ({len(card_ids)} questions): {url}/dashboard/{dash['id']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=["export", "import"])
    parser.add_argument("--dashboard", default=DEFAULT_DASHBOARD)
    parser.add_argument("--collection", help="import only: put the new questions and dashboard in a new collection")
    args = parser.parse_args()
    if args.action == "export":
        export(args.dashboard)
    else:
        import_(args.dashboard, args.collection)


if __name__ == "__main__":
    main()
