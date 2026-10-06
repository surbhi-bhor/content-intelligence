# Contributing

## Contents

- [Prerequisites](#prerequisites)
- [Conventions](#conventions)
- [Adding a new data source](#adding-a-new-data-source)
- [Adding a new dbt model](#adding-a-new-dbt-model)
- [Running tests](#running-tests)
- [Viewing dbt docs and lineage](#viewing-dbt-docs-and-lineage)

## Prerequisites

- Docker Desktop.
- API keys for TMDB, Simkl, and Hardcover. OpenLibrary needs no key.
- A running stack, set up with the README's [Quick start](README.md#quick-start). It covers `.env`, `init_db.py`, `dbt deps`, and the two `ollama pull` commands.

## Conventions

- **Raw holds what the source sent.** Raw tables only store fields that appear in the API response. Cleaning, decoding, joins, and derived columns belong in dbt.
- **Loads are idempotent.** Raw writes are upserts (`INSERT ... ON CONFLICT`). Every row records the run that wrote it in `pipeline_run_id`, and `ingested_at` only changes when a value actually changes.
- **Validate before inserting.** Each source has a Pydantic model. Records that fail validation are logged and skipped.
- **Fail clearly on empty sources.** Ingestion steps fail when an API returns no rows, and warn when it returns fewer than expected.
- **Display text is English.** Titles, names, genres, platforms, and subjects shown to the user must be in English. Use the `latin_or_null` and `is_non_english_subject` macros in `dbt/macros/english_text.sql` for new text columns; `dbt/tests/assert_display_text_is_english.sql` enforces it.
- **Settings are data.** Recommendation settings (languages, language slots, region, excluded genres) live in `meta.user_config`, not in code.

## Adding a new data source

1. Create `dagster/ops/<source>_op.py`, following `tmdb_op.py` or `simkl_op.py`. It needs:
   - a Pydantic model for the raw record,
   - a `fetch_*` helper for the API call,
   - an `@op` that upserts into a new `raw.<table>`, sets `pipeline_run_id`, and checks the row count.
2. Add a `CREATE TABLE IF NOT EXISTS` statement for the table to `dagster/init_db.py`.
3. Import the op in `dagster/pipeline.py` and add it to `full_ingestion_job`. Wire its real dependencies so it finishes before `run_dbt_transformations`.
4. Add the table to `dbt/models/sources.yml` with `not_null` and `unique` tests on its key columns. It picks up the shared freshness checks automatically.

## Adding a new dbt model

1. Write the SQL:
   - in `dbt/models/staging/` for a cleaned, typed view of one source, or
   - in `dbt/models/marts/` for joins and aggregates built on staging models.
2. Document it in the matching `schema.yml`:
   - a model description and column descriptions,
   - `not_null` and `unique` tests on the primary key,
   - `relationships` tests on every foreign key.
3. Run `dbt build --select <model>+` to build the model and everything that depends on it, including tests.

## Running tests

**Unit tests and lint** run on the host, with no database or containers needed:

```bash
py -3.12 -m venv .venv               # Python 3.12, same as Docker and CI; `python3.12 -m venv .venv` on macOS/Linux
.venv/Scripts/activate              # Windows; use `source .venv/bin/activate` on macOS/Linux
pip install -r requirements.txt -r requirements-dev.txt
ruff check dagster flask tests      # lint (real defects only, see ruff.toml)
pytest -q                           # unit tests
```

**dbt tests** run against the live database in the stack:

```bash
docker compose exec -w /opt/dbt dagster-code dbt deps               # first time only
docker compose exec -w /opt/dbt dagster-code dbt build              # all models and tests
docker compose exec -w /opt/dbt dagster-code dbt source freshness   # how recent the raw data is
```

**CI** (`.github/workflows/ci.yml`) runs the lint, the unit tests, and `dbt parse` on every push to `main` and on pull requests.

Any job can also be started by hand from the Dagster UI at `localhost:3000`.

## Viewing dbt docs and lineage

The generated docs (`manifest.json`, `catalog.json`, `index.html`) are not committed. To build and view them:

```bash
docker compose exec -w /opt/dbt dagster-code dbt docs generate
docker cp content_dagster_code:/opt/dbt/target/manifest.json docs/manifest.json
docker cp content_dagster_code:/opt/dbt/target/catalog.json docs/catalog.json
docker cp content_dagster_code:/opt/dbt/target/index.html docs/index.html
python -m http.server 8080 --directory docs
```

Then open `localhost:8080`.
