# Contributing

## Prerequisites

Docker Desktop, plus API keys for TMDB, Simkl, and Hardcover (OpenLibrary needs none). Set up the stack with the README's [Quick start](README.md#quick-start): it covers `.env`, `init_db.py`, `dbt deps`, and the two `ollama pull` commands.

## Conventions

- **Raw holds what the source sent.** Raw tables only get fields that appear in the API response. Decoding, cleaning, joins, and derived columns belong in dbt staging or marts.
- **Loads are idempotent.** Raw writes are `INSERT ... ON CONFLICT` upserts, every row is stamped with `pipeline_run_id` (`context.run_id`), and `ingested_at` only moves when a value actually changes.
- **Validate before insert.** Each source has a Pydantic model; records that fail validation are logged and skipped, never inserted.
- **Fail loudly on empty sources.** Ingestion ops raise on a zero-row API response and warn below an expected minimum.
- **Settings are data.** Tunable recommendation behaviour (languages, slots, region, excluded genres) lives in `meta.user_config`, not in code.

## Adding a new data source

1. Create `dagster/ops/<source>_op.py` following `tmdb_op.py` or `simkl_op.py`: a Pydantic model for the raw shape, a `fetch_*` helper, and an `@op` that upserts into a new `raw.<table>` with `pipeline_run_id` and a row-count guard.
2. Add the `CREATE TABLE IF NOT EXISTS` for that table to `dagster/init_db.py`.
3. Import the op in `dagster/pipeline.py`, add it to `full_ingestion_job`, and wire its real data dependencies (it must finish before `run_dbt_transformations`).
4. Add the table to `dbt/models/sources.yml` with `not_null`/`unique` tests on its key columns; it inherits the schema's freshness thresholds.

## Adding a new dbt model

1. Write SQL in `dbt/models/staging/` (one source, cleaned and typed) or `dbt/models/marts/` (joins and aggregates over staging).
2. Document it in the matching `schema.yml`: a model description, column descriptions, `not_null` and `unique` on the primary key, and `relationships` tests on every foreign key.
3. Run `dbt build --select <model>+` to build it and everything downstream, with tests.

## Running tests

```bash
docker compose exec -w /opt/dbt dagster-code dbt deps    # first time only
docker compose exec -w /opt/dbt dagster-code dbt build   # all models + all tests
docker compose exec -w /opt/dbt dagster-code dbt source freshness
```

Any job can also be launched by hand from the Dagster UI at `localhost:3000`.

## Viewing dbt docs and lineage

Generated docs (`manifest.json`, `catalog.json`, `index.html`) are gitignored, not committed:

```bash
docker compose exec -w /opt/dbt dagster-code dbt docs generate
docker cp content_dagster_code:/opt/dbt/target/manifest.json docs/manifest.json
docker cp content_dagster_code:/opt/dbt/target/catalog.json docs/catalog.json
docker cp content_dagster_code:/opt/dbt/target/index.html docs/index.html
python -m http.server 8080 --directory docs
```

Then open `localhost:8080`.
