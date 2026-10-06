# Architecture

```
┌───────────────────────── SOURCES ──────────────────────────┐
│  TMDB API · Simkl API · Hardcover API · OpenLibrary API    │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌───────────────── INGESTION (Dagster ops) ──────────────────┐
│  tmdb_op.py      ──► raw.raw_movies, raw.raw_movie_details │
│                      raw.raw_tv, raw.raw_tv_details        │
│  simkl_op.py     ──► raw.raw_ratings                       │
│  hardcover_op.py ──► raw.raw_book_ratings                  │
│  openlib_op.py   ──► raw.raw_books (enrich + discover)     │
│                                                            │
│  Pydantic validation per record                            │
│  Row-count guards: raise on 0 rows, warn below minimum     │
└──────────────────────────────┬─────────────────────────────┘
                               │  dbt run, then dbt test
                               ▼
┌───────────────────── TRANSFORM (dbt) ──────────────────────┐
│  staging (5 views)                                         │
│    stg_movies · stg_tv · stg_ratings                       │
│    stg_books · stg_book_ratings                            │
│          │                                                 │
│          ▼                                                 │
│  marts (10 tables, indexed)                                │
│    dim_watchable ◄── movies + TV, conformed                │
│    dim_book ◄── Hardcover + OpenLibrary, conformed         │
│    dim_genre · dim_platform · dim_book_subject             │
│    bridge_content_genre · bridge_content_platform          │
│    bridge_content_book_subject                             │
│    fact_watch_history · fact_reading_history               │
│      (incremental on the source ingested_at)               │
│                                                            │
│  67 schema tests gate everything downstream                │
│  Source freshness: warn 8 days, error 15 days              │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌──────────────────── META (Dagster ops) ────────────────────┐
│  taste_profile_op.py                                       │
│    ──► meta.taste_profile                                  │
│        (genre/creator avg + count, watch vs. read)         │
│                                                            │
│  recommendation_op.py                                      │
│    ──► meta.daily_recommendations_watch  (10 picks)        │
│    ──► meta.daily_recommendations_books  (5 picks)         │
│        (scored candidates + language/type balancing)       │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌───────────────── AI LAYER (local Ollama) ──────────────────┐
│  llama3.2:1b ──► picks selection                           │
│    constrained: chooses from a pre-scored shortlist,       │
│    every id validated, deterministic fallback              │
│                                                            │
│  llama3.2:3b ──► /ask text-to-SQL                          │
│    runs each candidate query on a read-only role,          │
│    answers only from the rows it returns                   │
└──────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
┌───────────────────────── SERVING ──────────────────────────┐
│  Flask :5000                                               │
│    /  picks page + ask bar    /tonight ──► /               │
│    /ask · /not-interested · /health · /usage               │
│  Metabase :4000 ──► BI dashboards over content_db          │
│    (app DB persisted on the metabase_data volume)          │
└────────────────────────────────────────────────────────────┘

┌────────────────────── ORCHESTRATION ───────────────────────┐
│  Dagster :3000 (webserver) + dagster-daemon                │
│  9 jobs, incl. full_ingestion_job (the whole chain above)  │
│  weekly_pipeline_schedule: Fri 12:00 IST (0 12 * * 5),     │
│    ships RUNNING                                           │
│  pipeline_failure_alert sensor ──► email alert +           │
│    meta.pipeline_alerts (/health goes red)                 │
└────────────────────────────────────────────────────────────┘

Postgres 15 (content_db) holds all pipeline data underneath every box above.
Dagster keeps its own run history in SQLite and Metabase its app DB in H2.
A postgres-backup sidecar pg_dumps it daily to ./backups/postgres (last 14 kept).
```

## Services

All eight run from one `docker-compose.yml`.

| Service | Port | Role |
| --- | --- | --- |
| `postgres` | 5432 | Postgres 15, database `content_db`: `raw`, `staging`, `marts`, and `meta` schemas |
| `dagster-code` | (internal gRPC) | Code server holding the jobs, ops, schedule, and sensor; also runs dbt as a subprocess |
| `dagster` | 3000 | Dagster webserver (UI, run launching, run history) |
| `dagster-daemon` | none | Fires the weekly schedule and the run-failure sensor |
| `ollama` | 11434 | Local LLM inference; keeps both models loaded (`OLLAMA_MAX_LOADED_MODELS=2`, no idle unload) |
| `flask` | 5000 | Picks page, `/ask`, `/not-interested`, `/health`, `/usage` |
| `metabase` | 4000 | BI dashboards over `content_db` |
| `postgres-backup` | none | Daily `pg_dump` of `content_db` to `./backups/postgres`, last 14 kept |

## Weekly run: `full_ingestion_job`

Ops run in dependency order; independent branches run in parallel.

1. **Ingest:** `ingest_tmdb_movies`, `ingest_tmdb_shows`, `ingest_simkl_ratings`, and `ingest_hardcover_books` run in parallel.
2. **Merge ids:** TMDB discovery ids and Simkl watched ids are merged per content type (`merge_movie_ids`, `merge_show_ids`).
3. **Enrich:** `ingest_tmdb_details` and `ingest_tmdb_tv_details` fetch details and streaming platforms for new titles and for titles older than 30 days; `enrich_book_metadata` matches library books to OpenLibrary.
4. **Transform and test:** `run_dbt_transformations` (`dbt run`), then `run_dbt_tests_op` (`dbt test`). A test failure fails the run here.
5. **Taste profile:** `build_taste_profile` appends a new row to `meta.taste_profile`.
6. **Book discovery:** `discover_openlibrary_books` searches OpenLibrary by top subjects and authors, followed by a second `dbt run` and `dbt test` so new books become candidates.
7. **Recommendations:** `generate_recommendations` replaces `meta.daily_recommendations_watch` (10 picks) and `meta.daily_recommendations_books` (5 picks).

The other eight jobs run single steps of this chain on demand (for example `recommendation_job` or `simkl_ingestion_job`).

## Where state lives

| State | Location | Written by |
| --- | --- | --- |
| Source data as received | `raw.*` tables | Ingestion ops (upserts stamped with `pipeline_run_id`) |
| Cleaned, typed data | `staging.*` views | dbt |
| Dimensions, bridges, facts | `marts.*` tables | dbt (facts are incremental) |
| Taste profile, picks, dismissals, settings, alerts | `meta.*` tables | Dagster ops, Flask (`not_interested`), and the failure sensor |
| Dagster run history and schedule state | SQLite on the `dagster_home` volume | Dagster |
| Metabase questions and dashboards | H2 on the `metabase_data` volume | Metabase |
| Model weights | `ollama_data` volume | Ollama |
| Backups | `./backups/postgres` on the host | `postgres-backup` |
