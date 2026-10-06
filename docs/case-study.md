# Case Study: Personal Content Intelligence Pipeline

## Why I Built This

Every streaming platform only knows what happens inside it. Netflix has no record of a book I finished last month, and Simkl has no record of a Hardcover rating. Recommendations stay siloed inside one app and tuned for its engagement, not my actual taste. Basic questions, like my highest-rated genre or whether I rate movies harder than TV, go unanswered even within one platform. I wanted one taste profile spanning movies, TV, and books, with a way to ask it plain-English questions.

## Architecture Decisions

**Raw stays exactly as the source sent it.** Raw tables hold source fields only, upserted idempotently and stamped with the Dagster run id. Every cleaning step, decode, and join lives in dbt staging or marts. The test for a column in `raw` is "did the source send it", not "is it convenient", which kept the layers honest when a join key (Simkl's own `tmdb` id) turned out to belong in raw after all.

**Single Postgres, not a lakehouse.** I rejected an S3/MinIO raw zone. One reproducible datastore is enough at single-user scale, and a daily `pg_dump` covers recovery. A lakehouse's durability guarantees matter at a scale this project doesn't have yet.

**dbt, not raw SQL scripts.** Typed, testable, version-controlled transforms. 67 schema tests (keys, foreign keys, accepted values, rating ranges) catch regressions a raw script never would.

**Dagster, not Airflow or cron.** Real data-dependency graphs, not just task ordering. One UI covers schedules, run history and logs, and a failed run re-executes from the exact step that failed.

**Ollama (local), not Groq or OpenAI.** Zero cost and no dependency on an external API (`llama3.2:1b` for picks, `llama3.2:3b` for `/ask`), at the cost of slower, CPU-bound inference.

**Text-to-SQL (LangChain + Ollama), not RAG/ChromaDB.** The data is structured, not free text. "My highest-rated genre" needs a real aggregate query, not semantic similarity over embeddings.

**Incremental fact tables, not full refresh.** Keyed on `content_id`, with the source row's `ingested_at` as the watermark. That timestamp only moves when a value really changes, so re-ratings and status changes are picked up without rescanning everything. A post-hook removes rows deleted at the source.

**Flask, not FastAPI or Django.** Six routes and one user. Nothing here would justify an async runtime or an ORM.

## What Was Hard

**Invalid content_ids from Ollama.** The picks model sometimes returned an id that wasn't in its own shortlist. The fix: validate every id before it reaches the database, with a deterministic fallback.

**"Apple TV+" breaking a content-type filter.** `/ask`'s SQL guard checks that a "TV" question produced a `content_type='tv'` filter. "Movies on Apple TV+" matched `\btv\b` against the platform name, so valid SQL was rejected. Fixed with a negative lookbehind that excludes "Apple TV" specifically.

**Language and type balance in picks.** TMDB's vote counts skew English, so a global `vote_count >= 10` floor gutted the regional pool (Marathi went from 71 unwatched titles to 2). Fixed with a lower floor for `hi`/`mr` plus a rebalancing pass that enforces per-language slot targets.

**Recommendations quietly losing regional titles.** Streaming availability was read from TMDB's US region, but this is an India-based watch history. 91% of Marathi and 60% of Hindi titles showed no platform, and picks require one, so those pools were nearly empty, with no error anywhere. I found it by profiling missing platforms by language. The fix made the region a user setting and re-fetches details older than 30 days, so availability can't freeze at first fetch either.

**A small model copying its own example.** `/ask` ranked "highest rated book subject" by averaging over just 3 books. The prompt's rules said subjects need at least 5 ratings, but the worked example for that exact question used the creator floor of 2, and the model copied the example over the rule. Fixing the example wasn't enough on its own, so a deterministic repair now adds or raises the minimum-count floor whenever a query ranks by an average. The answer now matches the taste profile the rest of the app uses.

**An empty API response that could wipe history.** Simkl delete reconciliation treated "not in this response" as "removed from my Simkl list". An empty-but-successful response would have matched every row and deleted the whole watch history before the zero-row check raised. Reconciliation now skips any content type that came back empty or had validation failures. A missed delete heals on the next run; a wrong one loses data.

**dbt tests never ran automatically.** A run could succeed with schema violations sitting in `marts`, invisible until someone ran `dbt test` by hand. Fixed by putting a test step in front of every downstream consumer.

## Data Model

`dim_watchable` is the spine: movies and TV conformed into one dimension, so no query cares which source an id came from. Genres and platforms are many-to-many, so they use bridge tables rather than arrays. Watch and reading history are separate fact tables because they have a different grain and a different source. `meta` is split from `marts`: mutable, Dagster-written state (taste profile, recommendations, dismissals) versus dbt-owned dimensional data.

## What I'd Do Differently

- **Start with software-defined assets and `dagster-dbt`.** Running dbt as one subprocess op hides per-model lineage and retries inside a single step. Modelling each table as a Dagster asset from day one would give lineage, partial re-runs, and freshness checks for free.
- **Write Python tests from the first op.** The dbt layer ended up well tested, but the logic that matters most for quality (id validation, SQL repairs, the delete guard) was verified by hand. Unit tests would have caught regressions sooner and made refactors cheaper.
- **Add CI before the second contributor, not after.** Linting, unit tests, and `dbt build` against a throwaway Postgres on every push.
- **Profile assumptions about sources early.** Two of the most expensive bugs came from defaults nobody questioned: a hardcoded US streaming region and a rated-only book filter. A quick per-language and per-status profile of each source on day one would have caught both.
- **Use hashed surrogate keys.** `row_number()` ids for genres and platforms shift when a new value appears; `dbt_utils.generate_surrogate_key` keeps them stable for anything that stores them.
- **Build a small eval set for `/ask`.** A fixed list of questions with expected rows turns every prompt or model change into a measurable comparison instead of a hand check.

## Results

- 4 source APIs, 7 raw tables, 5 dbt staging views, 10 marts tables, 7 meta tables
- About 630 movie and TV titles in the catalogue, 118 watched titles and 25 read books in the history
- 67 dbt tests, all passing
- 14 Dagster ops, 9 jobs
- 1 schedule (weekly, Friday noon IST) and 1 run-failure sensor with email alerts
- 2 Ollama models: `llama3.2:1b` (picks), `llama3.2:3b` (`/ask`)
