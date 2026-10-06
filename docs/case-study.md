# Case Study: Personal Content Intelligence Pipeline

## Contents

- [Why I built this](#why-i-built-this)
- [Design choices](#design-choices)
- [Challenges and how I handled them](#challenges-and-how-i-handled-them)
- [Data model](#data-model)
- [What I would do differently](#what-i-would-do-differently)
- [Results](#results)

## Why I built this

Each streaming or tracking app only sees its own slice of what I watch and read:

- Netflix has no idea which book I finished last month.
- Simkl doesn't know how I rated a book on Hardcover.
- Each app's recommendations are tuned to keep me in that app, rather than to my overall taste.

Simple questions, such as "what is my highest-rated genre?" or "do I rate movies more harshly than TV?", were hard to answer anywhere. I wanted one taste profile across movies, TV, and books, and a way to ask it questions in plain English. It also gave me a realistic project to practise end-to-end data engineering on.

## Design choices

Each choice below fits this project's scale: one user, one machine, and a weekly refresh. At a different scale, several of them would change.

### Keep raw data exactly as the source sent it

- **Choice:** raw tables only store fields the API actually returns. Cleaning, decoding, and joins happen later, in dbt.
- **Why:** it keeps a faithful copy of each source, so any transformation can be rebuilt or fixed later.
- **What I learned:** the rule is "did the source send it?", not "is it convenient?". At one point I wrongly removed a join key from raw, before realising Simkl sends that id itself.

### One Postgres database instead of a lakehouse

- **Choice:** a single Postgres instance, backed up daily with `pg_dump`.
- **Why:** with one user and a few hundred titles, a separate object-storage layer (S3 or MinIO) would add work without a clear benefit.
- **Trade-off:** the backups sit on the same machine. If the data or the number of users grew, a raw landing zone in object storage would be the next step.

### dbt for transformations

- **Choice:** dbt models instead of hand-written SQL scripts.
- **Why:** transformations are version-controlled and documented, and they come with tests. The 67 tests (keys, foreign keys, allowed values, rating ranges) caught several issues along the way.

### Dagster for orchestration

- **Choice:** Dagster rather than Airflow or cron.
- **Why:** it models real data dependencies between steps, and shows schedules, run history, and logs in one place. A failed run can be re-executed from the step that failed.

### Local models through Ollama

- **Choice:** `llama3.2:1b` for picks and `llama3.2:3b` for `/ask`, both running locally.
- **Why:** no cost and no reliance on an external API.
- **Trade-off:** inference runs on the CPU, so `/ask` can take from 40 seconds to a few minutes.

### Text-to-SQL rather than retrieval (RAG)

- **Choice:** the model writes SQL, and the answer comes from the query results.
- **Why:** the data is structured. A question like "my highest-rated genre" needs an aggregate query, which similarity search over text can't provide.

### Incremental fact tables

- **Choice:** the two fact tables only process rows that changed, using each source row's `ingested_at` timestamp as the marker.
- **Why:** that timestamp only moves when a value actually changes, so re-ratings and status changes are picked up without rescanning everything. A post-hook removes rows deleted at the source.

### Flask for the web app

- **Choice:** Flask rather than FastAPI or Django.
- **Why:** six routes and one user didn't call for an async framework or an ORM.

## Challenges and how I handled them

### The picks model returned invalid ids

- **Problem:** the model sometimes returned a title id that wasn't in the shortlist it was given.
- **Fix:** every id is checked against the shortlist before it is saved, and a deterministic fallback takes over if the model fails.

### "Apple TV+" broke a content-type check

- **Problem:** `/ask` checks that a question about TV produces a `content_type = 'tv'` filter. "Movies on Apple TV+" matched the word "TV" in the platform name, so valid SQL was rejected.
- **Fix:** the check now ignores "Apple TV".

### Regional titles were filtered out by vote counts

- **Problem:** TMDB vote counts are much higher for English titles. A single minimum of 10 votes reduced the unwatched Marathi titles from 71 to 2.
- **Fix:** a lower minimum for Hindi and Marathi, plus a pass that fills each language's share of the picks.

### Recommendations were quietly losing regional titles

- **Problem:** streaming availability was read for the US, but this is an India-based history. 91% of Marathi and 60% of Hindi titles showed no platform. Picks require one, so those languages had almost no candidates, and nothing raised an error.
- **How I found it:** by counting titles without a platform, broken down by language.
- **Fix:** the region is now a setting, and title details are re-fetched every 30 days so availability stays current.

### Daily soaps and TV serials slipped into the picks

- **Problem:** a variety show with 191 episodes in a single season appeared in the picks. The old check only blocked shows with more than 300 episodes in total.
- **Fix:** shows are now flagged using episodes per season together with vote count, in one column (`is_serial_format`) that both the picks and the replacement logic use. Popular long-running series such as One Piece are not affected.

### A small model copied its own example

- **Problem:** `/ask` answered "highest-rated book subject" with a subject based on only 3 books. The prompt said subjects need at least 5 ratings, but the worked example in the prompt used the creator minimum of 2, and the model followed the example.
- **Fix:** I corrected the example and added a code-level repair that applies the right minimum whenever a query ranks by an average. The answer now matches the taste profile.

### An empty API response could have wiped the watch history

- **Problem:** titles missing from the latest Simkl response were deleted. A successful but empty response would have deleted the entire history.
- **Fix:** deletion is skipped when the response is empty or contains invalid records. A missed deletion is corrected on the next run, while a wrong one would lose data.

### dbt tests didn't run automatically

- **Problem:** a run could finish successfully with test failures in `marts`, and nobody would know.
- **Fix:** a test step now runs before anything uses the marts, and a failure stops the run.

## Data model

- **`dim_watchable`** is the central dimension. Movies and TV are combined into one table, so queries don't depend on which source a title came from.
- **Genres and platforms** relate to titles many-to-many, so they use bridge tables rather than arrays.
- **Watch history and reading history** are separate fact tables, because they come from different sources and have different details.
- **`meta`** is kept separate from `marts`. It holds data the app produces or the user enters (taste profile, picks, dismissed titles, settings), which dbt must not overwrite.

## What I would do differently

- **Use Dagster assets with `dagster-dbt` from the start.** dbt currently runs as one step, which hides per-model lineage and retries. Defining each table as an asset would provide lineage, partial re-runs, and freshness checks.
- **Write Python tests from the first step.** The dbt layer is well tested, but the logic that matters most for quality (id validation, SQL repairs, the delete guard) was checked by hand.
- **Set up CI early.** Linting, unit tests, and `dbt build` against a temporary database on every push.
- **Check assumptions about each source on day one.** Two costly bugs came from defaults I didn't question: a US-only streaming region and a filter that only fetched rated books. A quick breakdown of each source by language and status would have caught both.
- **Use hashed keys for genres and platforms.** The current numeric ids can change when a new value appears. `dbt_utils.generate_surrogate_key` would keep them stable.
- **Build a small evaluation set for `/ask`.** A fixed list of questions with expected answers would make every prompt or model change measurable.

## Results

- **Sources and storage:** 4 source APIs, 7 raw tables, 5 staging views, 10 marts tables, 7 meta tables.
- **Data volume:** about 630 movie and TV titles in the catalogue, with roughly 120 watched titles and 25 read books in the history.
- **Testing:** 67 dbt tests, all passing.
- **Orchestration:** 14 Dagster ops, 9 jobs, a weekly schedule (Fridays at noon IST), and a failure sensor with email alerts.
- **Models:** `llama3.2:1b` for picks and `llama3.2:3b` for `/ask`.
