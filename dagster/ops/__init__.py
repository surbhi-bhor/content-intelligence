from dagster import Backoff, RetryPolicy

# For ops whose main work is calls to an outside API. Without it, one dropped
# connection (this network drops TLS connections to TMDB often) failed the op,
# and Dagster then skipped everything downstream: no dbt run, no taste
# profile, no picks for the week. Every op using this upserts, so a retry
# only repeats writes that already landed. Waits 1 then 2 minutes.
NETWORK_RETRY = RetryPolicy(max_retries=2, delay=60, backoff=Backoff.EXPONENTIAL)
