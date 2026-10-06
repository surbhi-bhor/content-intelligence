import re
import subprocess
from dagster import Definitions, job, op, get_dagster_logger, ScheduleDefinition, DefaultScheduleStatus
from ops.tmdb_op import ingest_tmdb_movies, ingest_tmdb_details, ingest_tmdb_shows, ingest_tmdb_tv_details
from ops.hardcover_op import ingest_hardcover_books
from ops.openlib_op import enrich_book_metadata, discover_openlibrary_books
from ops.simkl_op import ingest_simkl_ratings
from ops.taste_profile_op import build_taste_profile
from ops.recommendation_op import generate_recommendations
from sensors import pipeline_failure_alert

@op
def merge_movie_ids(context, tmdb_ids: list, simkl_ids: list):
    return list(set(tmdb_ids) | set(simkl_ids))

@op
def merge_show_ids(context, tmdb_ids: list, simkl_ids: list):
    return list(set(tmdb_ids) | set(simkl_ids))

@op
def run_dbt_transformations(context, movie_details=None, tv_details=None, book_metadata=None):
    log = get_dagster_logger()
    result = subprocess.run(
        ["dbt", "run"],
        cwd="/opt/dbt",
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"dbt run failed with exit code {result.returncode}")
    return result.returncode

@op
def run_dbt_tests_op(context, start=None):
    """dbt tests never ran automatically before this - a pipeline run could
    succeed end to end with real schema violations sitting in marts, with no
    visibility into it until someone happened to run `dbt test` by hand.
    Placed after each dbt run and before the ops that consume marts data
    (taste_profile, recommendations) so bad data is caught before it feeds
    either, not after."""
    log = get_dagster_logger()
    result = subprocess.run(
        ["dbt", "test"],
        cwd="/opt/dbt",
        capture_output=True,
        text=True,
    )
    stdout = result.stdout
    failed_tests = re.findall(r"FAIL \d+\s+(\S+)", stdout)
    total_match = re.search(r"TOTAL=(\d+)", stdout)
    pass_match = re.search(r"PASS=(\d+)", stdout)
    total = total_match.group(1) if total_match else "?"

    if result.returncode != 0 or failed_tests:
        log.error(f"[DBT TESTS] {len(failed_tests)} FAILED:")
        for name in failed_tests:
            log.error(f"  - {name}")
        if result.stderr:
            log.error(result.stderr)
        raise Exception(f"dbt test failed: {len(failed_tests)} of {total} tests failed")

    passed = pass_match.group(1) if pass_match else total
    log.info(f"[DBT TESTS] Running {total} tests...")
    log.info(f"[DBT TESTS] {passed} passed, 0 failed ✓")
    return result.returncode

@job
def tmdb_ingestion_job():
    ingest_tmdb_movies()

@job
def tmdb_tv_ingestion_job():
    ingest_tmdb_shows()

@job
def hardcover_ingestion_job():
    ingest_hardcover_books()

@job
def openlib_enrichment_job():
    enrich_book_metadata()

@job
def simkl_ingestion_job():
    ingest_simkl_ratings()

@job
def taste_profile_job():
    build_taste_profile()

@job
def book_discovery_job():
    # Seeds its searches from the subjects and authors of highly rated
    # books in marts.fact_reading_history.
    # The dbt run at the end is required, not optional: it's what turns the
    # newly inserted raw.raw_books rows into real marts.dim_book
    # candidates the recommendation logic can actually select from.
    discovery_result = discover_openlibrary_books()
    run_dbt_transformations(book_metadata=discovery_result)

@job
def recommendation_job():
    generate_recommendations()

@job
def full_ingestion_job():
    tmdb_movie_ids = ingest_tmdb_movies()
    tmdb_show_ids = ingest_tmdb_shows()
    simkl_movie_ids, simkl_show_ids = ingest_simkl_ratings()

    all_movie_ids = merge_movie_ids(tmdb_movie_ids, simkl_movie_ids)
    movie_details = ingest_tmdb_details(all_movie_ids)

    all_show_ids = merge_show_ids(tmdb_show_ids, simkl_show_ids)
    tv_details = ingest_tmdb_tv_details(all_show_ids)

    hardcover_result = ingest_hardcover_books()
    book_metadata = enrich_book_metadata(start=hardcover_result)

    dbt_result = run_dbt_transformations(movie_details, tv_details, book_metadata)
    test_result = run_dbt_tests_op(start=dbt_result)
    profile_result = build_taste_profile(start=test_result)

    # Book discovery reads the freshly built reading history (to know which
    # subjects and authors to search) and needs its own dbt run afterward - the
    # newly discovered raw.raw_books rows aren't real marts.dim_book
    # candidates until dbt materializes them, so generate_recommendations
    # must wait for THIS dbt run, not the earlier one.
    discovery_result = discover_openlibrary_books(start=profile_result)
    discovery_dbt_result = run_dbt_transformations.alias("materialize_discovered_books")(
        book_metadata=discovery_result
    )
    discovery_test_result = run_dbt_tests_op.alias("run_dbt_tests_op_2")(start=discovery_dbt_result)
    generate_recommendations(start=discovery_test_result)

weekly_pipeline_schedule = ScheduleDefinition(
    name="weekly_pipeline_schedule",
    job=full_ingestion_job,
    cron_schedule="0 12 * * 5",
    execution_timezone="Asia/Kolkata",
    description="Runs full ingestion → dbt → taste profile → recommendations every Friday at noon — weekend-recommendation cadence, not daily, since watch/read history doesn't turn over fast enough to need a nightly refresh",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    jobs=[
        tmdb_ingestion_job,
        tmdb_tv_ingestion_job,
        hardcover_ingestion_job,
        openlib_enrichment_job,
        book_discovery_job,
        simkl_ingestion_job,
        taste_profile_job,
        recommendation_job,
        full_ingestion_job,
    ],
    schedules=[weekly_pipeline_schedule],
    sensors=[pipeline_failure_alert],
)
