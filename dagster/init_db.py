import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # raw schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # raw movies from TMDB daily discovery
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_movies (
            tmdb_id           INTEGER,
            title             TEXT,
            release_date      DATE,
            popularity        FLOAT,
            vote_average      FLOAT,
            vote_count        INTEGER,
            original_language TEXT,
            overview          TEXT,
            genre_ids         JSONB,
            ingested_at_date  DATE DEFAULT CURRENT_DATE,
            ingested_at       TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (tmdb_id, ingested_at_date)
        );
    """)
    cur.execute("ALTER TABLE raw.raw_movies ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # enriched movie details
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_movie_details (
            tmdb_id           INTEGER PRIMARY KEY,
            title             TEXT,
            original_language TEXT,
            runtime_mins      INTEGER,
            budget            BIGINT,
            revenue           BIGINT,
            director          TEXT,
            genres            JSONB,
            platforms         JSONB,
            poster_path       TEXT,
            ingested_at       TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("ALTER TABLE raw.raw_movie_details ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # raw TV shows from TMDB daily discovery
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_tv (
            tmdb_id           INTEGER,
            title             TEXT,
            first_air_date    DATE,
            popularity        FLOAT,
            vote_average      FLOAT,
            vote_count        INTEGER,
            original_language TEXT,
            overview          TEXT,
            genre_ids         JSONB,
            ingested_at_date  DATE DEFAULT CURRENT_DATE,
            ingested_at       TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (tmdb_id, ingested_at_date)
        );
    """)
    cur.execute("ALTER TABLE raw.raw_tv ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # enriched TV show details
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_tv_details (
            tmdb_id              INTEGER PRIMARY KEY,
            title                TEXT,
            original_language    TEXT,
            number_of_seasons   INTEGER,
            number_of_episodes  INTEGER,
            episode_runtime_mins INTEGER,
            creator              TEXT,
            genres               JSONB,
            platforms            JSONB,
            poster_path          TEXT,
            ingested_at          TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("ALTER TABLE raw.raw_tv_details ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # simkl watch history + ratings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_ratings (
            simkl_id        INTEGER,
            tmdb_id         INTEGER,
            title           TEXT,
            content_type    TEXT,
            status          TEXT,
            rating          INTEGER,
            watched_at      TIMESTAMP,
            ingested_at     TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (simkl_id, content_type)
        );
    """)
    cur.execute("ALTER TABLE raw.raw_ratings ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # hardcover book ratings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_book_ratings (
            hardcover_id    TEXT PRIMARY KEY,
            title           TEXT,
            author          TEXT,
            isbn_13         TEXT,
            isbn_10         TEXT,
            rating          FLOAT,
            status          TEXT,
            finished_at     DATE,
            ingested_at     TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("ALTER TABLE raw.raw_book_ratings ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # openLibrary book metadata
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_books (
            ol_key          TEXT PRIMARY KEY,
            hardcover_id    TEXT,
            title           TEXT,
            author          TEXT,
            first_publish_year INTEGER,
            subjects        JSONB,
            ratings_average FLOAT,
            ratings_count   INTEGER,
            page_count      INTEGER,
            cover_id        INTEGER,
            ingested_at     TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("ALTER TABLE raw.raw_books ADD COLUMN IF NOT EXISTS cover_id INTEGER;")
    cur.execute("ALTER TABLE raw.raw_books ADD COLUMN IF NOT EXISTS pipeline_run_id TEXT;")

    # meta schema - artifacts computed by Dagster ops (not dbt)
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.taste_profile (
            id                      SERIAL PRIMARY KEY,
            generated_at            TIMESTAMP DEFAULT NOW(),
            summary_text            TEXT,
            top_genres_watch        JSONB,
            bottom_genres_watch     JSONB,
            top_creators_watch      JSONB,
            bottom_creators_watch   JSONB,
            top_genres_read         JSONB,
            bottom_genres_read      JSONB,
            top_creators_read       JSONB,
            bottom_creators_read    JSONB,
            avg_rating_by_type      JSONB,
            total_rated             INTEGER
        );
    """)

    # Split by domain rather than one table with a rank>=100 offset hack to
    # separate books from movies/tv - same reasoning as the marts split
    # (dim_watchable/dim_book, fact_watch_history/fact_reading_history).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.daily_recommendations_watch (
            id            SERIAL PRIMARY KEY,
            generated_at  TIMESTAMP DEFAULT NOW(),
            content_id    TEXT,
            title         TEXT,
            content_type  TEXT,
            rank          INTEGER,
            reason        TEXT,
            predicted_score FLOAT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.daily_recommendations_books (
            id            SERIAL PRIMARY KEY,
            generated_at  TIMESTAMP DEFAULT NOW(),
            content_id    TEXT,
            title         TEXT,
            rank          INTEGER,
            reason        TEXT
        );
    """)

    # meta.api_usage - daily local-LLM call volume/token size per feature
    # (ask / picks), used by the /usage endpoint for observability. No
    # external quota involved - fully local Ollama.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.api_usage (
            date         DATE NOT NULL,
            feature      TEXT NOT NULL,
            tokens_used  INTEGER NOT NULL DEFAULT 0,
            call_count   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (date, feature)
        );
    """)

    # meta.not_interested - user-driven exclusion list ("Not Interested" button
    # on the picks page). Checked by generate_recommendations' candidate query
    # so a dismissed title never gets recommended again, not just hidden once.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.not_interested (
            content_id  TEXT PRIMARY KEY,
            marked_at   TIMESTAMP DEFAULT NOW()
        );
    """)

    # meta.pipeline_alerts - one row per failed Dagster run, written by
    # sensors.py's run-failure sensor. Durable record behind the email alert,
    # and what /health reads to go red when the latest run failed.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.pipeline_alerts (
            run_id      TEXT PRIMARY KEY,
            job_name    TEXT NOT NULL,
            error       TEXT,
            failed_at   TIMESTAMP NOT NULL DEFAULT NOW(),
            email_sent  BOOLEAN NOT NULL DEFAULT FALSE
        );
    """)

    # meta.user_config - genuine user INPUT (set by hand), not a pipeline-derived
    # artifact like taste_profile/daily_recommendations. Read by ingestion ops
    # at runtime, so it must be available before any ingestion job runs.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.user_config (
            config_key    TEXT PRIMARY KEY,
            config_value  JSONB
        );
    """)

    cur.execute("""
        INSERT INTO meta.user_config (config_key, config_value)
        VALUES ('preferred_languages', '["en", "hi", "mr"]')
        ON CONFLICT (config_key) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO meta.user_config (config_key, config_value)
        VALUES ('recommendation_language_slots', '{"en": 4, "hi": 4, "mr": 2}')
        ON CONFLICT (config_key) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO meta.user_config (config_key, config_value)
        VALUES ('recommendation_fallback_language', '"hi"')
        ON CONFLICT (config_key) DO NOTHING;
    """)

    # TMDB watch-provider region (ISO 3166-1 country code) - streaming
    # availability differs per country, and picks require a platform.
    cur.execute("""
        INSERT INTO meta.user_config (config_key, config_value)
        VALUES ('watch_region', '"IN"')
        ON CONFLICT (config_key) DO NOTHING;
    """)

    # ── ask_readonly role: SELECT-only on marts/meta for the /ask LangChain
    # agent. No access to raw/staging (pre-cleaning data), no write grants
    # anywhere, read-only enforced at the role level so a bad agent-written
    # query can never mutate or delete real data, whatever SQL it generates.
    ask_readonly_password = os.getenv("ASK_READONLY_PASSWORD")
    if ask_readonly_password:
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ask_readonly') THEN
                    CREATE ROLE ask_readonly LOGIN PASSWORD %(password)s;
                END IF;
            END
            $$;
        """, {"password": ask_readonly_password})
        cur.execute("ALTER ROLE ask_readonly SET default_transaction_read_only = on;")
        cur.execute("ALTER ROLE ask_readonly SET statement_timeout = '5000ms';")
        cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO ask_readonly;").format(
            sql.Identifier(os.getenv("POSTGRES_DB"))))
        cur.execute("GRANT USAGE ON SCHEMA marts, meta TO ask_readonly;")
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA marts TO ask_readonly;")
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA meta TO ask_readonly;")
        # Default privileges must name the role that will CREATE future tables
        # (dbt and the ops connect as POSTGRES_USER) - hardcoding a role name
        # broke on any clone whose .env used a different user.
        owner = sql.Identifier(os.getenv("POSTGRES_USER"))
        for schema in ("marts", "meta"):
            cur.execute(sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT ON TABLES TO ask_readonly;"
            ).format(owner, sql.Identifier(schema)))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ All raw tables created.")

# Indexes for the meta.* tables this script actually owns (created here,
# never dropped/recreated). marts.* tables are dbt-managed and would have
# any index added here silently dropped on the next `dbt run` - those live
# in each model's own `config(indexes=[...])` instead, see dbt/models/marts/.
def create_indexes():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("CREATE INDEX IF NOT EXISTS idx_taste_profile_generated_at ON meta.taste_profile (generated_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_recommendations_watch_generated_at ON meta.daily_recommendations_watch (generated_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_recommendations_books_generated_at ON meta.daily_recommendations_books (generated_at DESC);")

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Meta indexes created.")

if __name__ == "__main__":
    init_db()
    create_indexes()