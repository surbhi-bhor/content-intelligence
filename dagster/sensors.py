import os
import smtplib
from email.message import EmailMessage

import psycopg2
from dagster import DefaultSensorStatus, RunFailureSensorContext, get_dagster_logger, run_failure_sensor

DAGSTER_UI_URL = os.getenv("DAGSTER_UI_URL", "http://localhost:3000")

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def send_failure_email(job_name: str, run_id: str, error: str) -> bool:
    """Gmail SMTP with an app password (a normal account password is
    rejected by Gmail for SMTP). Returns False without raising when alert
    email isn't configured - the meta.pipeline_alerts row and /health still
    surface the failure."""
    to_addr = os.getenv("ALERT_EMAIL_TO")
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_APP_PASSWORD")
    if not (to_addr and user and password):
        return False

    msg = EmailMessage()
    msg["Subject"] = f"[Content Pipeline] {job_name} failed"
    msg["From"] = user
    msg["To"] = to_addr
    msg.set_content(
        f"Dagster run failed.\n\n"
        f"Job:    {job_name}\n"
        f"Run ID: {run_id}\n"
        f"Logs:   {DAGSTER_UI_URL}/runs/{run_id}\n\n"
        f"Error:\n{error}\n"
    )
    with smtplib.SMTP(os.getenv("SMTP_HOST", "smtp.gmail.com"), int(os.getenv("SMTP_PORT", "587")), timeout=20) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)
    return True

def record_and_notify(job_name: str, run_id: str, error: str) -> bool:
    """Plain function behind the sensor so it can be exercised without a
    real failed run. Writes the alert row first - an SMTP outage must never
    lose the record of the failure itself."""
    log = get_dagster_logger()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO meta.pipeline_alerts (run_id, job_name, error)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING;
            """, (run_id, job_name, error))
        conn.commit()

        try:
            sent = send_failure_email(job_name, run_id, error)
        except Exception as e:
            log.error(f"[ALERT] failure email for run {run_id} could not be sent: {e}")
            sent = False
        if sent:
            with conn.cursor() as cur:
                cur.execute("UPDATE meta.pipeline_alerts SET email_sent = TRUE WHERE run_id = %s;", (run_id,))
            conn.commit()
        else:
            log.warning(f"[ALERT] {job_name} run {run_id} failed — recorded, no email sent")
        return sent
    finally:
        conn.close()

# Monitors every job in this code location. Ships RUNNING for the same reason
# the weekly schedule does: the whole point is catching the unattended Friday
# run, which nobody is watching. A daemon that's down entirely is caught by
# /health's 8-day staleness check instead, since no sensor can fire then.
@run_failure_sensor(
    name="pipeline_failure_alert",
    default_status=DefaultSensorStatus.RUNNING,
    description="Records every failed run in meta.pipeline_alerts and emails ALERT_EMAIL_TO",
)
def pipeline_failure_alert(context: RunFailureSensorContext):
    # The run-level message only names which steps failed - the actual
    # exception lives on each step's failure event.
    parts = [context.failure_event.message or "No run-level message captured"]
    for event in context.get_step_failure_events():
        step_error = getattr(event.event_specific_data, "error", None)
        if step_error is None:
            parts.append(f"[{event.step_key}] {event.message}")
            continue
        # The top-level message is Dagster's generic "Error occurred while
        # executing op"; the real exception (e.g. an SSL error) is in the
        # cause chain, so include every level.
        chain = []
        while step_error is not None:
            chain.append(step_error.message.strip().splitlines()[0])
            step_error = step_error.cause
        parts.append(f"[{event.step_key}] " + "\n  caused by: ".join(chain))
    record_and_notify(context.dagster_run.job_name, context.dagster_run.run_id, "\n\n".join(parts)[:4000])
