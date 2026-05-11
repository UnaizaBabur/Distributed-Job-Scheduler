"""
============================================================================
 consumer_lambda.py
============================================================================
 Triggered by:  SQS event source mapping on the main job queue.
 Responsibility:
    1. Parse each SQS message into a Job record.
    2. Mark the job as 'processing' in RDS.
    3. Execute the job's logic (dispatched by job_type).
    4. On success -> mark 'succeeded'.
    5. On failure -> raise an exception so SQS increments the receive count
       and re-delivers later, OR routes to the DLQ once maxReceiveCount is hit.

 Why we *raise* instead of catching:
    SQS + Lambda's retry semantics work via message visibility timeouts.
    If the Lambda invocation throws, the message becomes visible again
    after `VisibilityTimeout` seconds. SQS auto-routes to the DLQ when
    `ApproximateReceiveCount > maxReceiveCount`.  We piggyback on that
    rather than building our own retry loop.

 Exponential backoff:
    AWS SQS does not natively support backoff — every retry happens after
    `VisibilityTimeout`. To approximate exponential backoff we *extend* the
    visibility timeout for the message based on its current receive count
    BEFORE we throw. So retry 1 = 30s, retry 2 = 60s, retry 3 = 120s.

 Idempotency:
    Lambda may invoke this with the same message more than once
    (at-least-once delivery). We use the SQS message_id as a natural
    dedup key against the jobs table.
============================================================================
"""

import json
import os
import time
import uuid
import logging
import random
from typing import Any

import boto3
import psycopg2
from psycopg2.extras import Json

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Connection details come from Lambda env vars (set via Terraform/console).
DB_HOST     = os.environ["DB_HOST"]
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))

QUEUE_URL = os.environ["QUEUE_URL"]  # main queue, used to extend visibility

sqs = boto3.client("sqs")

# Reuse DB connection across warm Lambda invocations to avoid the cost
# of TCP+TLS handshake on every job. Lambda will re-init this on cold start.
_db_conn = None
def get_db():
    global _db_conn
    if _db_conn is None or _db_conn.closed:
        _db_conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER,
            password=DB_PASSWORD, port=DB_PORT, connect_timeout=5,
        )
        _db_conn.autocommit = False
    return _db_conn

# ---------------------------------------------------------------------------
# Backoff helper
# ---------------------------------------------------------------------------
def compute_backoff_seconds(receive_count: int) -> int:
    """
    Exponential backoff with jitter, capped at 900s (SQS max).
    receive_count is 1 on first delivery, 2 on first retry, etc.
    """
    base = 30
    # 30, 60, 120, 240, ... with +/- 20% jitter
    delay = base * (2 ** (receive_count - 1))
    jitter = delay * random.uniform(-0.2, 0.2)
    return min(int(delay + jitter), 900)


# ---------------------------------------------------------------------------
# Job execution dispatcher
# ---------------------------------------------------------------------------
def run_job(job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Pretend job runner. In a real system this would dispatch to actual
    workloads (image processing, ML inference, etc.). For our project
    we simulate work + occasional failures so retries/DLQ are observable.
    """
    if job_type == "echo":
        return {"echoed": payload}

    if job_type == "sleep":
        time.sleep(min(payload.get("seconds", 1), 5))
        return {"slept": payload.get("seconds", 1)}

    if job_type == "flaky":
        # 40% chance of failure - drives retry behaviour during testing.
        if random.random() < 0.4:
            raise RuntimeError("Simulated flaky failure")
        return {"ok": True}

    if job_type == "always_fail":
        # Used to demonstrate DLQ routing.
        raise RuntimeError("This job is designed to always fail")

    if job_type == "compute":
        # CPU-bound dummy work
        n = int(payload.get("n", 1000))
        s = sum(i * i for i in range(n))
        return {"sum_of_squares": s, "n": n}

    raise ValueError(f"Unknown job_type: {job_type}")


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------
def upsert_submitted(conn, job_id, job_type, payload, sqs_message_id):
    """First time we see a job, insert it. If we see it again (retry), no-op."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (job_id, job_type, payload, status, sqs_message_id)
            VALUES (%s, %s, %s, 'submitted', %s)
            ON CONFLICT (job_id) DO NOTHING
            """,
            (job_id, job_type, Json(payload), sqs_message_id),
        )
        cur.execute(
            "INSERT INTO job_events (job_id, event_type, detail) VALUES (%s, %s, %s)",
            (job_id, "submitted", f"sqs_message_id={sqs_message_id}"),
        )

def mark_processing(conn, job_id, receive_count):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
               SET status = 'processing',
                   started_at = COALESCE(started_at, NOW()),
                   retry_count = %s
             WHERE job_id = %s
            """,
            (max(0, receive_count - 1), job_id),
        )
        cur.execute(
            "INSERT INTO job_events (job_id, event_type, detail) VALUES (%s, %s, %s)",
            (job_id, "started", f"receive_count={receive_count}"),
        )

def mark_succeeded(conn, job_id, result):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
               SET status = 'succeeded',
                   finished_at = NOW(),
                   last_error = NULL
             WHERE job_id = %s
            """,
            (job_id,),
        )
        cur.execute(
            "INSERT INTO job_events (job_id, event_type, detail) VALUES (%s, %s, %s)",
            (job_id, "succeeded", json.dumps(result)[:500]),
        )

def mark_failed(conn, job_id, error_msg, receive_count, max_receives):
    """If we've hit the cap, mark dlq. Otherwise mark failed (will be retried)."""
    will_dlq = receive_count >= max_receives
    new_status = "dlq" if will_dlq else "failed"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
               SET status = %s,
                   last_error = %s,
                   retry_count = %s,
                   finished_at = CASE WHEN %s THEN NOW() ELSE finished_at END
             WHERE job_id = %s
            """,
            (new_status, error_msg[:2000], receive_count, will_dlq, job_id),
        )
        cur.execute(
            "INSERT INTO job_events (job_id, event_type, detail) VALUES (%s, %s, %s)",
            (job_id, new_status, error_msg[:500]),
        )


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Lambda entry point. event = {"Records": [<sqs_record>, ...]}.
    
    We process records one-by-one. If ANY record fails we use the
    `batchItemFailures` partial-batch-response feature so SQS only retries
    the failed items, not the whole batch (this requires the event source
    mapping to be configured with ReportBatchItemFailures).
    """
    failures = []
    conn = get_db()

    for record in event.get("Records", []):
        sqs_message_id = record["messageId"]
        receipt_handle = record["receiptHandle"]
        receive_count  = int(record["attributes"].get("ApproximateReceiveCount", "1"))
        max_receives   = int(os.environ.get("MAX_RECEIVES", "3"))

        try:
            body = json.loads(record["body"])
            job_id   = body.get("job_id") or str(uuid.uuid4())
            job_type = body["job_type"]
            payload  = body.get("payload", {})

            logger.info(
                "Processing job_id=%s job_type=%s receive_count=%d",
                job_id, job_type, receive_count,
            )

            # 1) ensure job row exists
            upsert_submitted(conn, job_id, job_type, payload, sqs_message_id)
            # 2) flag in-progress
            mark_processing(conn, job_id, receive_count)
            conn.commit()

            # 3) actually run it
            result = run_job(job_type, payload)

            # 4) success
            mark_succeeded(conn, job_id, result)
            conn.commit()

        except Exception as e:
            conn.rollback()
            err = f"{type(e).__name__}: {e}"
            logger.warning("Job failed: %s (receive_count=%d)", err, receive_count)

            try:
                # We need a job_id to log the failure. If we couldn't even parse
                # the body, fall back to the sqs_message_id as a synthetic uuid.
                jid = locals().get("job_id") or str(uuid.uuid5(uuid.NAMESPACE_OID, sqs_message_id))
                mark_failed(conn, jid, err, receive_count, max_receives)
                conn.commit()
            except Exception as db_err:
                logger.error("Failed to record failure in DB: %s", db_err)
                conn.rollback()

            # Extend visibility timeout BEFORE we tell SQS this message failed,
            # so the next retry happens after our backoff window, not the
            # default queue VisibilityTimeout.
            try:
                backoff = compute_backoff_seconds(receive_count)
                sqs.change_message_visibility(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=backoff,
                )
                logger.info("Set backoff for retry: %ds", backoff)
            except Exception as vis_err:
                # Non-fatal — the message will still retry, just with default timeout.
                logger.warning("Could not set visibility timeout: %s", vis_err)

            failures.append({"itemIdentifier": sqs_message_id})

    return {"batchItemFailures": failures}
