"""
============================================================================
 dlq_monitor_lambda.py
============================================================================
 Triggered by:  SQS event source mapping on the *Dead-Letter Queue*.
 Responsibility:
    Whenever a message lands in the DLQ (because the consumer Lambda
    exhausted its retries), this function:
      1. Updates the corresponding jobs row to status='dlq' if not already.
      2. Appends a 'dlq' event to job_events.
      3. (Optionally) emits a CloudWatch custom metric so we can alarm
         on DLQ growth.

 Why a separate Lambda?
    Consumer Lambda only sees the main queue. SQS auto-routes failed
    messages to the DLQ, but nothing else watches the DLQ unless we
    wire something to it. Without this monitor, DLQ messages would
    sit silently forever.

 Note:
    The consumer ALREADY marks status='dlq' when it sees the final
    failed attempt (receive_count >= max_receives). This Lambda is a
    safety net that catches edge cases where the consumer crashed
    before it could update the DB.
============================================================================
"""

import json
import os
import logging
import uuid

import boto3
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_HOST     = os.environ["DB_HOST"]
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))

cloudwatch = boto3.client("cloudwatch")

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


def lambda_handler(event, context):
    conn = get_db()
    dlq_count = 0

    for record in event.get("Records", []):
        sqs_message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            job_id   = body.get("job_id") or str(uuid.uuid5(uuid.NAMESPACE_OID, sqs_message_id))
            job_type = body.get("job_type", "unknown")
            payload  = body.get("payload", {})

            logger.warning("DLQ message received: job_id=%s job_type=%s", job_id, job_type)

            with conn.cursor() as cur:
                # Ensure a row exists, then force status='dlq'
                cur.execute(
                    """
                    INSERT INTO jobs (job_id, job_type, payload, status, sqs_message_id, finished_at)
                    VALUES (%s, %s, %s, 'dlq', %s, NOW())
                    ON CONFLICT (job_id) DO UPDATE
                       SET status = 'dlq',
                           finished_at = COALESCE(jobs.finished_at, NOW())
                    """,
                    (job_id, job_type, Json(payload), sqs_message_id),
                )
                cur.execute(
                    "INSERT INTO job_events (job_id, event_type, detail) VALUES (%s, %s, %s)",
                    (job_id, "dlq", "Detected by DLQ monitor"),
                )
            conn.commit()
            dlq_count += 1
        except Exception as e:
            conn.rollback()
            logger.error("Failed to handle DLQ message %s: %s", sqs_message_id, e)

    # Custom metric for dashboarding/alarming
    if dlq_count > 0:
        try:
            cloudwatch.put_metric_data(
                Namespace="JobScheduler",
                MetricData=[{
                    "MetricName": "DLQMessagesObserved",
                    "Value": dlq_count,
                    "Unit": "Count",
                }],
            )
        except Exception as cw_err:
            logger.warning("CloudWatch put_metric_data failed: %s", cw_err)

    return {"processed": dlq_count}
