"""
============================================================================
 app.py - Flask Monitoring Dashboard
============================================================================
 Hosted on:   EC2 (t2.micro), Ubuntu 22.04, exposed on port 5000.
 Reads from:  RDS PostgreSQL + CloudWatch metrics + SQS queue attributes.

 Endpoints:
   GET  /              -> HTML dashboard (templates/dashboard.html)
   GET  /api/stats     -> JSON: counts by status, last-N jobs, queue depth
   GET  /api/dlq       -> JSON: jobs currently in DLQ
   GET  /api/recent    -> JSON: recent job_events (audit trail)
   GET  /api/cloudwatch -> JSON: CloudWatch metric snapshots

 Why split into endpoints?
   The HTML page loads once. Chart.js then polls the JSON endpoints every
   few seconds. Keeps the UI responsive even when the dataset grows.
============================================================================
"""

import os
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
DB_HOST     = os.environ["DB_HOST"]
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))

MAIN_QUEUE_URL = os.environ.get("QUEUE_URL", "")
DLQ_URL        = os.environ.get("DLQ_URL", "")
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-1")

app = Flask(__name__)

# --------------------------------------------------------------------------
# Lazy AWS clients (created once per process)
# --------------------------------------------------------------------------
_sqs = None
_cw = None
def sqs():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs", region_name=AWS_REGION)
    return _sqs

def cw():
    global _cw
    if _cw is None:
        _cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    return _cw

# --------------------------------------------------------------------------
# DB connection helper
# --------------------------------------------------------------------------
def db_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT, connect_timeout=5,
    )

# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/stats")
def api_stats():
    """Counts by status + recent throughput from RDS."""
    out = {"status_counts": {}, "throughput_5min": 0, "queue_depth": None, "dlq_depth": None}
    try:
        with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status;")
            out["status_counts"] = {r["status"]: r["n"] for r in cur.fetchall()}

            cur.execute("""
                SELECT COUNT(*) AS n FROM jobs
                WHERE finished_at > NOW() - INTERVAL '5 minutes'
                  AND status IN ('succeeded', 'failed', 'dlq')
            """)
            out["throughput_5min"] = cur.fetchone()["n"]
    except Exception as e:
        out["db_error"] = str(e)

    # Live queue depth from SQS
    if MAIN_QUEUE_URL:
        try:
            r = sqs().get_queue_attributes(
                QueueUrl=MAIN_QUEUE_URL,
                AttributeNames=["ApproximateNumberOfMessages",
                                "ApproximateNumberOfMessagesNotVisible"],
            )
            out["queue_depth"] = {
                "visible":     int(r["Attributes"]["ApproximateNumberOfMessages"]),
                "in_flight":   int(r["Attributes"]["ApproximateNumberOfMessagesNotVisible"]),
            }
        except Exception as e:
            out["queue_error"] = str(e)

    if DLQ_URL:
        try:
            r = sqs().get_queue_attributes(
                QueueUrl=DLQ_URL,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            out["dlq_depth"] = int(r["Attributes"]["ApproximateNumberOfMessages"])
        except Exception as e:
            out["dlq_error"] = str(e)

    return jsonify(out)


@app.route("/api/dlq")
def api_dlq():
    rows = []
    try:
        with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT job_id, job_type, retry_count, last_error, finished_at
                FROM jobs
                WHERE status = 'dlq'
                ORDER BY finished_at DESC NULLS LAST
                LIMIT 50;
            """)
            rows = cur.fetchall()
        for r in rows:
            r["job_id"]      = str(r["job_id"])
            r["finished_at"] = r["finished_at"].isoformat() if r["finished_at"] else None
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(rows)


@app.route("/api/recent")
def api_recent():
    rows = []
    try:
        with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT e.event_id, e.job_id, e.event_type, e.detail, e.created_at,
                       j.job_type
                FROM job_events e
                LEFT JOIN jobs j ON j.job_id = e.job_id
                ORDER BY e.created_at DESC
                LIMIT 100;
            """)
            rows = cur.fetchall()
        for r in rows:
            r["job_id"]     = str(r["job_id"])
            r["created_at"] = r["created_at"].isoformat() if r["created_at"] else None
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(rows)


@app.route("/api/cloudwatch")
def api_cloudwatch():
    """Pull recent CloudWatch SQS metrics so the dashboard can chart them."""
    end   = datetime.now(timezone.utc)
    start = end - timedelta(minutes=30)

    metrics = {}
    if not MAIN_QUEUE_URL:
        return jsonify({"error": "QUEUE_URL not configured"}), 500
    queue_name = MAIN_QUEUE_URL.rsplit("/", 1)[-1]
    try:
        for metric in [
            "ApproximateNumberOfMessagesVisible",
            "NumberOfMessagesSent",
            "NumberOfMessagesReceived",
        ]:
            resp = cw().get_metric_statistics(
                Namespace="AWS/SQS",
                MetricName=metric,
                Dimensions=[{"Name": "QueueName", "Value": queue_name}],
                StartTime=start, EndTime=end,
                Period=60, Statistics=["Average", "Sum"],
            )
            datapoints = sorted(resp["Datapoints"], key=lambda d: d["Timestamp"])
            metrics[metric] = [
                {"t": d["Timestamp"].isoformat(),
                 "avg": d.get("Average"), "sum": d.get("Sum")}
                for d in datapoints
            ]
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(metrics)


@app.route("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    # In production you'd run via gunicorn behind nginx; for the demo,
    # `python app.py` is fine.
    app.run(host="0.0.0.0", port=5000, debug=False)
