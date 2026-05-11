# Distributed Job Scheduler

> A cloud-native, distributed job scheduling and monitoring system built on Amazon Web Services.  
> CE-308 Semester Project 

## Overview

This system accepts asynchronous task requests, processes them through event-driven AWS Lambda functions, applies exponential backoff retry policies, routes persistent failures to a Dead-Letter Queue (DLQ), and surfaces real-time operational metrics on a live monitoring dashboard backed by Amazon RDS PostgreSQL.

## Architecture

| Component | AWS Service | Role |
|---|---|---|
| Job Ingestion | SQS Standard Queue | Receives and queues incoming job messages from the producer script |
| Job Processor | AWS Lambda (Python 3.12) | Executes jobs with retry logic and exponential backoff; writes state to RDS |
| Failure Handling | SQS Dead-Letter Queue | Captures jobs that exceed `maxReceiveCount=3`; monitored by a secondary Lambda |
| State Persistence | Amazon RDS PostgreSQL 15 | Stores job lifecycle state (`submitted`, `processing`, `succeeded`, `failed`, `dlq`) and full audit log |
| Monitoring | Flask (EC2) + CloudWatch | Real-time dashboard showing queue depth, status distribution, DLQ contents, SQS metrics |

---

## Features

- **Serverless job ingestion** via SQS + Lambda with event source mapping (`batch=1`, `ReportBatchItemFailures`)
- **Failure handling** with `maxReceiveCount=3` redrive policy, exponential backoff via `change_message_visibility`, and a DLQ Monitor Lambda
- **Stateful job tracking** using a `jobs` table (current state) and `job_events` table (full audit log) with 5 status ENUM values
- **Live monitoring dashboard** with 4 JSON API endpoints, Chart.js charts, KPI cards, polling every 3 seconds
- **Resilience testing** via `always_fail` job type, load test sending 50–200 concurrent jobs, and `failure_simulation.py` running 4 failure scenarios

---

## AWS Infrastructure

### SQS Queues

- **Main Queue:** `job-scheduler-main`
  - `VisibilityTimeout`: 60 seconds
  - `MessageRetentionPeriod`: 4 days
  - `RedrivePolicy`: `maxReceiveCount=3`, `deadLetterTargetArn=job-scheduler-dlq`
- **Dead-Letter Queue:** `job-scheduler-dlq` (14-day retention)

### Lambda Functions

- `job-scheduler-consumer` — Python 3.12, 256 MB, 30s timeout; triggered by SQS main queue
- `job-scheduler-dlq-monitor` — Python 3.12, 256 MB, 30s timeout; triggered by SQS DLQ
- Both deployed with Linux-compatible `psycopg2-binary` (`manylinux2014_x86_64` wheel)

**Environment variables:** `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `QUEUE_URL`, `DLQ_URL`, `MAX_RECEIVES`

### RDS PostgreSQL

- Instance: `db.t3.micro`, PostgreSQL 15, Single-AZ
- Database: `jobscheduler`
- Tables: `jobs` (primary state), `job_events` (audit log)
- Views: `v_status_counts`, `v_recent_dlq`

### IAM Role

- Role: `job-scheduler-lambda-role`
- Policies: `AWSLambdaBasicExecutionRole`, `AmazonSQSFullAccess`, `CloudWatchFullAccess`

---

## Deployment

### Step 1 — RDS Database Creation

Create an Amazon RDS PostgreSQL 15 instance (`db.t3.micro`, free tier) via AWS Console. Configure public access and security group inbound rules. Apply `schema.sql` via pgAdmin to create the `jobs` and `job_events` tables.

### Step 2 — Infrastructure Provisioning

```bash
python infrastructure/setup_aws.py
```

This creates the SQS DLQ, main queue with redrive policy, IAM Lambda execution role, both Lambda functions, and event source mappings.

### Step 3 — Linux-Compatible Lambda Packaging

> **Important:** Packaging on Windows requires explicitly targeting the Linux runtime.

```bash
pip install psycopg2-binary \
  --platform manylinux2014_x86_64 \
  --target build/consumer_linux \
  --python-version 3.12 \
  --only-binary=:all:
```

Rezip using Python's `zipfile` module (the `zip` command is unavailable in Windows Git Bash) and redeploy.

### Step 4 — Environment Variable Configuration

```bash
aws lambda update-function-configuration \
  --function-name job-scheduler-consumer \
  --environment "Variables={DB_HOST=...,DB_NAME=...,DB_USER=...,DB_PASSWORD=...,QUEUE_URL=...,DLQ_URL=...,MAX_RECEIVES=3}"
```

Repeat for `job-scheduler-dlq-monitor`.

### Step 5 — Verification

Invoke Lambda directly with a base64-encoded test payload. Confirm `batchItemFailures: []` in the response and verify the row appears in the `jobs` table with `status = succeeded` via pgAdmin.

### Step 6 — Launch Dashboard

```bash
python dashboard/app.py
```

Visit `http://localhost:5000` to view the live monitoring dashboard.

---

## Key Technical Highlights

- **Exponential backoff** implemented via SQS visibility timeout extension (not sleep-based)
- **Split database design** — `jobs` for fast state queries, `job_events` for a complete audit trail
- **Partial-batch failure responses** to avoid penalizing healthy messages in a failed batch
- **Platform compatibility** — non-trivial Windows-to-Linux `psycopg2` deployment resolved without CI/CD

