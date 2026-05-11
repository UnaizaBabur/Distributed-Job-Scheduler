# Distributed Job Scheduling and Monitoring System

CE308 Cloud Computing — Spring 2026
Zermine Wajid (2023786) | Unaiza Babur (2023739)

A serverless, fault-tolerant job scheduler on AWS with a real-time monitoring dashboard.

## Architecture

```
   Producer (CLI / API Gateway)
            |
            v
   ┌─────────────────┐         ┌─────────────────┐
   │  SQS main queue │  ─────► │  DLQ (3-strikes)│
   └────────┬────────┘         └────────┬────────┘
            │ event-source              │ event-source
            v                           v
   ┌─────────────────┐         ┌─────────────────┐
   │ Consumer Lambda │         │ DLQ Monitor     │
   │  · retry+backoff│         │   Lambda        │
   │  · status writes│         │  · audit logs   │
   └────────┬────────┘         │  · CW metrics   │
            │                  └────────┬────────┘
            v                           │
   ┌──────────────────────┐ ◄───────────┘
   │  RDS PostgreSQL      │
   │   jobs / job_events  │
   └──────────┬───────────┘
              │
              v
   ┌──────────────────────────┐    +    ┌──────────────────┐
   │ Flask dashboard (EC2)    │ ◄────── │  CloudWatch      │
   │  · KPI cards · charts    │         │   AWS/SQS metrics│
   │  · DLQ viewer · events   │         └──────────────────┘
   └──────────────────────────┘
```

## Repo layout

```
distributed-job-scheduler/
├── infrastructure/
│   └── setup_aws.sh              ← one-shot AWS CLI provisioning
├── lambdas/
│   ├── consumer/                 ← processes jobs, retries, backoff
│   │   ├── consumer_lambda.py
│   │   └── requirements.txt
│   └── dlq_monitor/              ← watches the DLQ
│       ├── dlq_monitor_lambda.py
│       └── requirements.txt
├── producer/
│   └── producer.py               ← CLI: submit single jobs OR loadtest
├── dashboard/
│   ├── app.py                    ← Flask API + HTML
│   ├── templates/dashboard.html
│   └── requirements.txt
├── sql/
│   └── schema.sql                ← RDS PostgreSQL schema
├── tests/
│   ├── test_consumer_logic.py    ← pytest unit tests
│   └── failure_simulation.py     ← 4 demo scenarios (healthy / burst / DLQ / malformed)
└── docs/
    ├── EXPLANATION.docx          ← how everything works
    └── DESIGN_NOTES.docx         ← what we built, why, and what we didn't
```

## Quick start

```bash
# 1. Provision AWS (creates SQS, DLQ, IAM role, Lambdas, event mappings)
export DB_HOST=...   DB_PASSWORD=...
./infrastructure/setup_aws.sh

# 2. Apply DB schema
psql -h $DB_HOST -U jobadmin -d jobscheduler -f sql/schema.sql

# 3. Submit jobs
export QUEUE_URL=https://sqs.us-east-1.amazonaws.com/.../job-scheduler-main
python producer/producer.py loadtest --count 100 --concurrency 20

# 4. Open dashboard
cd dashboard && pip install -r requirements.txt
QUEUE_URL=$QUEUE_URL DLQ_URL=$DLQ_URL DB_HOST=$DB_HOST DB_PASSWORD=$DB_PASSWORD \
  python app.py
# -> http://<ec2-public-ip>:5000
```

## Running tests

```bash
pip install pytest psycopg2-binary boto3
AWS_DEFAULT_REGION=us-east-1 python -m pytest tests/test_consumer_logic.py -v
```

See `docs/EXPLANATION.docx` for the deep dive and `docs/DESIGN_NOTES.docx` for what was deferred and why.
