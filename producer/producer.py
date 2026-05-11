"""
============================================================================
 producer.py
============================================================================
 Submits job messages to the main SQS queue. Two modes:

   1. CLI mode:    python producer.py submit --type echo --payload '{"x":1}'
   2. Load mode:   python producer.py loadtest --count 200 --concurrency 20

 Why we have a load-test mode:
   The whole point of an SQS+Lambda pipeline is concurrency. To prove the
   system actually scales we need to be able to fire 50–200 jobs at once
   and watch the dashboard react.

 Message schema (JSON in SQS body):
   {
     "job_id":   "<uuid>",          # optional; consumer will mint one if absent
     "job_type": "echo|sleep|flaky|always_fail|compute",
     "payload":  { ... }             # arbitrary
   }
============================================================================
"""

import argparse
import concurrent.futures as cf
import json
import os
import random
import sys
import time
import uuid

import boto3

QUEUE_URL = os.environ.get("QUEUE_URL", "")
REGION    = os.environ.get("AWS_REGION", "us-east-1")


def make_sqs():
    return boto3.client("sqs", region_name=REGION)


# --------------------------------------------------------------------------
# Single submission
# --------------------------------------------------------------------------
def submit_one(sqs, queue_url: str, job_type: str, payload: dict, job_id: str | None = None) -> str:
    body = {
        "job_id":   job_id or str(uuid.uuid4()),
        "job_type": job_type,
        "payload":  payload,
    }
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))
    return resp["MessageId"]


# --------------------------------------------------------------------------
# Load test
# --------------------------------------------------------------------------
JOB_MIX = [
    # (job_type, weight, payload_factory)
    ("echo",        4, lambda: {"msg": f"hello-{random.randint(0, 9999)}"}),
    ("sleep",       3, lambda: {"seconds": random.randint(1, 3)}),
    ("compute",     3, lambda: {"n": random.randint(500, 5000)}),
    ("flaky",       2, lambda: {"attempt_id": str(uuid.uuid4())}),
    ("always_fail", 1, lambda: {"reason": "demonstrate DLQ"}),
]

def pick_job():
    types, weights, factories = zip(*[(t, w, f) for t, w, f in JOB_MIX])
    chosen = random.choices(range(len(types)), weights=weights, k=1)[0]
    return types[chosen], factories[chosen]()


def loadtest(queue_url: str, count: int, concurrency: int) -> None:
    sqs = make_sqs()
    print(f"[loadtest] submitting {count} jobs with concurrency={concurrency}")
    t0 = time.time()
    submitted = 0

    def task(_):
        jt, pl = pick_job()
        return submit_one(sqs, queue_url, jt, pl)

    with cf.ThreadPoolExecutor(max_workers=concurrency) as pool:
        for _ in pool.map(task, range(count)):
            submitted += 1
            if submitted % 25 == 0:
                print(f"  ... {submitted}/{count} submitted")

    elapsed = time.time() - t0
    print(f"[loadtest] done. {submitted} jobs in {elapsed:.2f}s "
          f"= {submitted/elapsed:.1f} msg/s")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("submit", help="submit a single job")
    p1.add_argument("--type", required=True, dest="job_type",
                    choices=["echo", "sleep", "flaky", "always_fail", "compute"])
    p1.add_argument("--payload", default="{}", help="JSON payload")
    p1.add_argument("--queue-url", default=QUEUE_URL)

    p2 = sub.add_parser("loadtest", help="submit many jobs concurrently")
    p2.add_argument("--count",       type=int, default=100)
    p2.add_argument("--concurrency", type=int, default=10)
    p2.add_argument("--queue-url",   default=QUEUE_URL)

    args = p.parse_args()
    if not args.queue_url:
        print("ERROR: must set QUEUE_URL env var or pass --queue-url", file=sys.stderr)
        sys.exit(2)

    if args.cmd == "submit":
        sqs = make_sqs()
        msg_id = submit_one(sqs, args.queue_url, args.job_type, json.loads(args.payload))
        print(f"submitted MessageId={msg_id}")
    elif args.cmd == "loadtest":
        loadtest(args.queue_url, args.count, args.concurrency)


if __name__ == "__main__":
    main()
