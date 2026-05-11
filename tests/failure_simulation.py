"""
============================================================================
 failure_simulation.py
============================================================================
 Drives the system through 4 scenarios so we can demonstrate resilience
 in the project demo:

   Scenario A:  Healthy traffic        — 100 mixed jobs, expect ~80% success
   Scenario B:  Burst load            — 200 jobs at concurrency=40
   Scenario C:  Forced DLQ           — 10 always_fail jobs, watch DLQ fill
   Scenario D:  Malformed messages   — bad JSON, watch consumer reject

 Run:
   QUEUE_URL=...  python tests/failure_simulation.py
============================================================================
"""

import json
import os
import sys
import time
import uuid

import boto3

QUEUE_URL = os.environ.get("QUEUE_URL")
REGION    = os.environ.get("AWS_REGION", "us-east-1")

if not QUEUE_URL:
    print("ERROR: set QUEUE_URL")
    sys.exit(2)

sqs = boto3.client("sqs", region_name=REGION)


def submit_raw(body: str):
    return sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=body)["MessageId"]

def submit(job_type: str, payload: dict):
    return submit_raw(json.dumps({
        "job_id": str(uuid.uuid4()),
        "job_type": job_type,
        "payload": payload,
    }))


# ---------------------------------------------------------------------------
def scenario_a_healthy(n=100):
    print(f"\n[Scenario A] Healthy traffic — {n} mixed jobs")
    types = ["echo", "compute", "sleep"]
    for i in range(n):
        submit(types[i % 3], {"i": i, "n": 500})
    print(f"  submitted {n} jobs")

def scenario_b_burst(n=200, concurrency=40):
    print(f"\n[Scenario B] Burst — {n} jobs (concurrency={concurrency})")
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = [pool.submit(submit, "compute", {"n": 200}) for _ in range(n)]
        for f in cf.as_completed(futs):
            _ = f.result()
    print(f"  burst submitted")

def scenario_c_force_dlq(n=10):
    print(f"\n[Scenario C] Force DLQ — {n} always_fail jobs")
    for _ in range(n):
        submit("always_fail", {"intentional": True})
    print(f"  submitted {n} guaranteed-fail jobs")
    print(f"  expect them in DLQ after 3 retries (~3-5 minutes)")

def scenario_d_malformed():
    print("\n[Scenario D] Malformed messages")
    # not valid JSON
    submit_raw("this is not json {{{")
    # JSON but missing required field
    submit_raw(json.dumps({"foo": "bar"}))
    # JSON but invalid job_type
    submit_raw(json.dumps({"job_id": str(uuid.uuid4()),
                           "job_type": "this_does_not_exist", "payload": {}}))
    print("  submitted 3 malformed messages")
    print("  expect consumer to reject -> retry -> DLQ")


if __name__ == "__main__":
    scenario_a_healthy()
    time.sleep(2)
    scenario_b_burst()
    time.sleep(2)
    scenario_c_force_dlq()
    time.sleep(2)
    scenario_d_malformed()
    print("\nAll scenarios queued. Watch the dashboard for ~5 minutes.")
