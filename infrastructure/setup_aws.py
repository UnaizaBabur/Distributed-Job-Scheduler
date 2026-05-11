"""
setup_aws.py
------------
Run this ONCE to provision everything:
  - SQS DLQ
  - SQS main queue (with redrive to DLQ)
  - IAM role for Lambda
  - Package + deploy Consumer Lambda
  - Package + deploy DLQ Monitor Lambda
  - Wire SQS -> Lambda event source mappings

Usage:
    python infrastructure/setup_aws.py

Make sure these are set before running:
    set DB_HOST=your-rds-endpoint.rds.amazonaws.com
    set DB_PASSWORD=yourpassword
    (or edit the CONFIG block below directly)
"""

import boto3
import json
import os
import shutil
import subprocess
import sys
import time
DB_HOST     = os.environ.get("DB_HOST", "jobscheduler-db.cuvwes8oikjc.us-east-1.rds.amazonaws.com")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "pillowcase")
import zipfile
from pathlib import Path

# ============================================================
# CONFIG — edit these before running
# ============================================================
REGION      = "us-east-1"
DB_HOST     = os.environ.get("DB_HOST", "FILL_THIS_IN")
DB_NAME     = "jobscheduler"
DB_USER     = "jobadmin"
DB_PASSWORD = os.environ.get("DB_PASSWORD", "FILL_THIS_IN")
DB_PORT     = "5432"
MAX_RECEIVES = "3"

PROJECT        = "job-scheduler"
MAIN_QUEUE     = f"{PROJECT}-main"
DLQ_QUEUE      = f"{PROJECT}-dlq"
CONSUMER_FN    = f"{PROJECT}-consumer"
DLQ_MON_FN     = f"{PROJECT}-dlq-monitor"
ROLE_NAME      = f"{PROJECT}-lambda-role"
# ============================================================

# Paths (relative to project root, i.e. where you run this from)
ROOT         = Path(__file__).parent.parent
CONSUMER_DIR = ROOT / "lambdas" / "consumer"
DLQ_DIR      = ROOT / "lambdas" / "dlq_monitor"
BUILD_DIR    = ROOT / "build"


def banner(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print('='*60)


def wait(msg, seconds=10):
    print(f"  Waiting {seconds}s for {msg}...", end="", flush=True)
    time.sleep(seconds)
    print(" done")


# ============================================================
# CLIENTS
# ============================================================
session  = boto3.Session(region_name=REGION)
sqs      = session.client("sqs")
iam      = session.client("iam")
lam      = session.client("lambda")


# ============================================================
# STEP 1: DLQ
# ============================================================
banner("1/6  Creating SQS Dead-Letter Queue")

resp = sqs.create_queue(
    QueueName=DLQ_QUEUE,
    Attributes={"MessageRetentionPeriod": "1209600"}   # 14 days
)
DLQ_URL = resp["QueueUrl"]

DLQ_ARN = sqs.get_queue_attributes(
    QueueUrl=DLQ_URL,
    AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]

print(f"  DLQ_URL = {DLQ_URL}")
print(f"  DLQ_ARN = {DLQ_ARN}")


# ============================================================
# STEP 2: Main queue with redrive policy
# ============================================================
banner("2/6  Creating main SQS queue")

redrive = json.dumps({
    "deadLetterTargetArn": DLQ_ARN,
    "maxReceiveCount":     MAX_RECEIVES
})

resp = sqs.create_queue(
    QueueName=MAIN_QUEUE,
    Attributes={
        "VisibilityTimeout":        "60",
        "MessageRetentionPeriod":   "345600",   # 4 days
        "RedrivePolicy":            redrive
    }
)
MAIN_URL = resp["QueueUrl"]

MAIN_ARN = sqs.get_queue_attributes(
    QueueUrl=MAIN_URL,
    AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]

print(f"  MAIN_URL = {MAIN_URL}")
print(f"  MAIN_ARN = {MAIN_ARN}")


# ============================================================
# STEP 3: IAM role
# ============================================================
banner("3/6  Creating Lambda IAM role")

trust = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect":    "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action":    "sts:AssumeRole"
    }]
})

try:
    iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=trust
    )
    print(f"  Created role: {ROLE_NAME}")
except iam.exceptions.EntityAlreadyExistsException:
    print(f"  Role already exists, continuing")

for policy in [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    "arn:aws:iam::aws:policy/CloudWatchFullAccess",
]:
    iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=policy)
    print(f"  Attached: {policy.split('/')[-1]}")

ROLE_ARN = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
print(f"  ROLE_ARN = {ROLE_ARN}")

# IAM roles need ~10s to propagate before Lambda can use them
wait("IAM role to propagate", 12)


# ============================================================
# STEP 4: Package Lambdas into zip files
# ============================================================
banner("4/6  Packaging Lambda functions")

BUILD_DIR.mkdir(exist_ok=True)

def build_zip(source_dir: Path, zip_path: Path):
    """
    Install dependencies into a temp folder, copy the .py files,
    zip everything up. No bash needed.
    """
    tmp = BUILD_DIR / source_dir.name
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    # Install psycopg2-binary into the tmp folder
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "psycopg2-binary",
        "--target", str(tmp),
        "--quiet"
    ])

    # Copy all .py files from the Lambda source directory
    for py_file in source_dir.glob("*.py"):
        shutil.copy(py_file, tmp / py_file.name)

    # Zip everything
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in tmp.rglob("*"):
            if file.is_file():
                zf.write(file, file.relative_to(tmp))

    print(f"  Built {zip_path.name} ({zip_path.stat().st_size // 1024} KB)")


CONSUMER_ZIP = BUILD_DIR / "consumer.zip"
DLQ_MON_ZIP  = BUILD_DIR / "dlq_monitor.zip"

build_zip(CONSUMER_DIR, CONSUMER_ZIP)
build_zip(DLQ_DIR,      DLQ_MON_ZIP)


# ============================================================
# STEP 5: Create / update Lambda functions
# ============================================================
banner("5/6  Deploying Lambda functions")

ENV_VARS = {
    "Variables": {
        "DB_HOST":      DB_HOST,
        "DB_NAME":      DB_NAME,
        "DB_USER":      DB_USER,
        "DB_PASSWORD":  DB_PASSWORD,
        "DB_PORT":      DB_PORT,
        "QUEUE_URL":    MAIN_URL,
        "DLQ_URL":      DLQ_URL,
        "MAX_RECEIVES": MAX_RECEIVES,
    }
}

def deploy_lambda(name, zip_path, handler):
    code = zip_path.read_bytes()
    try:
        lam.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler=handler,
            Code={"ZipFile": code},
            Timeout=30,
            MemorySize=256,
            Environment=ENV_VARS,
        )
        print(f"  Created Lambda: {name}")
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=name, ZipFile=code)
        lam.update_function_configuration(
            FunctionName=name,
            Environment=ENV_VARS,
        )
        print(f"  Updated Lambda: {name}")

deploy_lambda(CONSUMER_FN, CONSUMER_ZIP, "consumer_lambda.lambda_handler")
deploy_lambda(DLQ_MON_FN,  DLQ_MON_ZIP,  "dlq_monitor_lambda.lambda_handler")

wait("Lambda deployments to stabilise", 10)


# ============================================================
# STEP 6: Event source mappings
# ============================================================
banner("6/6  Wiring SQS -> Lambda")

def create_mapping(function_name, queue_arn, batch_size, extra=None):
    kwargs = dict(
        FunctionName=function_name,
        EventSourceArn=queue_arn,
        BatchSize=batch_size,
        Enabled=True,
    )
    if extra:
        kwargs.update(extra)
    try:
        lam.create_event_source_mapping(**kwargs)
        print(f"  Mapped {queue_arn.split(':')[-1]} -> {function_name}")
    except lam.exceptions.ResourceConflictException:
        print(f"  Mapping already exists for {function_name}, skipping")

create_mapping(
    CONSUMER_FN, MAIN_ARN, 5,
    {"FunctionResponseTypes": ["ReportBatchItemFailures"]}
)
create_mapping(DLQ_MON_FN, DLQ_ARN, 5)


# ============================================================
# DONE
# ============================================================
print(f"""
{'='*60}
  ALL DONE — copy these for the next steps:

  export QUEUE_URL="{MAIN_URL}"
  export DLQ_URL="{DLQ_URL}"
  export DB_HOST="{DB_HOST}"
  export DB_NAME="{DB_NAME}"
  export DB_USER="{DB_USER}"
  export DB_PASSWORD="{DB_PASSWORD}"
  export AWS_REGION="{REGION}"
{'='*60}

Next steps:
  1. Apply schema in pgAdmin (run sql/schema.sql)
  2. Test: python producer/producer.py submit --type echo --payload "{{}}"
  3. Dashboard: cd dashboard && python app.py
""")