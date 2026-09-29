#!/usr/bin/env python3
"""Bounded deployment and execution commands for the synthetic batch pipeline."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

from template import build
from transform import check_batch_id, transform

HERE = Path(__file__).resolve().parent
STATE = Path(".pipeline-state.json")
OWNER = "data-pipelines-120-book"
LIMITS = {"calls": 500, "uploads": 6, "invokes": 10, "queries": 20, "executions": 20}
PREFIXES = ("raw/", "curated/", "quarantine/", "receipts/", "results/")


class LabError(Exception):
    pass


class AwsError(LabError):
    def __init__(self, code):
        self.code = code
        super().__init__(f"AWS operation failed: {code}. Private error details suppressed.")


def save(state):
    temp = STATE.with_suffix(".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(state, stream, indent=2)
    os.replace(temp, STATE)


def load():
    if not STATE.exists():
        raise LabError("No local state. Run setup first from the same directory.")
    state = json.loads(STATE.read_text())
    if state.get("owner") != OWNER or not re.fullmatch(r"dp120-[a-f0-9]{12}", state.get("name", "")):
        raise LabError("Invalid owner or resource name in local state.")
    if state.get("region") != "us-east-1":
        raise LabError("Unexpected region in local state.")
    return state


def spend(state, key, amount=1):
    count = state.setdefault("usage", {}).get(key, 0)
    if count + amount > LIMITS[key]:
        raise LabError(f"Lab {key} cap reached. Retain state; do not reset limits.")
    state["usage"][key] = count + amount
    save(state)


def aws(state, service, operation, payload=None, extra=None, cleanup=False):
    if not cleanup:
        spend(state, "calls")
    command = ["aws", service, operation, "--region", state["region"], "--output", "json",
               "--no-cli-pager", "--cli-connect-timeout", "10", "--cli-read-timeout", "40"]
    if payload is not None:
        command += ["--cli-input-json", json.dumps(payload)]
    command += extra or []
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=100,
                                env=dict(os.environ, AWS_PAGER="", AWS_MAX_ATTEMPTS="2", AWS_RETRY_MODE="standard"))
    except (OSError, subprocess.TimeoutExpired):
        raise LabError("CLI failed or timed out. Keep state and inspect stack status before retrying.") from None
    if result.returncode:
        if operation == "describe-stacks" and re.search(r"Stack with id .+ does not exist", result.stderr):
            raise AwsError("StackNotFound")
        found = re.search(r"An error occurred \(([A-Za-z0-9_.-]+)\)", result.stderr)
        raise AwsError(found.group(1) if found else "CLIError")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def identity(state, cleanup=False):
    got = aws(state, "sts", "get-caller-identity", cleanup=cleanup)
    if got.get("Arn", "").endswith(":root"):
        raise LabError("Use an authorized IAM user or role, not root.")
    if state.get("account") and state["account"] != got.get("Account"):
        raise LabError("Current account differs from the lab's creation account.")
    state["account"] = got["Account"]
    save(state)


def stack(state, cleanup=False):
    result = aws(state, "cloudformation", "describe-stacks", {"StackName": state["name"]}, cleanup=cleanup)
    record = result["Stacks"][0]
    tags = {t["Key"]: t["Value"] for t in record.get("Tags", [])}
    if tags.get("BookLab") != OWNER or tags.get("LabRun") != state["name"]:
        raise LabError("Stack ownership mismatch. Refusing to change resources.")
    if record["StackStatus"] == "CREATE_COMPLETE":
        outputs = {x["OutputKey"]: x["OutputValue"] for x in record.get("Outputs", [])}
        if outputs.get("Bucket") != state["name"] or outputs.get("FunctionName") != state["name"] or outputs.get("Workgroup") != state["name"]:
            raise LabError("Unexpected stack outputs.")
        state["outputs"] = outputs
        save(state)
    return record


def ready(state):
    if stack(state)["StackStatus"] != "CREATE_COMPLETE":
        raise LabError("Stack is not ready. Use status; clean up a failed stack.")
    return state["outputs"]


def setup():
    if STATE.exists():
        raise LabError("Local state exists. Resume or clean this experiment rather than overwrite it.")
    suffix = uuid.uuid4().hex[:12]
    state = {"owner": OWNER, "name": "dp120-" + suffix, "database": "dp120_" + suffix,
             "region": "us-east-1", "created": int(time.time()), "usage": {}, "executions": [], "queries": []}
    save(state)
    identity(state)
    template = build()
    body = json.dumps(template)
    if len(body.encode()) > 51_200:
        raise LabError("Template exceeds inline CloudFormation limit.")
    aws(state, "cloudformation", "validate-template", {"TemplateBody": body})
    aws(state, "cloudformation", "create-stack", {
        "StackName": state["name"], "TemplateBody": body, "Capabilities": ["CAPABILITY_IAM"],
        "Parameters": [{"ParameterKey": "LabName", "ParameterValue": state["name"]},
                       {"ParameterKey": "DatabaseName", "ParameterValue": state["database"]}],
        "Tags": [{"Key": "BookLab", "Value": OWNER}, {"Key": "LabRun", "Value": state["name"]}],
        "TimeoutInMinutes": 10, "OnFailure": "ROLLBACK"})
    print("Stack creation requested. Use status until CREATE_COMPLETE; no scheduled producers were created.")


def upload(state, batch_id):
    out = ready(state)
    raw = (HERE / "fixtures/orders.csv").read_bytes()
    transform(raw, batch_id)
    spend(state, "uploads")
    try:
        aws(state, "s3api", "put-object", {"Bucket": out["Bucket"], "Key": f"raw/{batch_id}/orders.csv",
            "IfNoneMatch": "*", "ContentType": "text/csv", "ServerSideEncryption": "AES256"},
            extra=["--body", str(HERE / "fixtures/orders.csv")])
    except AwsError as exc:
        if exc.code == "PreconditionFailed":
            raise LabError("That immutable batch already exists. Reuse it without uploading, or choose a new batch ID.") from None
        raise
    print("UPLOAD PASS: synthetic raw batch created without overwriting an existing batch.")


def running_executions(state, outputs, cleanup=False):
    result = aws(state, "stepfunctions", "list-executions", {"stateMachineArn": outputs["WorkflowArn"],
                 "statusFilter": "RUNNING", "maxResults": 25}, cleanup=cleanup)
    if result.get("nextToken"):
        raise LabError("Unexpected execution volume. Inspect this lab privately before proceeding.")
    return result.get("executions", [])


def invoke(state, batch_id):
    out = ready(state)
    if running_executions(state, out):
        raise LabError("A workflow is running. Do not overlap writers; wait for its terminal status.")
    spend(state, "invokes")
    target = HERE / "build/invoke-response.json"
    target.parent.mkdir(exist_ok=True)
    # Lambda's CLI customization requires explicit --function-name even when
    # generic --cli-input-json would carry FunctionName for most AWS commands.
    result = aws(state, "lambda", "invoke", extra=["--function-name", out["FunctionName"],
        "--invocation-type", "RequestResponse", "--payload", json.dumps({"batch_id": batch_id}),
        "--cli-binary-format", "raw-in-base64-out", str(target)])
    if result.get("FunctionError"):
        raise LabError("Function failed. Inspect owned Lambda logs privately; failure is not a quarantined record.")
    response = json.loads(target.read_text())
    print(json.dumps({k: response[k] for k in ["batch_id", "source_rows", "accepted_rows", "rejected_rows", "accepted_amount_cents"]}, indent=2))


def run(state, batch_id):
    out = ready(state)
    if running_executions(state, out):
        raise LabError("Wait for the current workflow before starting another run.")
    spend(state, "executions")
    name = f"batch-{state['usage']['executions']:03d}"
    result = aws(state, "stepfunctions", "start-execution", {"stateMachineArn": out["WorkflowArn"],
         "name": name, "input": json.dumps({"batch_id": batch_id})})
    state["executions"].append(result["executionArn"])
    save(state)
    print(f"Workflow {name} started. Use status. Query success alone does not pass reconciliation.")


def query(state, batch_id):
    out = ready(state)
    if running_executions(state, out):
        raise LabError("Wait for the writer before querying curated data.")
    spend(state, "queries")
    sql = f"SELECT count(*) AS accepted_rows, coalesce(sum(amount_cents),0) AS accepted_amount_cents FROM orders WHERE batch_id = '{batch_id}'"
    result = aws(state, "athena", "start-query-execution", {"QueryString": sql,
        "QueryExecutionContext": {"Database": out["DatabaseName"]}, "WorkGroup": out["Workgroup"],
        "ResultReuseConfiguration": {"ResultReuseByAgeConfiguration": {"Enabled": False}}})
    state["queries"].append(result["QueryExecutionId"])
    save(state)
    print("Bounded Athena query started. Use status to inspect count, sum and scanned bytes.")


def status(state):
    try:
        record = stack(state, cleanup=True)
    except AwsError as exc:
        if exc.code == "StackNotFound" and state.get("deleting"):
            state["closed"] = True
            save(state)
            print("CLEANUP PASS: book-owned CloudFormation stack deleted.")
            return
        raise
    print("Stack:", record["StackStatus"])
    for arn in state["executions"][-3:]:
        result = aws(state, "stepfunctions", "describe-execution", {"executionArn": arn})
        report = {"workflow": result["status"]}
        if result["status"] == "SUCCEEDED":
            output = json.loads(result["output"])
            report.update({k: output[k] for k in ["batch_id", "source_rows", "accepted_rows", "rejected_rows",
                                                 "accepted_amount_cents", "reconciliation"]})
        print(json.dumps(report, indent=2))
    for qid in state["queries"][-3:]:
        execution = aws(state, "athena", "get-query-execution", {"QueryExecutionId": qid})["QueryExecution"]
        report = {"query": execution["Status"]["State"], "bytesScanned": execution.get("Statistics", {}).get("DataScannedInBytes", 0)}
        if report["query"] == "SUCCEEDED":
            rows = aws(state, "athena", "get-query-results", {"QueryExecutionId": qid, "MaxResults": 2})["ResultSet"]["Rows"]
            report["rows"] = [[cell.get("VarCharValue") for cell in row["Data"]] for row in rows]
        print(json.dumps(report, indent=2))


def cleanup(state):
    record = stack(state, cleanup=True)
    if record["StackStatus"].endswith("IN_PROGRESS"):
        raise LabError("Wait for the stack operation to finish before cleanup.")
    out = state.get("outputs")
    if out:
        # Stop work before changing or deleting its inputs and outputs.
        for execution in running_executions(state, out, cleanup=True):
            aws(state, "stepfunctions", "stop-execution", {"executionArn": execution["executionArn"],
                "error": "LabCleanup", "cause": "Operator requested bounded lab cleanup"}, cleanup=True)
        active_queries = []
        # Include .sync query executions, which may not be in local direct-query records.
        listing = aws(state, "athena", "list-query-executions", {"WorkGroup": out["Workgroup"], "MaxResults": 50}, cleanup=True)
        if listing.get("NextToken"):
            raise LabError("Unexpected query volume; cleanup requires private inspection.")
        for qid in listing.get("QueryExecutionIds", []):
            status_value = aws(state, "athena", "get-query-execution", {"QueryExecutionId": qid}, cleanup=True)["QueryExecution"]["Status"]["State"]
            if status_value in ("RUNNING", "QUEUED"):
                aws(state, "athena", "stop-query-execution", {"QueryExecutionId": qid}, cleanup=True)
                active_queries.append(qid)
        if active_queries or running_executions(state, out, cleanup=True):
            raise LabError("Stop requests sent. Wait for terminal statuses and run cleanup again.")
        # Lambda's max run time is30s. A stopped workflow may leave its invocation finishing.
        if not state.get("cleanup_quiet_since"):
            state["cleanup_quiet_since"] = int(time.time())
            save(state)
            raise LabError("Writers stopped. Wait40 seconds for in-flight Lambda before running cleanup again.")
        if time.time() - state["cleanup_quiet_since"] < 40:
            raise LabError("Wait until the40-second quiet window ends, then run cleanup again.")
    bucket = state["name"]
    try:
        bucket_tags = aws(state, "s3api", "get-bucket-tagging", {"Bucket": bucket}, cleanup=True)
    except AwsError as exc:
        if exc.code != "NoSuchBucket":
            raise
    else:
        actual = {t["Key"]: t["Value"] for t in bucket_tags["TagSet"]}
        if actual.get("BookLab") != OWNER or actual.get("LabRun") != state["name"]:
            raise LabError("Bucket ownership mismatch; nothing will be deleted.")
        versioning = aws(state, "s3api", "get-bucket-versioning", {"Bucket": bucket}, cleanup=True)
        if versioning.get("Status"):
            raise LabError("Unexpected versioned bucket. This nonversioned lab cleanup refuses it.")
        objects = aws(state, "s3api", "list-objects-v2", {"Bucket": bucket, "MaxKeys": 1000}, cleanup=True)
        uploads = aws(state, "s3api", "list-multipart-uploads", {"Bucket": bucket, "MaxUploads": 1000}, cleanup=True)
        keys = [x["Key"] for x in objects.get("Contents", [])] + [x["Key"] for x in uploads.get("Uploads", [])]
        if objects.get("IsTruncated") or uploads.get("IsTruncated") or any(not k.startswith(PREFIXES) for k in keys):
            raise LabError("Unexpected bucket contents or volume. Cleanup refuses deletion.")
        for upload in uploads.get("Uploads", []):
            aws(state, "s3api", "abort-multipart-upload", {"Bucket": bucket, "Key": upload["Key"], "UploadId": upload["UploadId"]}, cleanup=True)
        if objects.get("Contents"):
            deleted = aws(state, "s3api", "delete-objects", {"Bucket": bucket, "Delete": {
                "Objects": [{"Key": x["Key"]} for x in objects["Contents"]], "Quiet": True}}, cleanup=True)
            if deleted.get("Errors"):
                raise LabError("Some owned objects could not be deleted. Retain state and inspect permissions.")
    aws(state, "cloudformation", "delete-stack", {"StackName": state["name"]}, cleanup=True)
    state["deleting"] = True
    save(state)
    print("Owned stack deletion requested. Use status until CLEANUP PASS. Keep local usage state.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "status", "upload", "invoke", "query", "run", "cleanup"])
    parser.add_argument("--batch", default="demo-a")
    args = parser.parse_args()
    check_batch_id(args.batch)
    if args.action == "setup":
        setup()
        return
    state = load()
    if state.get("closed"):
        if args.action == "status":
            print("Lab already confirmed deleted; local usage record retained.")
            return
        raise LabError("Lab closed. Do not reset state to bypass experiment limits.")
    inspection = args.action in ("cleanup", "status")
    if not inspection and (state.get("deleting") or state.get("cleanup_quiet_since")):
        raise LabError("Cleanup started. No new writers may be launched.")
    if not inspection and time.time() - state["created"] > 7200:
        raise LabError("Two-hour experiment window expired. Run cleanup.")
    identity(state, cleanup=inspection)
    if inspection:
        {"status": status, "cleanup": cleanup}[args.action](state)
    else:
        {"upload": upload, "invoke": invoke, "query": query, "run": run}[args.action](state, args.batch)


if __name__ == "__main__":
    try:
        main()
    except (LabError, ValueError, KeyError) as exc:
        print(str(exc) if isinstance(exc, LabError) else "Invalid local data or response; inspect privately.", file=sys.stderr)
        sys.exit(1)
