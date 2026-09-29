"""Lambda runtime adapter. Raw batch objects are write-once by the lab uploader."""
import json
import os
from transform import MAX_BYTES, check_batch_id, json_lines, transform


def lambda_handler(event, context):
    import boto3
    from botocore.config import Config

    batch_id = check_batch_id(event.get("batch_id"))
    bucket = os.environ["LAB_BUCKET"]
    key = f"raw/{batch_id}/orders.csv"
    s3 = boto3.client("s3", config=Config(retries={"mode": "standard", "total_max_attempts": 2},
                                         connect_timeout=3, read_timeout=10))
    head = s3.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] > MAX_BYTES:
        raise ValueError("source object exceeds the one-megabyte contract")
    response = s3.get_object(Bucket=bucket, Key=key)
    with response["Body"] as body:
        raw = body.read(MAX_BYTES + 1)
    accepted, rejected, summary = transform(raw, batch_id)
    outputs = [
        (f"quarantine/{batch_id}/rejected.jsonl", json_lines(rejected)),
        (f"curated/{batch_id}/orders.jsonl", json_lines(accepted)),
        (f"receipts/{batch_id}/summary.json", json.dumps(summary, sort_keys=True).encode()),
    ]
    # Fixed keys make retries replace each object's result. These writes are NOT a transaction.
    # The workflow queries only after this function succeeds; external readers must obey that gate.
    for output_key, content in outputs:
        s3.put_object(Bucket=bucket, Key=output_key, Body=content, ContentType="application/json",
                      ServerSideEncryption="AES256")
    print(json.dumps({"event": "batch_complete", **summary}, sort_keys=True))
    return {**summary, "reconciliation_sql":
            "SELECT count(*) AS accepted_rows, coalesce(sum(amount_cents), 0) "
            f"AS accepted_amount_cents FROM orders WHERE batch_id = '{batch_id}'"}
