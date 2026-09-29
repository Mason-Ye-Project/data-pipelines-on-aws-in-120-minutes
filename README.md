# Data Pipelines on AWS in 120 Minutes — companion

This repository holds the tested companion for the book *Data Pipelines on AWS in 120 Minutes* by Mason Ye: a small, bounded batch pipeline around synthetic order data. You deploy it with one CloudFormation stack and drive it with one script, then tear it down.

The pipeline: a raw CSV batch in a private S3 bucket → a Python Lambda that validates and transforms it into curated JSON Lines (plus a quarantine file of rejected rows and a receipt) → a Glue Data Catalog table → an Amazon Athena reconciliation query → all orchestrated by an AWS Step Functions Standard state machine.

> The code here is licensed under 0BSD (see `LICENSE-CODE.txt`). The book's prose and figures are not. See `NOTICE` for trademarks and the AI disclosure.

## What you need

- **An AWS account you may experiment in**, and permission to deploy and delete a CloudFormation stack containing an S3 bucket and bucket policy, a Lambda function with a role and log group, a Glue database and table, an Athena workgroup, and a Step Functions state machine with a role. Prefer a non-root IAM user or role; the distributed scripts refuse to run as the account root and recommend an authorized IAM identity. Arranging these deploy-time permissions is your responsibility.
- **AWS CLI v2**, configured so `aws sts get-caller-identity` returns your identity.
- **Python 3.12** (or compatible) and a Bash-like shell. On Windows, use WSL.
- The **Region is `us-east-1`**. The driver pins it; setting a different `AWS_REGION` does not move the lab.

## The command sequence

Run everything from the repository root:

```bash
python3 pipelinelab.py setup                 # build + validate template, create the stack
python3 pipelinelab.py status                # repeat until the stack reads CREATE_COMPLETE
python3 pipelinelab.py upload  --batch demo-a # create-only upload of fixtures/orders.csv
python3 pipelinelab.py invoke  --batch demo-a # run the Lambda directly; prints the summary
python3 pipelinelab.py query   --batch demo-a # run the Athena reconciliation query
python3 pipelinelab.py status                # read the query result (8 rows, 27449 cents) and bytes scanned
python3 pipelinelab.py run     --batch demo-a # run the orchestrated Step Functions workflow
python3 pipelinelab.py status                # confirm SUCCEEDED and reconciled

# Failure and recovery demo:
python3 pipelinelab.py run     --batch missing-a   # fails: no raw uploaded
python3 pipelinelab.py status                      # shows FAILED
python3 pipelinelab.py upload  --batch missing-a   # now provide the input
python3 pipelinelab.py run     --batch missing-a   # succeeds after the failed run is terminal
python3 pipelinelab.py status

# Cleanup (two steps, with a real ~40-second drain window between them):
python3 pipelinelab.py cleanup               # stop work; then wait out the quiet window
python3 pipelinelab.py cleanup               # verify ownership, empty owned prefixes, delete the stack
python3 pipelinelab.py status                # repeat until it reports CLEANUP PASS
```

The fixed fixture yields 12 source rows → 8 accepted, 4 rejected, 27,449 accepted cents.

## What the driver does and does not guarantee

- It records this run's resource names in a **private local state file, `.pipeline-state.json`**, in the working directory. That file is git-ignored and must not be committed or shared; it is not part of the published repository.
- It applies **bounded caps** per lab instance (at most 6 uploads, 10 direct invokes, 20 queries, 20 workflow executions, 500 total CLI calls, within a 2-hour window). These bound the driver's own work; ad-hoc AWS CLI or Athena-console commands you run yourself are **not** counted.
- Before a writer command it makes a **best-effort precheck** that no workflow is already running. This assumes one operator running one command at a time; it is **not** a distributed lock, so do not run overlapping commands or share the lab bucket with other writers.
- **Cleanup** verifies the bucket's ownership tags and refuses on unexpected contents, but within the lab's known prefixes it deletes what it finds. Keep the lab bucket exclusive to the lab. A delete can fail and leave resources; confirm with `status` rather than assuming.

## Cost and safety

The lab uses only tiny synthetic data, a private bucket (Block Public Access on, ACLs disabled, default encryption, TLS-only policy), and short-lived resources. The whole run is a small rate-based cost estimate — conservatively under about US$0.50, including failed attempts — not a finalized bill; no software or AWS control provides a hard dollar cap, so set an AWS Budget/alert. Confirm current prices on the AWS pricing pages.

## Tests

```bash
python3 -m pytest        # or: python3 test_transform.py ; python3 test_deployment.py
```

`test_transform.py` exercises the pure validation/transformation core (no AWS). `test_deployment.py` checks the generated template.

## Repository contents

- `transform.py` — pure validation + transformation (no AWS).
- `handler.py` — the Lambda entry point (reads raw, calls `transform`, writes outputs).
- `template.py` — generates `build/template.json` from the handler sources.
- `pipelinelab.py` — the runtime driver (`setup/status/upload/invoke/query/run/cleanup`).
- `test_transform.py`, `test_deployment.py` — tests.
- `fixtures/orders.csv` — the 12-row synthetic batch.

## Reference-run scope

A bounded publisher reference run exercised the deployed Lambda and Step Functions runtime roles and confirmed cleanup. It used an already-authorized console session through a private operator adapter; the distributed controller still refuses root. The reader deployment identity policy was documentation-reviewed, not live-validated under a non-root deployment principal. Private state and that operator adapter are not distributed.
