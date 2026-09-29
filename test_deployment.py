import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pipelinelab as lab
from template import build


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.patch = patch.object(lab, "STATE", Path(self.tmp.name) / "state.json")
        self.patch.start()
        self.state = {"name": "dp120-012345abcdef", "owner": lab.OWNER, "region": "us-east-1",
                      "usage": {}, "queries": [], "executions": []}

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_template_source_runs_same_transform(self):
        source = build()["Resources"]["Transformer"]["Properties"]["Code"]["ZipFile"]
        namespace = {}
        exec(compile(source, "inline_lambda", "exec"), namespace)
        raw = (lab.HERE / "fixtures/orders.csv").read_bytes()
        self.assertEqual(namespace["transform"](raw, "demo-a")[2]["accepted_amount_cents"], 27449)

    def test_workflow_checks_numbers_before_success(self):
        definition = json.loads(build()["Resources"]["Workflow"]["Properties"]["DefinitionString"])
        self.assertEqual(definition["States"]["CheckTotals"]["Default"], "Mismatch")
        self.assertEqual(definition["States"]["Mismatch"]["Type"], "Fail")
        checks = definition["States"]["CheckTotals"]["Choices"][0]["And"]
        self.assertEqual({x["NumericEqualsPath"] for x in checks}, {"$.accepted_rows", "$.accepted_amount_cents"})

    def test_stack_owner_mismatch_is_not_ready(self):
        with patch.object(lab, "aws", return_value={"Stacks": [{"StackStatus": "CREATE_COMPLETE", "Tags": []}]}):
            with self.assertRaises(lab.LabError):
                lab.ready(self.state)

    def test_upload_never_overwrites_raw_batch(self):
        with patch.object(lab, "ready", return_value={"Bucket": self.state["name"]}), patch.object(lab, "aws", side_effect=lab.AwsError("PreconditionFailed")) as call:
            with self.assertRaises(lab.LabError):
                lab.upload(self.state, "demo-a")
        self.assertEqual(call.call_args.args[3]["IfNoneMatch"], "*")

    def test_running_workflow_blocks_manual_writer(self):
        with patch.object(lab, "ready", return_value={}), patch.object(lab, "running_executions", return_value=[{}]), patch.object(lab, "aws") as call:
            with self.assertRaises(lab.LabError):
                lab.invoke(self.state, "demo-a")
            call.assert_not_called()

    def test_cleanup_unknown_prefix_deletes_nothing(self):
        operations = []
        def fake(state, service, op, payload=None, extra=None, cleanup=False):
            operations.append(op)
            if op == "get-bucket-tagging":
                return {"TagSet": [{"Key": "BookLab", "Value": lab.OWNER}, {"Key": "LabRun", "Value": state["name"]}]}
            if op == "list-objects-v2":
                return {"Contents": [{"Key": "unrelated-private-file"}]}
            return {}
        with patch.object(lab, "stack", return_value={"StackStatus": "ROLLBACK_COMPLETE"}), patch.object(lab, "aws", side_effect=fake):
            with self.assertRaises(lab.LabError):
                lab.cleanup(self.state)
        self.assertFalse(any(x.startswith("delete-") for x in operations))

    def test_versioned_bucket_requires_separate_recovery(self):
        def fake(state, service, op, payload=None, extra=None, cleanup=False):
            if op == "get-bucket-tagging":
                return {"TagSet": [{"Key": "BookLab", "Value": lab.OWNER}, {"Key": "LabRun", "Value": state["name"]}]}
            if op == "get-bucket-versioning":
                return {"Status": "Suspended"}
            self.fail("Deletion must not be attempted on an unexpectedly versioned bucket")
        with patch.object(lab, "stack", return_value={"StackStatus": "ROLLBACK_COMPLETE"}), patch.object(lab, "aws", side_effect=fake):
            with self.assertRaises(lab.LabError):
                lab.cleanup(self.state)


if __name__ == "__main__":
    unittest.main()
