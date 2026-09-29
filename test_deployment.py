import io
import json
from types import SimpleNamespace
from contextlib import redirect_stderr
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


    def test_setup_preflight_failures_leave_no_persistent_state(self):
        caller = SimpleNamespace(returncode=0, stdout=json.dumps({
            "Account": "example", "Arn": "arn:aws:iam::example:user/reader"}), stderr="")
        root = SimpleNamespace(returncode=0, stdout=json.dumps({
            "Account": "example", "Arn": "arn:aws:iam::example:root"}), stderr="")
        denied = SimpleNamespace(returncode=1, stdout="", stderr="An error occurred (AccessDenied)")
        for responses in ([root], [denied], [caller, denied]):
            with self.subTest(responses=responses), patch.object(lab.subprocess, "run", side_effect=responses) as call:
                with self.assertRaises(lab.LabError):
                    lab.setup()
                self.assertFalse(lab.STATE.exists())
                self.assertFalse(any(c.args[0][2] == "create-stack" for c in call.call_args_list))
        # A subsequent valid attempt can proceed without manually deleting a record.
        ok = SimpleNamespace(returncode=0, stdout="{}", stderr="")
        with patch.object(lab.subprocess, "run", side_effect=[caller, ok, ok]):
            lab.setup()
        state = json.loads(lab.STATE.read_text())
        self.assertEqual(state["usage"]["calls"], 3)
        self.assertEqual(state["account"], "example")

    def test_setup_keeps_state_when_create_result_is_uncertain(self):
        import subprocess
        caller = SimpleNamespace(returncode=0, stdout=json.dumps({
            "Account": "example", "Arn": "arn:aws:iam::example:user/reader"}), stderr="")
        ok = SimpleNamespace(returncode=0, stdout="{}", stderr="")
        def result(command, **kwargs):
            if command[2] == "get-caller-identity":
                return caller
            if command[2] == "validate-template":
                self.assertFalse(lab.STATE.exists())
                return ok
            self.assertEqual(command[2], "create-stack")
            self.assertTrue(lab.STATE.exists())
            raise subprocess.TimeoutExpired(command, 100)
        with patch.object(lab.subprocess, "run", side_effect=result):
            with self.assertRaises(lab.LabError):
                lab.setup()
        self.assertTrue(lab.STATE.exists())
        with self.assertRaisesRegex(lab.LabError, "Local state exists"):
            lab.setup()

    def test_two_hour_mark_warns_without_blocking_reader(self):
        self.state["created"] = 0
        lab.save(self.state)
        warning = io.StringIO()
        with patch.object(lab.sys, "argv", ["pipelinelab.py", "run"]), \
             patch.object(lab.time, "time", return_value=7201), \
             patch.object(lab, "identity"), patch.object(lab, "run") as run, \
             redirect_stderr(warning):
            lab.main()
        run.assert_called_once()
        self.assertIn("WARNING", warning.getvalue())
        # Warning mode does not remove the independent operation caps.
        self.state["usage"]["executions"] = lab.LIMITS["executions"]
        with self.assertRaises(lab.LabError):
            lab.spend(self.state, "executions")


if __name__ == "__main__":
    unittest.main()
