import csv
import io
from pathlib import Path
import unittest
from transform import check_batch_id, transform, json_lines


class TransformTests(unittest.TestCase):
    def setUp(self):
        self.raw = (Path(__file__).parent / "fixtures/orders.csv").read_bytes()

    def test_reconciliation(self):
        good, bad, result = transform(self.raw, "demo-a")
        self.assertEqual(result, {"batch_id": "demo-a", "source_rows": 12, "accepted_rows": 8,
                                  "rejected_rows": 4, "accepted_amount_cents": 27449})
        self.assertEqual({r for row in bad for r in row["reasons"]},
                         {"AMOUNT_CENTS", "ORDER_DATE", "DUPLICATE_ORDER_ID", "COUNTRY"})
        self.assertEqual(len({r["order_id"] for r in good}), 8)

    def test_replay_semantics(self):
        first = transform(self.raw, "demo-a")
        second = transform(self.raw, "demo-a")
        self.assertEqual(first, second)
        # Same fixed object key on each run replaces the same eight records; it does not append.
        objects = {}
        for data in (first, second):
            objects["curated/demo-a/orders.jsonl"] = json_lines(data[0])
        self.assertEqual(sum(len(v.splitlines()) for v in objects.values()), 8)

    def test_batch_id_rejects_path_and_query_injection(self):
        for value in ["../other", "x' OR 1=1", "a/b", "", None, "A", "x" * 41]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                check_batch_id(value)

    def test_schema_drift_fails_batch(self):
        with self.assertRaises(ValueError):
            transform(self.raw.replace(b"amount_cents", b"amount_dollars"), "demo-a")

    def test_empty_batch_fails(self):
        with self.assertRaises(ValueError):
            transform(self.raw.splitlines()[0] + b"\n", "demo-a")

    def test_size_and_row_limits(self):
        with self.assertRaises(ValueError):
            transform(b"a" * 1_000_001, "demo-a")
        with self.assertRaises(ValueError):
            transform(self.raw.splitlines()[0] + b"\n" + (self.raw.splitlines()[1] + b"\n") * 1001, "demo-a")

    def test_malformed_csv_is_batch_failure(self):
        with self.assertRaises(csv.Error):
            transform(self.raw.splitlines()[0] + b'\n"unterminated', "demo-a")

    def test_extra_field_quarantined_without_sensitive_raw_copy(self):
        data = self.raw.splitlines()[0] + b"\n" + self.raw.splitlines()[1] + b",private-extra\n"
        good, bad, _ = transform(data, "demo-a")
        self.assertEqual(good, [])
        self.assertIn("FIELD_COUNT", bad[0]["reasons"])
        self.assertNotIn(b"private-extra", json_lines(bad))


if __name__ == "__main__":
    unittest.main()
