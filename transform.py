"""Pure validation and transformation for the synthetic orders lab."""
import csv
import datetime as dt
import io
import json
import re

FIELDS = ["order_id", "customer_id", "order_date", "country", "amount_cents"]
MAX_BYTES = 1_000_000
MAX_ROWS = 1000


def check_batch_id(batch_id):
    if not isinstance(batch_id, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", batch_id):
        raise ValueError("batch_id must be 1-40 lowercase letters, digits or hyphens, starting with a letter")
    return batch_id


def transform(raw, batch_id):
    check_batch_id(batch_id)
    if not isinstance(raw, bytes) or len(raw) > MAX_BYTES:
        raise ValueError("input exceeds the one-megabyte contract")
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), strict=True)
    if reader.fieldnames != FIELDS:
        raise ValueError("input schema differs from the exact expected CSV header")
    accepted, rejected, seen = [], [], set()
    for row_number, row in enumerate(reader, start=2):
        if row_number - 1 > MAX_ROWS:
            raise ValueError("input exceeds the 1000-record contract")
        errors = []
        if None in row or any(row.get(field) is None for field in FIELDS):
            errors.append("FIELD_COUNT")
        order_id = row.get("order_id") or ""
        customer_id = row.get("customer_id") or ""
        if not re.fullmatch(r"ORD-[0-9]{4}", order_id):
            errors.append("ORDER_ID")
        if order_id in seen:
            errors.append("DUPLICATE_ORDER_ID")
        if not re.fullmatch(r"CUST-[0-9]{3}", customer_id):
            errors.append("CUSTOMER_ID")
        date = row.get("order_date") or ""
        try:
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
                raise ValueError()
            dt.date.fromisoformat(date)
        except ValueError:
            errors.append("ORDER_DATE")
        country = row.get("country") or ""
        if country not in {"AU", "NZ", "US", "GB"}:
            errors.append("COUNTRY")
        amount = row.get("amount_cents") or ""
        if not re.fullmatch(r"[0-9]{1,8}", amount):
            errors.append("AMOUNT_CENTS")
        # Only accepted identifiers reserve a key; a later corrected valid row may be accepted.
        if errors:
            rejected.append({"batch_id": batch_id, "source_row": row_number, "reasons": errors})
        else:
            seen.add(order_id)
            accepted.append({"batch_id": batch_id, "order_id": order_id, "customer_id": customer_id,
                             "order_date": date, "country": country, "amount_cents": int(amount)})
    if not accepted and not rejected:
        raise ValueError("input contains no data records")
    summary = {"batch_id": batch_id, "source_rows": len(accepted) + len(rejected),
               "accepted_rows": len(accepted), "rejected_rows": len(rejected),
               "accepted_amount_cents": sum(row["amount_cents"] for row in accepted)}
    return accepted, rejected, summary


def json_lines(rows):
    return "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows).encode()
