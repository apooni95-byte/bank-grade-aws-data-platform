import json
import sys
import glob
from datetime import datetime, timezone

AMOUNT_MIN = -50_000
AMOUNT_MAX = 50_000

REQUIRED_FIELDS = [
    "transaction_id",
    "account_id",
    "product_type",
    "transaction_ts",
    "amount",
    "channel",
]


def load_latest_transactions_file() -> str:
    """
    Finds the newest transactions_YYYY-MM-DD.json in the current folder.
    """
    files = sorted(glob.glob("transactions_*.json"))
    if not files:
        raise FileNotFoundError("No transactions_*.json files found. Run generate_transactions.py first.")
    return files[-1]


def parse_iso_ts(ts: str) -> datetime:
    """
    Parse ISO timestamp produced by datetime.isoformat().
    Handles timestamps with/without timezone info.
    """
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def dq_check(records: list[dict]) -> dict:
    errors = []
    warnings = []

    # 1) Schema checks
    for i, r in enumerate(records):
        missing = [f for f in REQUIRED_FIELDS if f not in r]
        if missing:
            errors.append({"row": i, "type": "missing_fields", "details": missing})

    # If schema is broken badly, stop early
    if errors:
        return {
            "status": "FAIL",
            "total_rows": len(records),
            "valid_rows": 0,
            "invalid_rows": len(records),
            "errors": errors[:50],  # keep report readable
            "warnings": warnings,
        }

    # 2) Null/empty checks
    for i, r in enumerate(records):
        if not str(r["transaction_id"]).strip():
            errors.append({"row": i, "type": "null_transaction_id"})
        if not str(r["account_id"]).strip():
            errors.append({"row": i, "type": "null_account_id"})

    # 3) Duplicate transaction_id
    seen = set()
    dupes = 0
    for r in records:
        tid = r["transaction_id"]
        if tid in seen:
            dupes += 1
        seen.add(tid)
    if dupes > 0:
        errors.append({"type": "duplicate_transaction_id", "count": dupes})

    # 4) Timestamp sanity (no future timestamps)
    now = datetime.now(timezone.utc)
    future_count = 0
    for i, r in enumerate(records):
        try:
            ts = parse_iso_ts(r["transaction_ts"])
            if ts > now:
                future_count += 1
        except Exception:
            errors.append({"row": i, "type": "invalid_timestamp", "value": r["transaction_ts"]})
    if future_count > 0:
        errors.append({"type": "future_timestamps", "count": future_count})

    # 5) Amount bounds
    out_of_bounds = 0
    for i, r in enumerate(records):
        try:
            amt = float(r["amount"])
            if amt < AMOUNT_MIN or amt > AMOUNT_MAX:
                out_of_bounds += 1
        except Exception:
            errors.append({"row": i, "type": "invalid_amount", "value": r["amount"]})
    if out_of_bounds > 0:
        errors.append({"type": "amount_out_of_bounds", "count": out_of_bounds, "range": [AMOUNT_MIN, AMOUNT_MAX]})

    invalid_rows = len(errors)
    status = "PASS" if invalid_rows == 0 else "FAIL"

    return {
        "status": status,
        "checked_at": now.isoformat(),
        "total_rows": len(records),
        "valid_rows": len(records) if status == "PASS" else max(len(records) - invalid_rows, 0),
        "invalid_rows": invalid_rows,
        "errors": errors[:50],
        "warnings": warnings,
    }


def main():
    # Allow optional explicit file path:
    # python3 dq_check.py transactions_2026-02-21.json
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = load_latest_transactions_file()

    with open(path, "r") as f:
        records = json.load(f)

    report = dq_check(records)

    report_path = path.replace("transactions_", "dq_report_")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"DQ Status: {report['status']}")
    print(f"Rows checked: {report['total_rows']}")
    print(f"Report saved: {report_path}")

    # In real pipelines, FAIL should stop the workflow:
    if report["status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
