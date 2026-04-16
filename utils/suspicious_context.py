from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class SuspiciousContextQuery:
    dataset_dir: str
    iban: str | None = None
    biotag: str | None = None
    fraud_report_path: str | None = None


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_transactions(transactions_path: Path) -> list[dict[str, Any]]:
    with transactions_path.open(encoding="utf-8", newline="") as handle:
        rows = []
        for row in csv.DictReader(handle):
            rows.append(
                {
                    **row,
                    "amount": float(row["amount"]),
                    "balance_after": float(row["balance_after"]),
                    "timestamp": datetime.fromisoformat(row["timestamp"]),
                }
            )
        return rows


def _default_fraud_report_path(dataset_path: Path) -> Path:
    dataset_slug = dataset_path.name.lower().replace(" ", "_")
    return Path("output") / f"{dataset_slug}_fraud_report.json"


def _index_biotag_by_iban(transactions: Iterable[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for tx in transactions:
        sender_iban = tx.get("sender_iban")
        sender_id = tx.get("sender_id")
        if sender_iban and sender_id and sender_iban not in mapping:
            mapping[sender_iban] = sender_id
    return mapping


def _index_transactions_by_sender_id(
    transactions: Iterable[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_sender: dict[str, list[dict[str, Any]]] = {}
    for tx in transactions:
        by_sender.setdefault(tx["sender_id"], []).append(tx)
    for sender_id in by_sender:
        by_sender[sender_id].sort(key=lambda r: r["timestamp"])
    return by_sender


def _normalize(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [_normalize(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in obj.items()}
    return obj


def _extract_suspicious_transaction_reasons(
    fraud_report: dict[str, Any] | None,
) -> dict[str, list[str]]:
    reasons: dict[str, list[str]] = {}
    if not fraud_report:
        return reasons

    patterns = fraud_report.get("patterns") or {}
    if not isinstance(patterns, dict):
        return reasons

    for pattern_key, items in patterns.items():
        if not isinstance(items, list):
            continue

        for item in items:
            txs: list[dict[str, Any]] = []
            if pattern_key == "ping_pong_movements":
                txs = item.get("transactions_during_trip") or []
            elif pattern_key.endswith("_transactions") or pattern_key.endswith("_mismatches"):
                tx = item.get("transaction")
                if tx:
                    txs = [tx]
            elif pattern_key.endswith("_sequences"):
                txs = item.get("transactions") or []

            for tx in txs:
                tx_id = tx.get("transaction_id")
                if not tx_id:
                    continue
                reasons.setdefault(tx_id, [])
                if pattern_key not in reasons[tx_id]:
                    reasons[tx_id].append(pattern_key)

    return reasons


def _filter_patterns_for_biotag(
    fraud_report: dict[str, Any] | None, biotag: str
) -> dict[str, list[dict[str, Any]]]:
    if not fraud_report:
        return {}
    patterns = fraud_report.get("patterns") or {}
    if not isinstance(patterns, dict):
        return {}

    filtered: dict[str, list[dict[str, Any]]] = {}
    for key, items in patterns.items():
        if not isinstance(items, list):
            continue
        user_items = [it for it in items if it.get("biotag") == biotag]
        if user_items:
            filtered[key] = user_items
    return filtered


def _extract_suspicious_location_events(
    patterns_for_user: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """
    Returns only the *suspect* location-related events derived from the fraud report,
    not the entire BioTag GPS history.
    """
    events: list[dict[str, Any]] = []

    for item in patterns_for_user.get("ping_pong_movements", []):
        events.append(
            {
                "pattern": "ping_pong_movements",
                "home_city": item.get("home_city"),
                "visited_city": item.get("visited_city"),
                "timeline": item.get("timeline"),
                "movement_segments": item.get("movement_segments"),
                "days_a_to_b": item.get("days_a_to_b"),
                "days_b_to_a": item.get("days_b_to_a"),
                "days_a_to_return_a": item.get("days_a_to_return_a"),
            }
        )

    for item in patterns_for_user.get("gps_transaction_mismatches", []):
        events.append(
            {
                "pattern": "gps_transaction_mismatches",
                "transaction_id": (item.get("transaction") or {}).get("transaction_id"),
                "merchant_city": (item.get("transaction") or {}).get("merchant_city"),
                "gps_city": (item.get("transaction") or {}).get("inferred_city_from_biotag"),
                "timestamp": (item.get("transaction") or {}).get("timestamp"),
            }
        )

    for item in patterns_for_user.get("gps_dark_period_transactions", []):
        tx = item.get("transaction") or {}
        events.append(
            {
                "pattern": "gps_dark_period_transactions",
                "transaction_id": tx.get("transaction_id"),
                "timestamp": tx.get("timestamp"),
                "gps_gap_hours": tx.get("gps_gap_hours"),
                "gps_city": tx.get("inferred_city_from_biotag"),
            }
        )

    # Add other pattern-specific location signals here if you later enable them.
    return events


def get_user_suspicious_context(query: SuspiciousContextQuery) -> dict[str, Any]:
    """
    Runtime-callable method.

    - Input: dataset path + user identifier (iban or biotag)
    - Output: only suspicious info: suspicious transactions, suspicious emails/SMS,
      and *suspect* location events (derived from fraud report).
    """
    if not query.iban and not query.biotag:
        raise ValueError("Provide either iban or biotag.")

    dataset_path = Path(query.dataset_dir)
    users = _load_json(dataset_path / "users.json")
    transactions = _load_transactions(dataset_path / "transactions.csv")

    mails_processed_path = dataset_path / "mails_processed.json"
    sms_processed_path = dataset_path / "sms_processed.json"
    mails_processed = _load_json(mails_processed_path) if mails_processed_path.exists() else []
    sms_processed = _load_json(sms_processed_path) if sms_processed_path.exists() else []

    fraud_path = (
        Path(query.fraud_report_path)
        if query.fraud_report_path
        else _default_fraud_report_path(dataset_path)
    )
    fraud_report = _load_json(fraud_path) if fraud_path.exists() else None

    biotag_by_iban = _index_biotag_by_iban(transactions)
    biotag = query.biotag or biotag_by_iban.get(query.iban or "")
    if not biotag:
        raise ValueError("Could not resolve biotag for the provided identifier.")

    # resolve user object
    user_obj = None
    if query.iban:
        user_obj = next((u for u in users if u.get("iban") == query.iban), None)
    if not user_obj:
        # fallback: match by sender_iban seen for this biotag
        sender_tx = next((t for t in transactions if t.get("sender_id") == biotag), None)
        if sender_tx:
            user_obj = next((u for u in users if u.get("iban") == sender_tx.get("sender_iban")), None)

    if not user_obj:
        user_obj = {"iban": query.iban, "biotag": biotag}

    # suspicious transactions from fraud report
    tx_by_sender = _index_transactions_by_sender_id(transactions)
    reasons_by_tx_id = _extract_suspicious_transaction_reasons(fraud_report)

    user_transactions = tx_by_sender.get(biotag, [])
    suspicious_transactions = []
    for tx in user_transactions:
        reasons = reasons_by_tx_id.get(tx["transaction_id"], [])
        if not reasons:
            continue
        suspicious_transactions.append(
            _normalize(
                {
                    **tx,
                    "timestamp": tx["timestamp"],
                    "suspect_reasons": reasons,
                }
            )
        )

    # suspicious emails (receiver_email or receiver_name match)
    first_name = (user_obj.get("first_name") or "").strip()
    last_name = (user_obj.get("last_name") or "").strip()
    full_name = f"{first_name} {last_name}".strip().lower()
    expected_email_prefix = f"{first_name}.{last_name}".lower()

    suspicious_emails = []
    for email in mails_processed:
        if email.get("suspect") is not True:
            continue
        receiver_email = (email.get("receiver_email") or "").lower()
        receiver_name = (email.get("receiver_name") or "").lower()
        if expected_email_prefix and expected_email_prefix in receiver_email:
            suspicious_emails.append(email)
            continue
        if full_name and full_name == receiver_name.strip():
            suspicious_emails.append(email)

    # suspicious sms: keep only suspect==true and try to assign by name match
    suspicious_sms = []
    for sms in sms_processed:
        if sms.get("suspect") is not True:
            continue
        content = (sms.get("content") or "").lower()
        if first_name and first_name.lower() in content:
            suspicious_sms.append(sms)

    patterns_for_user = _filter_patterns_for_biotag(fraud_report, biotag)
    suspicious_location_events = _extract_suspicious_location_events(patterns_for_user)

    return {
        "user": {
            "first_name": user_obj.get("first_name"),
            "last_name": user_obj.get("last_name"),
            "iban": user_obj.get("iban"),
            "biotag": biotag,
            "residence": user_obj.get("residence"),
        },
        "suspicious": {
            "transactions": suspicious_transactions,
            "emails": suspicious_emails,
            "sms": suspicious_sms,
            "locations": suspicious_location_events,
            "patterns_raw": patterns_for_user,
        },
        "sources": {
            "dataset_dir": str(dataset_path),
            "fraud_report_path": str(fraud_path) if fraud_report else None,
        },
    }

