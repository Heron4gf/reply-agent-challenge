#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class User:
    first_name: str
    last_name: str
    birth_year: int
    salary: float
    job: str
    iban: str
    residence_city: str
    residence_lat: float
    residence_lng: float
    description: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build per-user enriched profiles by merging users, transactions, "
            "GPS locations (BioTag), and processed SMS/emails. Suspicious/banned "
            "items are grouped for LLM consumption."
        )
    )
    parser.add_argument(
        "--dataset",
        default="data/The Truman Show - train",
        help="Dataset folder containing users.json, locations.json, transactions.csv, etc.",
    )
    parser.add_argument(
        "--fraud-report",
        default=None,
        help=(
            "Optional fraud report JSON path produced by detect_suspicious_movements.py. "
            "If omitted, will try output/<dataset>_fraud_report.json."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="output/enriched_users",
        help="Directory where per-user JSON files will be written.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_users(users_path: Path) -> list[User]:
    raw_users = load_json(users_path)
    users: list[User] = []
    for item in raw_users:
        users.append(
            User(
                first_name=item["first_name"],
                last_name=item["last_name"],
                birth_year=int(item["birth_year"]),
                salary=float(item["salary"]),
                job=item["job"],
                iban=item["iban"],
                residence_city=item["residence"]["city"],
                residence_lat=float(item["residence"]["lat"]),
                residence_lng=float(item["residence"]["lng"]),
                description=item["description"],
            )
        )
    return users


def load_transactions(transactions_path: Path) -> list[dict[str, Any]]:
    with transactions_path.open(encoding="utf-8", newline="") as handle:
        return [
            {
                **row,
                "amount": float(row["amount"]),
                "balance_after": float(row["balance_after"]),
                "timestamp": datetime.fromisoformat(row["timestamp"]),
            }
            for row in csv.DictReader(handle)
        ]


def load_locations(locations_path: Path) -> list[dict[str, Any]]:
    raw = load_json(locations_path)
    for item in raw:
        item["timestamp"] = datetime.fromisoformat(item["timestamp"])
        item["lat"] = float(item["lat"])
        item["lng"] = float(item["lng"])
    return raw


def build_default_fraud_report_path(dataset_path: Path) -> Path:
    dataset_slug = dataset_path.name.lower().replace(" ", "_")
    return Path("output") / f"{dataset_slug}_fraud_report.json"


def index_biotag_by_iban(transactions: list[dict[str, Any]]) -> dict[str, str]:
    # sender_iban -> sender_id (biotag)
    mapping: dict[str, str] = {}
    for tx in transactions:
        sender_iban = tx["sender_iban"]
        sender_id = tx["sender_id"]
        if sender_iban and sender_id and sender_iban not in mapping:
            mapping[sender_iban] = sender_id
    return mapping


def index_transactions_by_sender_id(transactions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_sender: dict[str, list[dict[str, Any]]] = {}
    for tx in transactions:
        by_sender.setdefault(tx["sender_id"], []).append(tx)
    for sender_id in by_sender:
        by_sender[sender_id].sort(key=lambda r: r["timestamp"])
    return by_sender


def index_locations_by_biotag(locations: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_biotag: dict[str, list[dict[str, Any]]] = {}
    for item in locations:
        by_biotag.setdefault(item["biotag"], []).append(item)
    for biotag in by_biotag:
        by_biotag[biotag].sort(key=lambda r: r["timestamp"])
    return by_biotag


def index_emails_by_receiver(mails_processed: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_receiver: dict[str, list[dict[str, Any]]] = {}
    for email in mails_processed:
        receiver = (email.get("receiver_email") or "").lower()
        by_receiver.setdefault(receiver, []).append(email)
    return by_receiver


def guess_sms_owner_first_name(sms: dict[str, Any], candidate_first_names: set[str]) -> str | None:
    content = (sms.get("content") or "").lower()
    for first_name in candidate_first_names:
        if first_name.lower() in content:
            return first_name
    return None


def index_sms_by_user_first_name(
    sms_processed: list[dict[str, Any]], users: list[User]
) -> dict[str, list[dict[str, Any]]]:
    candidate_first_names = {u.first_name for u in users}
    by_first_name: dict[str, list[dict[str, Any]]] = {u.first_name: [] for u in users}

    for sms in sms_processed:
        guessed = guess_sms_owner_first_name(sms, candidate_first_names)
        if guessed:
            by_first_name.setdefault(guessed, []).append(sms)

    return by_first_name


def normalize_for_json(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [normalize_for_json(x) for x in obj]
    if isinstance(obj, dict):
        return {k: normalize_for_json(v) for k, v in obj.items()}
    return obj


def extract_suspicious_transaction_index(fraud_report: dict[str, Any] | None) -> dict[str, list[str]]:
    # transaction_id -> list of pattern keys
    reasons: dict[str, list[str]] = {}
    if not fraud_report:
        return reasons

    patterns = fraud_report.get("patterns") or {}
    for pattern_key, items in patterns.items():
        if not isinstance(items, list):
            continue
        for item in items:
            txs = None
            if pattern_key == "ping_pong_movements":
                txs = item.get("transactions_during_trip")
            elif pattern_key.endswith("_transactions") or pattern_key.endswith("_mismatches"):
                txs = [item.get("transaction")] if item.get("transaction") else []
            elif pattern_key.endswith("_sequences"):
                txs = item.get("transactions")

            if not txs:
                continue

            for tx in txs:
                tx_id = tx.get("transaction_id")
                if not tx_id:
                    continue
                reasons.setdefault(tx_id, [])
                if pattern_key not in reasons[tx_id]:
                    reasons[tx_id].append(pattern_key)

    return reasons


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    users = load_users(dataset_path / "users.json")
    transactions = load_transactions(dataset_path / "transactions.csv")
    locations = load_locations(dataset_path / "locations.json")

    mails_processed_path = dataset_path / "mails_processed.json"
    sms_processed_path = dataset_path / "sms_processed.json"
    mails_processed = load_json(mails_processed_path) if mails_processed_path.exists() else []
    sms_processed = load_json(sms_processed_path) if sms_processed_path.exists() else []

    fraud_report_path = (
        Path(args.fraud_report)
        if args.fraud_report
        else build_default_fraud_report_path(dataset_path)
    )
    fraud_report = load_json(fraud_report_path) if fraud_report_path.exists() else None

    suspicious_tx_reasons = extract_suspicious_transaction_index(fraud_report)

    biotag_by_iban = index_biotag_by_iban(transactions)
    transactions_by_sender = index_transactions_by_sender_id(transactions)
    locations_by_biotag = index_locations_by_biotag(locations)
    emails_by_receiver = index_emails_by_receiver(mails_processed)
    sms_by_first_name = index_sms_by_user_first_name(sms_processed, users)

    for user in users:
        biotag = biotag_by_iban.get(user.iban)
        user_transactions = transactions_by_sender.get(biotag, []) if biotag else []
        user_locations = locations_by_biotag.get(biotag, []) if biotag else []

        receiver_email = f"{user.first_name}.{user.last_name}".lower().replace(" ", "-") + "@example.com"
        user_emails = emails_by_receiver.get(receiver_email, [])
        user_sms = sms_by_first_name.get(user.first_name, [])

        enriched_transactions = []
        for tx in user_transactions:
            tx_id = tx["transaction_id"]
            reasons = suspicious_tx_reasons.get(tx_id, [])
            enriched_transactions.append(
                normalize_for_json(
                    {
                        **tx,
                        "timestamp": tx["timestamp"],
                        "suspect": len(reasons) > 0,
                        "suspect_reasons": reasons,
                    }
                )
            )

        suspicious_emails = [e for e in user_emails if e.get("suspect") is True]
        suspicious_sms = [s for s in user_sms if s.get("suspect") is True]

        profile = {
            "user": {
                "first_name": user.first_name,
                "last_name": user.last_name,
                "birth_year": user.birth_year,
                "salary": user.salary,
                "job": user.job,
                "iban": user.iban,
                "residence": {
                    "city": user.residence_city,
                    "lat": user.residence_lat,
                    "lng": user.residence_lng,
                },
                "description": user.description,
                "biotag": biotag,
            },
            "linked_data": {
                "locations": normalize_for_json(user_locations),
                "transactions": enriched_transactions,
                "emails": user_emails,
                "sms": user_sms,
            },
            "banned_suspicious": {
                "transactions": [t for t in enriched_transactions if t.get("suspect") is True],
                "emails": suspicious_emails,
                "sms": suspicious_sms,
            },
            "sources": {
                "dataset": str(dataset_path),
                "fraud_report_used": str(fraud_report_path) if fraud_report else None,
            },
        }

        safe_name = f"{user.first_name}_{user.last_name}".lower().replace(" ", "_")
        out_path = output_dir / f"{safe_name}.json"
        out_path.write_text(json.dumps(profile, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote {len(users)} enriched profiles to {output_dir}")


if __name__ == "__main__":
    main()

