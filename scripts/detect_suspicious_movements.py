#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LocationPoint:
    biotag: str
    timestamp: datetime
    city: str
    lat: float
    lng: float


@dataclass(frozen=True)
class TransactionRecord:
    transaction_id: str
    sender_id: str
    recipient_id: str
    transaction_type: str
    amount: float
    location: str
    payment_method: str
    sender_iban: str
    recipient_iban: str
    balance_after: float
    description: str
    timestamp: datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Detect suspicious transaction windows based on biotag location "
            "timelines that move from city A to city B and then back to city A."
        )
    )
    parser.add_argument(
        "--dataset",
        default="data/The Truman Show - train",
        help="Path containing users.json, locations.json, and transactions.csv.",
    )
    parser.add_argument(
        "--return-days",
        type=float,
        default=7.0,
        help="Maximum number of days allowed for an A -> B -> A return to be flagged.",
    )
    parser.add_argument(
        "--output",
        help="Optional JSON output file path. Prints to stdout when omitted.",
    )
    return parser.parse_args()


def load_users(users_path: Path) -> dict[str, dict[str, Any]]:
    users = json.loads(users_path.read_text(encoding="utf-8"))
    users_by_iban: dict[str, dict[str, Any]] = {}
    for user in users:
        users_by_iban[user["iban"]] = user
    return users_by_iban


def load_locations(locations_path: Path) -> dict[str, list[LocationPoint]]:
    raw_locations = json.loads(locations_path.read_text(encoding="utf-8"))
    locations_by_biotag: dict[str, list[LocationPoint]] = {}

    for entry in raw_locations:
        point = LocationPoint(
            biotag=entry["biotag"],
            timestamp=datetime.fromisoformat(entry["timestamp"]),
            city=entry["city"],
            lat=float(entry["lat"]),
            lng=float(entry["lng"]),
        )
        locations_by_biotag.setdefault(point.biotag, []).append(point)

    for biotag_locations in locations_by_biotag.values():
        biotag_locations.sort(key=lambda item: item.timestamp)

    return locations_by_biotag


def load_transactions(transactions_path: Path) -> dict[str, list[TransactionRecord]]:
    transactions_by_sender: dict[str, list[TransactionRecord]] = {}
    with transactions_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            transaction = TransactionRecord(
                transaction_id=row["transaction_id"],
                sender_id=row["sender_id"],
                recipient_id=row["recipient_id"],
                transaction_type=row["transaction_type"],
                amount=float(row["amount"]),
                location=row["location"] or "",
                payment_method=row["payment_method"] or "",
                sender_iban=row["sender_iban"],
                recipient_iban=row["recipient_iban"],
                balance_after=float(row["balance_after"]),
                description=row["description"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
            )
            transactions_by_sender.setdefault(transaction.sender_id, []).append(transaction)

    for sender_transactions in transactions_by_sender.values():
        sender_transactions.sort(key=lambda item: item.timestamp)

    return transactions_by_sender


def compress_city_changes(points: list[LocationPoint]) -> list[LocationPoint]:
    changes: list[LocationPoint] = []
    previous_city: str | None = None

    for point in points:
        if point.city != previous_city:
            changes.append(point)
            previous_city = point.city

    return changes


def infer_transaction_city(
    transaction: TransactionRecord,
    locations: list[LocationPoint],
) -> str | None:
    timestamps = [point.timestamp for point in locations]
    insertion_index = bisect_right(timestamps, transaction.timestamp) - 1
    if insertion_index < 0:
        return None
    return locations[insertion_index].city


def build_user_label(user: dict[str, Any] | None, biotag: str) -> str:
    if not user:
        return biotag
    return f"{user['first_name']} {user['last_name']}"


def build_timeline_segments(points: list[LocationPoint]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []

    for index in range(1, len(points)):
        previous_point = points[index - 1]
        current_point = points[index]
        days_between = (
            current_point.timestamp - previous_point.timestamp
        ).total_seconds() / 86400

        segments.append(
            {
                "from_city": previous_point.city,
                "to_city": current_point.city,
                "from_timestamp": previous_point.timestamp.isoformat(),
                "to_timestamp": current_point.timestamp.isoformat(),
                "days_between": round(days_between, 2),
                "movement_label": f"{previous_point.city} -> {current_point.city}",
            }
        )

    return segments


def build_user_timelines(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
) -> list[dict[str, Any]]:
    timelines: list[dict[str, Any]] = []

    for biotag, points in sorted(locations_by_biotag.items()):
        sender_transactions = transactions_by_sender.get(biotag, [])
        user = None
        if sender_transactions:
            user = users_by_iban.get(sender_transactions[0].sender_iban)

        city_changes = compress_city_changes(points)
        timelines.append(
            {
                "biotag": biotag,
                "user": build_user_label(user, biotag),
                "city_changes": [
                    {"timestamp": point.timestamp.isoformat(), "city": point.city}
                    for point in city_changes
                ],
                "movement_segments": build_timeline_segments(city_changes),
            }
        )

    return timelines


def detect_suspicious_windows(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
    max_return_days: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for biotag, points in sorted(locations_by_biotag.items()):
        changes = compress_city_changes(points)
        sender_transactions = transactions_by_sender.get(biotag, [])
        user = None
        if sender_transactions:
            user = users_by_iban.get(sender_transactions[0].sender_iban)

        for index in range(2, len(changes)):
            first = changes[index - 2]
            second = changes[index - 1]
            third = changes[index]

            if first.city != third.city or first.city == second.city:
                continue

            days_away = (third.timestamp - second.timestamp).total_seconds() / 86400
            if days_away > max_return_days:
                continue

            suspicious_transactions = []
            for transaction in sender_transactions:
                if second.timestamp <= transaction.timestamp <= third.timestamp:
                    suspicious_transactions.append(
                        {
                            "transaction_id": transaction.transaction_id,
                            "timestamp": transaction.timestamp.isoformat(),
                            "amount": transaction.amount,
                            "transaction_type": transaction.transaction_type,
                            "description": transaction.description,
                            "merchant_location": transaction.location or None,
                            "payment_method": transaction.payment_method or None,
                            "inferred_city_from_biotag": infer_transaction_city(
                                transaction,
                                points,
                            ),
                        }
                    )

            suspicious_timeline = [first, second, third]
            suspicious_segments = build_timeline_segments(suspicious_timeline)

            results.append(
                {
                    "biotag": biotag,
                    "user": build_user_label(user, biotag),
                    "home_city": first.city,
                    "visited_city": second.city,
                    "baseline_city_seen_at": first.timestamp.isoformat(),
                    "visited_city_at": second.timestamp.isoformat(),
                    "returned_home_at": third.timestamp.isoformat(),
                    "timeline": [
                        {"timestamp": first.timestamp.isoformat(), "city": first.city},
                        {"timestamp": second.timestamp.isoformat(), "city": second.city},
                        {"timestamp": third.timestamp.isoformat(), "city": third.city},
                    ],
                    "movement_segments": suspicious_segments,
                    "days_a_to_b": suspicious_segments[0]["days_between"],
                    "days_b_to_a": suspicious_segments[1]["days_between"],
                    "days_a_to_return_a": round(
                        (third.timestamp - first.timestamp).total_seconds() / 86400, 2
                    ),
                    "transactions_during_trip": suspicious_transactions,
                }
            )

    return results


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)

    users_by_iban = load_users(dataset_path / "users.json")
    locations_by_biotag = load_locations(dataset_path / "locations.json")
    transactions_by_sender = load_transactions(dataset_path / "transactions.csv")

    suspicious_windows = detect_suspicious_windows(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
        max_return_days=args.return_days,
    )
    user_timelines = build_user_timelines(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
    )

    report = {
        "dataset": str(dataset_path),
        "rule": (
            f"Flag A -> B -> A city returns completed within {args.return_days} days "
            "and include transactions that occurred while the user was away."
        ),
        "user_timelines": user_timelines,
        "total_suspicious_windows": len(suspicious_windows),
        "results": suspicious_windows,
    }

    rendered = json.dumps(report, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
