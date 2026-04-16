#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from itertools import pairwise
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
    parser.add_argument(
        "--gps-gap-hours",
        type=float,
        default=12.0,
        help="Flag transactions that happen during a GPS gap longer than this many hours.",
    )
    parser.add_argument(
        "--travel-signal-days",
        type=float,
        default=3.0,
        help="Days window used to decide whether a city change recently signaled travel.",
    )
    parser.add_argument(
        "--late-night-start",
        type=int,
        default=23,
        help="Late-night start hour included in residential anomaly checks.",
    )
    parser.add_argument(
        "--late-night-end",
        type=int,
        default=5,
        help="Late-night end hour included in residential anomaly checks.",
    )
    parser.add_argument(
        "--small-amount-threshold",
        type=float,
        default=50.0,
        help="Maximum amount considered a small test transaction.",
    )
    parser.add_argument(
        "--rapid-sequence-hours",
        type=float,
        default=6.0,
        help="Maximum window size for small deceptive transaction sequences.",
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


def get_surrounding_locations(
    transaction: TransactionRecord,
    locations: list[LocationPoint],
) -> tuple[LocationPoint | None, LocationPoint | None]:
    timestamps = [point.timestamp for point in locations]
    insertion_index = bisect_right(timestamps, transaction.timestamp)
    previous_point = locations[insertion_index - 1] if insertion_index > 0 else None
    next_point = locations[insertion_index] if insertion_index < len(locations) else None
    return previous_point, next_point


def extract_transaction_city(transaction: TransactionRecord) -> str | None:
    if not transaction.location:
        return None

    if transaction.transaction_type != "in-person payment":
        return None

    if " - " in transaction.location:
        return transaction.location.split(" - ", 1)[0].strip()

    return transaction.location.strip() or None


def resolve_user(
    biotag: str,
    users_by_iban: dict[str, dict[str, Any]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
) -> dict[str, Any] | None:
    sender_transactions = transactions_by_sender.get(biotag, [])
    if not sender_transactions:
        return None
    return users_by_iban.get(sender_transactions[0].sender_iban)


def hours_between(first: datetime, second: datetime) -> float:
    return abs((second - first).total_seconds()) / 3600


def days_between(first: datetime, second: datetime) -> float:
    return abs((second - first).total_seconds()) / 86400


def is_late_night_hour(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour <= end_hour:
        return start_hour <= hour <= end_hour
    return hour >= start_hour or hour <= end_hour


def build_transaction_context(
    transaction: TransactionRecord,
    locations: list[LocationPoint],
) -> dict[str, Any]:
    previous_point, next_point = get_surrounding_locations(transaction, locations)
    inferred_city = infer_transaction_city(transaction, locations)
    merchant_city = extract_transaction_city(transaction)

    context: dict[str, Any] = {
        "transaction_id": transaction.transaction_id,
        "timestamp": transaction.timestamp.isoformat(),
        "amount": transaction.amount,
        "transaction_type": transaction.transaction_type,
        "description": transaction.description,
        "merchant_location": transaction.location or None,
        "merchant_city": merchant_city,
        "payment_method": transaction.payment_method or None,
        "inferred_city_from_biotag": inferred_city,
    }

    if previous_point:
        context["previous_gps_ping"] = {
            "timestamp": previous_point.timestamp.isoformat(),
            "city": previous_point.city,
            "hours_from_transaction": round(
                hours_between(transaction.timestamp, previous_point.timestamp), 2
            ),
        }
    else:
        context["previous_gps_ping"] = None

    if next_point:
        context["next_gps_ping"] = {
            "timestamp": next_point.timestamp.isoformat(),
            "city": next_point.city,
            "hours_from_transaction": round(
                hours_between(transaction.timestamp, next_point.timestamp), 2
            ),
        }
    else:
        context["next_gps_ping"] = None

    if previous_point and next_point:
        context["gps_gap_hours"] = round(
            hours_between(previous_point.timestamp, next_point.timestamp), 2
        )
    else:
        context["gps_gap_hours"] = None

    return context


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
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)

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


def detect_gps_transaction_mismatches(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    for biotag, transactions in sorted(transactions_by_sender.items()):
        points = locations_by_biotag.get(biotag, [])
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)

        for transaction in transactions:
            if transaction.transaction_type != "in-person payment":
                continue

            context = build_transaction_context(transaction, points)
            merchant_city = context["merchant_city"]
            gps_city = context["inferred_city_from_biotag"]

            if not merchant_city or not gps_city or merchant_city == gps_city:
                continue

            findings.append(
                {
                    "biotag": biotag,
                    "user": build_user_label(user, biotag),
                    "pattern": "gps_transaction_mismatch",
                    "transaction": context,
                }
            )

    return findings


def detect_new_jurisdiction_transactions(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
    travel_signal_days: float,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    for biotag, transactions in sorted(transactions_by_sender.items()):
        points = locations_by_biotag.get(biotag, [])
        changes = compress_city_changes(points)
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)
        residence_city = user["residence"]["city"] if user else None

        for transaction in transactions:
            context = build_transaction_context(transaction, points)
            transaction_city = context["merchant_city"] or context["inferred_city_from_biotag"]
            if not transaction_city:
                continue

            prior_changes = [point for point in changes if point.timestamp < transaction.timestamp]
            prior_cities = {point.city for point in prior_changes}
            recently_changed = any(
                days_between(transaction.timestamp, point.timestamp) <= travel_signal_days
                for point in prior_changes[-2:]
            )

            if transaction_city in prior_cities:
                continue
            if residence_city and transaction_city == residence_city:
                continue
            if recently_changed:
                continue

            findings.append(
                {
                    "biotag": biotag,
                    "user": build_user_label(user, biotag),
                    "pattern": "new_jurisdiction_without_recent_travel_signal",
                    "residence_city": residence_city,
                    "transaction_city": transaction_city,
                    "travel_signal_days_threshold": travel_signal_days,
                    "transaction": context,
                }
            )

    return findings


def detect_residential_habit_anomalies(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
    late_night_start: int,
    late_night_end: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    for biotag, transactions in sorted(transactions_by_sender.items()):
        points = locations_by_biotag.get(biotag, [])
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)
        residence_city = user["residence"]["city"] if user else None

        for transaction in transactions:
            if not is_late_night_hour(transaction.timestamp.hour, late_night_start, late_night_end):
                continue

            context = build_transaction_context(transaction, points)
            effective_city = context["merchant_city"] or context["inferred_city_from_biotag"]
            if not effective_city or not residence_city:
                continue
            if effective_city == residence_city:
                continue

            findings.append(
                {
                    "biotag": biotag,
                    "user": build_user_label(user, biotag),
                    "pattern": "late_night_outside_residential_habit",
                    "residence_city": residence_city,
                    "transaction_city": effective_city,
                    "transaction": context,
                }
            )

    return findings


def detect_small_deceptive_sequences(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
    small_amount_threshold: float,
    rapid_sequence_hours: float,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    for biotag, transactions in sorted(transactions_by_sender.items()):
        points = locations_by_biotag.get(biotag, [])
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)
        candidate_transactions = []

        for transaction in transactions:
            if transaction.amount > small_amount_threshold:
                continue
            context = build_transaction_context(transaction, points)
            effective_city = context["merchant_city"] or context["inferred_city_from_biotag"]
            if not effective_city:
                continue
            candidate_transactions.append((transaction, context, effective_city))

        for left, right in pairwise(candidate_transactions):
            first_tx, first_context, first_city = left
            second_tx, second_context, second_city = right
            if first_city == second_city:
                continue
            if hours_between(first_tx.timestamp, second_tx.timestamp) > rapid_sequence_hours:
                continue

            findings.append(
                {
                    "biotag": biotag,
                    "user": build_user_label(user, biotag),
                    "pattern": "small_rapid_multi_city_sequence",
                    "sequence_window_hours": round(
                        hours_between(first_tx.timestamp, second_tx.timestamp), 2
                    ),
                    "transactions": [first_context, second_context],
                }
            )

    return findings


def detect_gps_dark_period_transactions(
    users_by_iban: dict[str, dict[str, Any]],
    locations_by_biotag: dict[str, list[LocationPoint]],
    transactions_by_sender: dict[str, list[TransactionRecord]],
    gps_gap_hours: float,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    for biotag, transactions in sorted(transactions_by_sender.items()):
        points = locations_by_biotag.get(biotag, [])
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)

        for transaction in transactions:
            context = build_transaction_context(transaction, points)
            gap = context["gps_gap_hours"]
            if gap is None or gap <= gps_gap_hours:
                continue

            prev_ping = context["previous_gps_ping"]
            next_ping = context["next_gps_ping"]
            hours_to_prev = prev_ping["hours_from_transaction"] if prev_ping else None
            hours_to_next = next_ping["hours_from_transaction"] if next_ping else None

            if hours_to_prev is not None and hours_to_next is not None:
                if hours_to_prev == 0 or hours_to_next == 0:
                    continue

            findings.append(
                {
                    "biotag": biotag,
                    "user": build_user_label(user, biotag),
                    "pattern": "transaction_during_gps_dark_period",
                    "gps_gap_hours_threshold": gps_gap_hours,
                    "transaction": context,
                }
            )

    return findings


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
        user = resolve_user(biotag, users_by_iban, transactions_by_sender)

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
                        build_transaction_context(transaction, points)
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
    gps_transaction_mismatches = detect_gps_transaction_mismatches(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
    )
    new_jurisdiction_transactions = detect_new_jurisdiction_transactions(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
        travel_signal_days=args.travel_signal_days,
    )
    residential_habit_anomalies = detect_residential_habit_anomalies(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
        late_night_start=args.late_night_start,
        late_night_end=args.late_night_end,
    )
    deceptive_sequences = detect_small_deceptive_sequences(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
        small_amount_threshold=args.small_amount_threshold,
        rapid_sequence_hours=args.rapid_sequence_hours,
    )
    gps_dark_period_transactions = detect_gps_dark_period_transactions(
        users_by_iban=users_by_iban,
        locations_by_biotag=locations_by_biotag,
        transactions_by_sender=transactions_by_sender,
        gps_gap_hours=args.gps_gap_hours,
    )

    report = {
        "dataset": str(dataset_path),
        "rules": {
            "ping_pong_return_days": args.return_days,
            "gps_gap_hours": args.gps_gap_hours,
            "travel_signal_days": args.travel_signal_days,
            "late_night_hours": {
                "start": args.late_night_start,
                "end": args.late_night_end,
            },
            "small_amount_threshold": args.small_amount_threshold,
            "rapid_sequence_hours": args.rapid_sequence_hours,
        },
        "user_timelines": user_timelines,
        "pattern_counts": {
            "ping_pong_movements": len(suspicious_windows),
            "gps_transaction_mismatches": len(gps_transaction_mismatches),
            "new_jurisdiction_transactions": len(new_jurisdiction_transactions),
            "residential_habit_anomalies": len(residential_habit_anomalies),
            "deceptive_small_transaction_sequences": len(deceptive_sequences),
            "gps_dark_period_transactions": len(gps_dark_period_transactions),
        },
        "patterns": {
            "ping_pong_movements": suspicious_windows,
            "gps_transaction_mismatches": gps_transaction_mismatches,
            "new_jurisdiction_transactions": new_jurisdiction_transactions,
            "residential_habit_anomalies": residential_habit_anomalies,
            "deceptive_small_transaction_sequences": deceptive_sequences,
            "gps_dark_period_transactions": gps_dark_period_transactions,
        },
        "notes": [
            "GPS mismatch requires a transaction with a physical merchant location.",
            "New jurisdiction checks use city changes in BioTag history; explicit airport or station signals are not available in this dataset.",
            "Residential anomaly checks are strongest for transactions with a physical location or a reliable inferred BioTag city.",
        ],
    }

    rendered = json.dumps(report, indent=2)
    if args.output:
        output_path = Path(args.output)
    else:
        dataset_slug = dataset_path.name.lower().replace(" ", "_")
        output_path = Path("output") / f"{dataset_slug}_fraud_report.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered + "\n", encoding="utf-8")
    print(f"Report written to {output_path}")


if __name__ == "__main__":
    main()
