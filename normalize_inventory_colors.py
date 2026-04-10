from __future__ import annotations

import argparse
import csv
from pathlib import Path

from daily_bike_monitor import normalize_color_value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize inventory color values to canonical labels.")
    parser.add_argument("input_csv", help="Input inventory CSV path.")
    parser.add_argument("output_csv", help="Output inventory CSV path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
            handle.write("")
        return 0

    fieldnames = list(rows[0].keys())
    for row in rows:
        row["色"] = normalize_color_value(row.get("色", ""))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
