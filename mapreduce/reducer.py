#!/usr/bin/env python3
import sys


def main():
    current_key = None
    total_count = 0
    total_amount = 0.0
    total_distance = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, val = line.split("\t", 1)
            count_str, amount_str, dist_str = val.split(",")
            count = int(count_str)
            amount = float(amount_str)
            distance = float(dist_str)
        except ValueError:
            continue

        if key == current_key:
            total_count += count
            total_amount += amount
            total_distance += distance
        else:
            if current_key is not None:
                avg_amount = total_amount / total_count if total_count else 0.0
                avg_dist = total_distance / total_count if total_count else 0.0
                print(
                    f"{current_key}\t{total_count}\t{total_amount:.2f}\t{avg_amount:.2f}\t{avg_dist:.2f}"
                )
            current_key = key
            total_count = count
            total_amount = amount
            total_distance = distance

    if current_key is not None:
        avg_amount = total_amount / total_count if total_count else 0.0
        avg_dist = total_distance / total_count if total_count else 0.0
        print(
            f"{current_key}\t{total_count}\t{total_amount:.2f}\t{avg_amount:.2f}\t{avg_dist:.2f}"
        )


if __name__ == "__main__":
    main()
