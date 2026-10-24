#!/usr/bin/env python3
"""Combiner: menjumlahkan output mapper per key di sisi map, sebelum shuffle.

Input dan output memakai format yang sama dengan mapper (KEY \t count,amount,distance),
karena Hadoop boleh menjalankan combiner nol, satu, atau beberapa kali.
Rata-rata tidak dihitung di sini: rata-rata dari rata-rata hasilnya salah.
"""
import sys


def emit(key, count, amount, distance):
    print(f"{key}\t{count},{amount:.2f},{distance:.2f}")


def main():
    current_key = None
    total_count = 0
    total_amount = 0.0
    total_distance = 0.0

    for line in sys.stdin:
        key, val = line.rstrip("\n").split("\t", 1)
        count_str, amount_str, dist_str = val.split(",")

        if key != current_key:
            if current_key is not None:
                emit(current_key, total_count, total_amount, total_distance)
            current_key = key
            total_count, total_amount, total_distance = 0, 0.0, 0.0

        total_count += int(count_str)
        total_amount += float(amount_str)
        total_distance += float(dist_str)

    if current_key is not None:
        emit(current_key, total_count, total_amount, total_distance)


if __name__ == "__main__":
    main()
