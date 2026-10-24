#!/usr/bin/env python3
"""Bukti kebenaran: hasil semua engine untuk satu dataset harus sama.

Jumlah trip (count) harus identik. Nilai uang dan jarak boleh beda maksimal 0.01
karena penjumlahan float dalam urutan berbeda bisa menggeser pembulatan digit terakhir.
Kelima file hasil wajib ada; file yang hilang membuat verifikasi gagal.

Contoh: python benchmark/verify_results.py taxi_1m
"""
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOLERANCE = 0.011


def load(path):
    rows = {}
    with open(path) as f:
        for line in f:
            key, count, total, avg_amt, avg_dist = line.rstrip("\n").split("\t")
            rows[key] = (int(count), float(total), float(avg_amt), float(avg_dist))
    return rows


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "taxi_100k"
    files = {
        "mapreduce_local": f"output/mapreduce/mr_result_{name}.tsv",
        "hadoop_yarn": f"output/hadoop/hadoop_result_{name}.tsv",
        "hadoop_yarn_combiner": f"output/hadoop/hadoop_result_{name}_combiner.tsv",
        "spark_df": f"output/spark/spark_df_result_{name}.tsv",
        "spark_rdd": f"output/spark/spark_rdd_result_{name}.tsv",
    }
    missing = [p for p in files.values() if not os.path.exists(os.path.join(BASE_DIR, p))]
    for p in missing:
        print(f"[HILANG] {p}")
    results = {e: load(os.path.join(BASE_DIR, p)) for e, p in files.items() if p not in missing}
    if not results:
        sys.exit(1)
    ref_name, ref = next(iter(results.items()))
    ok = not missing
    for engine, rows in results.items():
        problems = []
        if rows.keys() != ref.keys():
            problems.append(f"key berbeda: {sorted(rows.keys() ^ ref.keys())[:5]}")
        for key in rows.keys() & ref.keys():
            a, b = rows[key], ref[key]
            if a[0] != b[0] or any(abs(x - y) > TOLERANCE for x, y in zip(a[1:], b[1:])):
                problems.append(f"{key}: {a} vs {b}")
        ok &= not problems
        print(f"[{'OK' if not problems else 'BEDA'}] {engine} vs {ref_name}: {len(rows)} key")
        for p in problems[:5]:
            print("      ", p)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
