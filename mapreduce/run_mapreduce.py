#!/usr/bin/env python3
import os
import sys
import time
import tempfile
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPER_PATH = os.path.join(BASE_DIR, "mapreduce", "mapper.py")
REDUCER_PATH = os.path.join(BASE_DIR, "mapreduce", "reducer.py")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "mapreduce")


def format_cli_report(output_lines, elapsed_sec, input_file, engine):
    zones = []
    hours = []

    for line in output_lines:
        parts = line.strip().split("\t")
        if len(parts) != 5:
            continue
        key, count, total_amt, avg_amt, avg_dist = parts
        rec = (key.split("|")[1], int(count), float(total_amt), float(avg_amt), float(avg_dist))
        if key.startswith("ZONE|"):
            zones.append(rec)
        elif key.startswith("HOUR|"):
            hours.append(rec)

    zones.sort(key=lambda x: x[2], reverse=True)
    hours.sort(key=lambda x: x[0])

    print("=" * 74)
    print(f" LAPORAN PIPELINE BATCH: {engine}")
    print(f" Dataset : {os.path.basename(input_file)} | Waktu Eksekusi: {elapsed_sec:.2f} detik")
    print("=" * 74)

    print("\n[1] TOP 10 ZONA PENJEMPUTAN (BERDASARKAN TOTAL PENDAPATAN)")
    print(f"{'PULocationID':<14} | {'Total Trip':>12} | {'Total Revenue ($)':>18} | {'Avg Tarif ($)':>13} | {'Avg Jarak (mi)':>14}")
    print("-" * 74)
    for z_id, cnt, rev, avg_a, avg_d in zones[:10]:
        print(f"{z_id:<14} | {cnt:>12,} | {rev:>18,.2f} | {avg_a:>13.2f} | {avg_d:>14.2f}")

    print("\n[2] ANALISIS JAM SIBUK (PEAK HOURS 00:00 - 23:00)")
    print(f"{'Jam':<6} | {'Total Trip':>10} | {'Total Revenue ($)':>17} | {'Avg Tarif ($)':>13} | {'Distribusi Volume (CLI Bar)'}")
    print("-" * 74)
    max_trips = max((h[1] for h in hours), default=1)
    for hr, cnt, rev, avg_a, _ in hours:
        bar_len = int((cnt / max_trips) * 20)
        bar = "#" * bar_len
        print(f"{hr + ':00':<6} | {cnt:>10,} | {rev:>17,.2f} | {avg_a:>13.2f} | {bar}")
    print("=" * 74)


def run_pipeline(input_csv):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_time = time.time()

    # Simulasi arsitektur MapReduce berbasis Disk I/O:
    # Phase 1: Map -> tulis hasil intermediate ke disk
    # Phase 2: Shuffle & Sort -> urutkan file intermediate di disk
    # Phase 3: Reduce -> agregasi hasil terurut dan simpan ke output akhir
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix="_mapped.tsv") as tmp_map, \
         tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix="_sorted.tsv") as tmp_sort:
        map_path = tmp_map.name
        sort_path = tmp_sort.name

    try:
        with open(input_csv, "rb") as fin, open(map_path, "wb") as fout_map:
            subprocess.run([sys.executable, MAPPER_PATH], stdin=fin, stdout=fout_map, check=True)

        env = os.environ.copy()
        env["LC_ALL"] = "C"
        with open(sort_path, "wb") as fout_sort:
            subprocess.run(["sort", "-t", "\t", "-k1,1", map_path], stdout=fout_sort, env=env, check=True)

        name = os.path.splitext(os.path.basename(input_csv))[0]
        out_file = os.path.join(OUTPUT_DIR, f"mr_result_{name}.tsv")
        with open(sort_path, "rb") as fin_sort, open(out_file, "wb") as fout_red:
            subprocess.run([sys.executable, REDUCER_PATH], stdin=fin_sort, stdout=fout_red, check=True)

        elapsed = time.time() - start_time
        with open(out_file, "r", encoding="utf-8") as f:
            lines = f.readlines()

        format_cli_report(lines, elapsed, input_csv, "MapReduce lokal (simulasi)")
    finally:
        for p in (map_path, sort_path):
            if os.path.exists(p):
                os.remove(p)


if __name__ == "__main__":
    default_csv = os.path.join(BASE_DIR, "data", "taxi_100k.csv")
    target_csv = sys.argv[1] if len(sys.argv) > 1 else default_csv
    if not os.path.exists(target_csv):
        print(f"[ERROR] File dataset tidak ditemukan: {target_csv}")
        sys.exit(1)
    run_pipeline(target_csv)
