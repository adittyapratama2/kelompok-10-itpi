#!/usr/bin/env python3
"""Benchmark semua engine pada 4 ukuran dataset, masing-masing diulang beberapa kali.

Mengukur per run:
  wall_sec    : waktu total proses dari luar (termasuk startup JVM/Python/Docker)
  job_sec     : waktu yang dilaporkan engine sendiri ("Waktu Eksekusi" di laporan CLI)
  cpu_sec     : total waktu CPU (user + system)
  peak_mem_mb : puncak memori fisik (RSS)

Engine lokal (MapReduce simulasi, Spark DF, Spark RDD) diukur dengan psutil dari
proses + semua child-nya. Hadoop berjalan di dalam container, jadi CPU dan memorinya
diambil dari Hadoop counters di log job (CPU time spent, Physical memory snapshot).

Setiap run langsung ditambahkan ke benchmark/results/benchmark.csv, jadi run yang terhenti
tidak menghapus hasil sebelumnya. Menjalankan ulang dataset yang sama menambah baris, bukan mengganti.

Prasyarat Hadoop: docker compose -f infra/docker-compose.yml up -d
Contoh: python benchmark/run_benchmark.py --reps 3
        python benchmark/run_benchmark.py --reps 3 --datasets taxi_full
"""
import argparse
import csv
import os
import re
import resource
import statistics
import subprocess
import sys
import time

import psutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT_CSV = os.path.join(BASE_DIR, "benchmark", "results", "benchmark.csv")
DATASETS = ["taxi_100k", "taxi_500k", "taxi_1m", "taxi_full"]
FIELDS = ["dataset", "engine", "rep", "wall_sec", "job_sec", "cpu_sec", "peak_mem_mb",
          "spilled_records", "shuffle_bytes"]
PY = sys.executable
HADOOP = ["docker", "compose", "-f", "infra/docker-compose.yml", "exec", "-T",
          "namenode", "bash", "/workspace/infra/run_hadoop_job.sh"]


def command_for(engine, name):
    csv_path = f"data/{name}.csv"
    return {
        "mapreduce_local": [PY, "mapreduce/run_mapreduce.py", csv_path],
        "hadoop_yarn": HADOOP + [csv_path],
        "hadoop_yarn_combiner": HADOOP + [csv_path, "combiner"],
        "spark_df": [PY, "spark/spark_job.py", csv_path, "--mode", "df"],
        "spark_rdd": [PY, "spark/spark_job.py", csv_path, "--mode", "rdd"],
    }[engine]


def run_and_measure(cmd):
    """Jalankan cmd, sampling CPU dan memori proses beserta child-nya tiap 0.1 detik."""
    start = time.time()
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    proc = subprocess.Popen(cmd, cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    root = psutil.Process(proc.pid)
    cpu_by_pid = {}  # CPU terakhir tiap proses; child yang sudah selesai tetap terhitung
    peak_rss = 0
    while proc.poll() is None:
        try:
            procs = [root] + root.children(recursive=True)
        except psutil.NoSuchProcess:
            break
        rss = 0
        for p in procs:
            try:
                t = p.cpu_times()
                cpu_by_pid[p.pid] = t.user + t.system
                rss += p.memory_info().rss
            except psutil.NoSuchProcess:
                pass
        peak_rss = max(peak_rss, rss)
        time.sleep(0.1)
    output = proc.communicate()[0]
    wall = time.time() - start
    if proc.returncode != 0:
        raise RuntimeError(f"Gagal: {' '.join(cmd)}\n{output[-2000:]}")
    # Proses yang sangat singkat bisa lolos dari sampling; rusage menghitung CPU
    # semua child yang sudah selesai secara pasti. Ambil yang lebih besar.
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    rusage_cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    return wall, max(sum(cpu_by_pid.values()), rusage_cpu), peak_rss / 2**20, output


def hadoop_counters(log_name):
    with open(os.path.join(BASE_DIR, "output", "hadoop", f"hadoop_log_{log_name}.txt")) as f:
        log = f.read()

    def counter(label):
        return int(re.search(label + r"=(\d+)", log).group(1))

    return (counter(r"CPU time spent \(ms\)") / 1000,
            counter(r"Physical memory \(bytes\) snapshot") / 2**20,
            counter("Spilled Records"),
            counter("Reduce shuffle bytes"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--engines", nargs="+",
                        default=["mapreduce_local", "hadoop_yarn", "hadoop_yarn_combiner", "spark_df", "spark_rdd"])
    parser.add_argument("--datasets", nargs="+", default=DATASETS)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(RESULT_CSV), exist_ok=True)
    new_file = not os.path.exists(RESULT_CSV) or os.path.getsize(RESULT_CSV) == 0
    rows = []
    with open(RESULT_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        for name in args.datasets:
            for engine in args.engines:
                for rep in range(1, args.reps + 1):
                    wall, cpu, mem, output = run_and_measure(command_for(engine, name))
                    job = float(re.search(r"Waktu Eksekusi: ([\d.]+)", output).group(1))
                    spilled = shuffle = ""
                    if engine.startswith("hadoop_yarn"):
                        suffix = "_combiner" if engine.endswith("combiner") else ""
                        cpu, mem, spilled, shuffle = hadoop_counters(name + suffix)
                    row = {"dataset": name, "engine": engine, "rep": rep,
                           "wall_sec": round(wall, 2), "job_sec": round(job, 2),
                           "cpu_sec": round(cpu, 2), "peak_mem_mb": round(mem, 1),
                           "spilled_records": spilled, "shuffle_bytes": shuffle}
                    rows.append(row)
                    writer.writerow(row)
                    f.flush()
                    print(f"{name:<10} {engine:<20} rep {rep}: wall {wall:6.2f}s  job {job:6.2f}s  "
                          f"cpu {cpu:6.2f}s  mem {mem:7.1f} MB", flush=True)

    # Ringkasan: median antar ulangan, supaya satu run yang lambat tidak mendominasi.
    print(f"\nMEDIAN dari {args.reps} ulangan (detail: {os.path.relpath(RESULT_CSV, BASE_DIR)})")
    print(f"{'dataset':<10} {'engine':<20} {'wall (s)':>9} {'job (s)':>9} {'cpu (s)':>9} {'mem (MB)':>9}")
    for name in args.datasets:
        for engine in args.engines:
            sel = [r for r in rows if r["dataset"] == name and r["engine"] == engine]
            med = {k: statistics.median(r[k] for r in sel) for k in ("wall_sec", "job_sec", "cpu_sec", "peak_mem_mb")}
            print(f"{name:<10} {engine:<20} {med['wall_sec']:>9.2f} {med['job_sec']:>9.2f} "
                  f"{med['cpu_sec']:>9.2f} {med['peak_mem_mb']:>9.1f}")


if __name__ == "__main__":
    main()
