#!/usr/bin/env python3
"""Pipeline batch versi Spark untuk agregasi yang sama dengan mapreduce/mapper.py + reducer.py.

Dua mode:
  df  : DataFrame API
  rdd : RDD API, langkahnya ditulis mirip mapper dan reducer

Contoh: python spark/spark_job.py data/taxi_1m.csv --mode df
"""
import argparse
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "spark")
sys.path.insert(0, os.path.join(BASE_DIR, "mapreduce"))
from run_mapreduce import format_cli_report  # noqa: E402

# Skema ditulis manual agar Spark tidak perlu membaca data dua kali (inferSchema).
SCHEMA = StructType([
    StructField("tpep_pickup_datetime", StringType()),
    StructField("PULocationID", StringType()),
    StructField("passenger_count", StringType()),
    StructField("trip_distance", DoubleType()),
    StructField("payment_type", StringType()),
    StructField("total_amount", DoubleType()),
])


def run_dataframe(spark, input_csv):
    df = spark.read.csv(input_csv, header=True, schema=SCHEMA)

    # Filter sama dengan mapper.py. Nilai yang gagal dibaca sebagai angka menjadi null.
    valid = df.where(
        F.col("total_amount").isNotNull() & F.col("trip_distance").isNotNull()
        & (F.col("total_amount") > 0) & (F.col("trip_distance") >= 0)
        & F.col("PULocationID").isNotNull()
        & (F.length("tpep_pickup_datetime") >= 13)
    )

    # Setara mapper yang mencetak dua baris: setiap trip dipecah menjadi key ZONE| dan HOUR|.
    pairs = valid.select(
        F.explode(F.array(
            F.concat(F.lit("ZONE|"), F.col("PULocationID")),
            F.concat(F.lit("HOUR|"), F.substring("tpep_pickup_datetime", 12, 2)),
        )).alias("key"),
        F.col("total_amount").alias("amount"),
        F.col("trip_distance").alias("distance"),
    )

    # Setara reducer: jumlahkan per key.
    result = pairs.groupBy("key").agg(
        F.count("*").alias("cnt"),
        F.sum("amount").alias("total"),
        F.sum("distance").alias("dist"),
    )
    return [(r["key"], r["cnt"], r["total"], r["dist"]) for r in result.collect()]


def parse_line(line):
    """Setara mapper.py: satu baris CSV menjadi dua pasangan (key, (count, amount, distance))."""
    parts = line.split(",")
    if len(parts) < 6:
        return []
    try:
        amount = float(parts[5])
        distance = float(parts[3])
    except ValueError:
        return []  # header atau nilai kosong
    if amount <= 0 or distance < 0:
        return []
    pickup_dt = parts[0].strip('"')
    zone_id = parts[1].strip('"')
    if not zone_id or len(pickup_dt) < 13:
        return []
    value = (1, amount, distance)
    return [("ZONE|" + zone_id, value), ("HOUR|" + pickup_dt[11:13], value)]


def run_rdd(spark, input_csv):
    sc = spark.sparkContext
    # Default textFile hanya 2 partisi (2 core terpakai); samakan dengan jumlah core local[4].
    pairs = sc.textFile(input_csv, minPartitions=sc.defaultParallelism).flatMap(parse_line)
    # Setara reducer.py. reduceByKey juga menjumlah di tiap partisi sebelum shuffle (seperti combiner).
    totals = pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))
    return [(k, v[0], v[1], v[2]) for k, v in totals.collect()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", nargs="?", default=os.path.join(BASE_DIR, "data", "taxi_100k.csv"))
    parser.add_argument("--mode", choices=["df", "rdd"], default="df")
    args = parser.parse_args()

    start = time.time()
    # 4 core dan 4 GB, mengikuti batas NodeManager YARN (4 vcore, 4096 MB) di infra/config.
    spark = (
        SparkSession.builder.master("local[4]").appName(f"taxi-batch-{args.mode}")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.time() - start

    if args.mode == "df":
        rows = run_dataframe(spark, args.input_csv)
    else:
        rows = run_rdd(spark, args.input_csv)
    elapsed = time.time() - start
    spark.stop()

    # Format output sama persis dengan reducer.py agar hasil bisa dibandingkan baris per baris.
    lines = [
        f"{key}\t{cnt}\t{total:.2f}\t{total / cnt:.2f}\t{dist / cnt:.2f}\n"
        for key, cnt, total, dist in sorted(rows)
    ]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    name = os.path.splitext(os.path.basename(args.input_csv))[0]
    with open(os.path.join(OUTPUT_DIR, f"spark_{args.mode}_result_{name}.tsv"), "w") as f:
        f.writelines(lines)

    engine = "Spark DataFrame" if args.mode == "df" else "Spark RDD"
    format_cli_report(lines, elapsed, args.input_csv, engine)
    print(f"Startup SparkSession: {startup:.2f} detik | Pemrosesan: {elapsed - startup:.2f} detik")


if __name__ == "__main__":
    main()
