# Tugas 2 ITPI Kelompok 10: Pipeline Batch Hadoop MapReduce dan Spark

Dataset yang sama (NYC Yellow Taxi, Januari 2024) diproses dengan Hadoop MapReduce dan Spark (RDD dan DataFrame). Agregasinya per zona penjemputan dan per jam: jumlah trip, total pendapatan, rata-rata tarif, rata-rata jarak. Waktu eksekusi dan pemakaian resource kemudian dibandingkan.

Penjelasan alur, kode per baris, alasan setiap pilihan, dan hasil benchmark: [docs/PANDUAN.md](docs/PANDUAN.md).

## Struktur

```
preprocessing/prepare_dataset.py   unduh Parquet, buat taxi_100k / 500k / 1m / full .csv
mapreduce/mapper.py                tahap map (Hadoop Streaming)
mapreduce/combiner.py              penjumlahan di sisi map, opsional
mapreduce/reducer.py               tahap reduce
mapreduce/run_mapreduce.py         simulasi map, sort, reduce tanpa cluster
infra/                             cluster Hadoop 3.4.1 di Docker (HDFS + YARN)
spark/spark_job.py                 versi Spark, --mode df atau --mode rdd
benchmark/verify_results.py        cek hasil semua engine sama
benchmark/run_benchmark.py         ukur waktu, CPU, memori
benchmark/results/benchmark.csv    hasil pengukuran per run
```

## Menjalankan

Butuh Python 3 (diuji dengan 3.12), Java 17 atau 21 (untuk Spark 4), dan Docker.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python preprocessing/prepare_dataset.py

# MapReduce lokal
python mapreduce/run_mapreduce.py data/taxi_100k.csv

# Hadoop di Docker, tanpa dan dengan combiner
docker compose -f infra/docker-compose.yml up -d --build
docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv
docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv combiner

# Spark
python spark/spark_job.py data/taxi_100k.csv --mode df
python spark/spark_job.py data/taxi_100k.csv --mode rdd

# Cek kebenaran dan benchmark (4 dataset x 5 engine x 3 ulangan, sekitar 40 menit)
python benchmark/verify_results.py taxi_100k
python benchmark/run_benchmark.py --reps 3

docker compose -f infra/docker-compose.yml down
```
