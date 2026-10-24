# Cluster Hadoop (pseudo-distributed, Docker)

Image `apache/hadoop:3.4.1` ditambah Python 3.6 untuk mapper/reducer Streaming.
Empat container: `namenode`, `datanode` (HDFS) serta `resourcemanager`, `nodemanager` (YARN).
Resource NodeManager: RAM 4096 MB, 4 vcore. Pakai batas yang sama untuk Spark (`local[4]`) agar benchmark adil.

## Menyalakan

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

Web UI: HDFS http://localhost:9870, YARN http://localhost:8088

## Menjalankan job

```bash
docker compose -f infra/docker-compose.yml exec namenode \
  bash /workspace/infra/run_hadoop_job.sh data/taxi_1m.csv
```

Script mengunggah CSV ke HDFS, menjalankan Hadoop Streaming dengan `mapreduce/mapper.py` dan `mapreduce/reducer.py`, lalu mencetak laporan CLI dan counters job.

Hasil di `output/hadoop/`:
- `hadoop_result_<nama>.tsv`: hasil agregasi (format sama dengan `run_mapreduce.py`)
- `hadoop_log_<nama>.txt`: log job lengkap, termasuk counters (`Spilled Records`, `Reduce shuffle bytes`, `CPU time spent`, `Physical memory`)

## Mematikan

```bash
docker compose -f infra/docker-compose.yml down
```
