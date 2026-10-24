#!/usr/bin/env bash
# Jalankan dari host: docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_1m.csv [combiner]
set -euo pipefail
cd /workspace

INPUT=${1:-data/taxi_100k.csv}
NAME=$(basename "$INPUT" .csv)
# Argumen kedua "combiner" menambahkan combiner.py; hasil disimpan dengan akhiran _combiner.
SUFFIX=""
if [ "${2:-}" = "combiner" ]; then
  SUFFIX="_combiner"
fi
OUT_DIR=output/hadoop
mkdir -p "$OUT_DIR"

# NameNode menolak tulis selama safe mode (beberapa detik setelah cluster dinyalakan).
hdfs dfsadmin -safemode wait >/dev/null
hdfs dfs -mkdir -p /data
hdfs dfs -put -f "$INPUT" "/data/$NAME.csv"
hdfs dfs -rm -r -f -skipTrash "/output/$NAME$SUFFIX" >/dev/null

START=$(date +%s.%N)
# Log job berisi Hadoop counters (Spilled Records, shuffle bytes) untuk analisis.
HADOOP_ROOT_LOGGER=INFO,console hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapreduce/mapper.py,mapreduce/combiner.py,mapreduce/reducer.py \
  -mapper "python3 mapper.py" ${SUFFIX:+-combiner "python3 combiner.py"} -reducer "python3 reducer.py" \
  -input "/data/$NAME.csv" -output "/output/$NAME$SUFFIX" \
  > "$OUT_DIR/hadoop_log_$NAME$SUFFIX.txt" 2>&1
END=$(date +%s.%N)
ELAPSED=$(python3 -c "print($END - $START)")

RESULT="$OUT_DIR/hadoop_result_$NAME$SUFFIX.tsv"
rm -f "$RESULT" "$OUT_DIR/.$(basename "$RESULT").crc"
hdfs dfs -getmerge "/output/$NAME$SUFFIX" "$RESULT"

python3 - "$RESULT" "$ELAPSED" "$INPUT" "Hadoop MapReduce (YARN)${SUFFIX:+ + combiner}" <<'PY'
import sys
sys.path.insert(0, "mapreduce")
from run_mapreduce import format_cli_report
path, elapsed, src, engine = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4]
with open(path) as f:
    format_cli_report(f.readlines(), elapsed, src, engine)
PY
grep -E "Map output (records|bytes)|Combine (input|output) records|Spilled Records|Reduce shuffle bytes|CPU time spent|Physical memory" "$OUT_DIR/hadoop_log_$NAME$SUFFIX.txt" || true
