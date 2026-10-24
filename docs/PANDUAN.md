# Panduan Memahami Kode Tugas 2

Pipeline pemrosesan batch dengan Hadoop MapReduce dan Spark, Kelompok 10.

Dokumen ini untuk anggota kelompok. Tujuannya: setiap orang bisa membuka file mana pun di repo ini, menjelaskan apa yang dilakukan setiap barisnya, kenapa ditulis begitu, dan kenapa tidak memakai cara lain. Baca berurutan dari bagian 1.

Satu prinsip saat menjelaskan ke dosen: **katakan apa yang kita ukur dan apa yang tidak kita ukur.** Kalau ada pilihan yang tidak kita bandingkan dengan alternatifnya, bilang begitu. Lebih aman mengakui batasan daripada mengklaim sesuatu yang tidak bisa kita buktikan dengan angka di repo ini.

Daftar isi:

1. Soal dan apa yang kita hitung
2. Alur data dari awal sampai akhir
3. Cara menjalankan semuanya
4. Penjelasan kode per file dan per baris
5. Alasan setiap pilihan dan alternatifnya
6. MapReduce dan Spark: apa bedanya di balik layar
7. Hasil benchmark dan cara membacanya
8. Bank pertanyaan kritis dosen
9. Pembagian penjelasan untuk 5 anggota

---

## 1. Soal dan apa yang kita hitung

Soal Tugas 2 (lihat `docs/Tugas 2.png`):

- Buat **dua versi pipeline batch untuk dataset yang sama**: (1) Hadoop MapReduce, (2) Spark (RDD/DataFrame).
- **Bandingkan performa**: waktu eksekusi dan pemakaian resource.
- Penilaian: kebenaran implementasi, kualitas kode, kedalaman analisis komparatif.

**Dataset**: NYC Yellow Taxi, Januari 2024, dari situs resmi NYC TLC. Setiap baris adalah satu perjalanan taksi yang dibayar, jadi ini data transaksi seperti contoh di soal. File asli berisi 2.964.624 baris. Kita ambil empat potongan untuk menguji skala:

| File | Baris data | Ukuran |
|---|---|---|
| `data/taxi_100k.csv` | 100.000 | 4,3 MB |
| `data/taxi_500k.csv` | 500.000 | 21 MB |
| `data/taxi_1m.csv` | 1.000.000 | 43 MB |
| `data/taxi_full.csv` | 2.964.624 (satu bulan penuh) | 127 MB |

Potongan diambil dari **awal file** (N baris pertama), bukan sampel acak. Alasannya ada di bagian 5.3.

Kolom yang disimpan (6 dari 19 kolom asli):

| Index | Kolom | Arti |
|---|---|---|
| 0 | `tpep_pickup_datetime` | Waktu penjemputan, contoh `2024-01-01 00:57:55.000000` |
| 1 | `PULocationID` | ID zona penjemputan (angka 1 sampai 265) |
| 2 | `passenger_count` | Jumlah penumpang (tidak dipakai di agregasi) |
| 3 | `trip_distance` | Jarak perjalanan dalam mil |
| 4 | `payment_type` | Cara bayar (tidak dipakai di agregasi) |
| 5 | `total_amount` | Total yang dibayar penumpang dalam dolar |

Contoh dua baris pertama file CSV:

```
"tpep_pickup_datetime","PULocationID","passenger_count","trip_distance","payment_type","total_amount"
2024-01-01 00:57:55.000000,186,1,1.72,2,22.7
```

**Yang dihitung** (sama persis di semua versi), dua agregasi sekaligus:

1. **Per zona penjemputan**: jumlah trip, total pendapatan, rata-rata tarif, rata-rata jarak.
2. **Per jam penjemputan (00 sampai 23)**: metrik yang sama.

Baris yang **dibuang**: tarif `<= 0` (transaksi batal atau refund), jarak negatif, angka yang tidak bisa dibaca, zona kosong, dan waktu yang terlalu pendek untuk diambil jamnya.

Format hasil (satu baris per key, dipisah tab), sama untuk semua engine:

```
KEY        jumlah_trip  total_pendapatan  rata2_tarif  rata2_jarak
HOUR|08    40434        1044872.87        25.84        3.24
ZONE|132   58240        4679347.74        80.35        15.76
```

Dua baris di atas diambil dari hasil Hadoop untuk `taxi_1m.csv`. Zona 132 adalah Bandara JFK; tarif rata-ratanya tinggi karena perjalanannya jauh.

---

## 2. Alur data dari awal sampai akhir

```
 [Situs NYC TLC]
       |  preprocessing/prepare_dataset.py
       v
 data/yellow_tripdata_2024-01.parquet  (2,96 juta baris, 19 kolom)
       |  ambil 6 kolom, potong 100k / 500k / 1m baris pertama
       v
 data/taxi_100k.csv, taxi_500k.csv, taxi_1m.csv
       |
       +-------------------+--------------------+--------------------+-------------------+
       |                   |                    |                    |                   |
 (A) MapReduce lokal  (B) Hadoop YARN     (C) Hadoop YARN     (D) Spark DataFrame  (E) Spark RDD
 run_mapreduce.py     run_hadoop_job.sh   run_hadoop_job.sh   spark_job.py         spark_job.py
 mapper | sort |      mapper + reducer    ... combiner        --mode df            --mode rdd
 reducer                                  mapper + combiner   explode + groupBy    flatMap +
                                          + reducer                                reduceByKey
       |                   |                    |                    |                   |
       v                   v                    v                    v                   v
 output/mapreduce/    output/hadoop/      output/hadoop/      output/spark/        output/spark/
 mr_result_*.tsv      hadoop_result_      hadoop_result_      spark_df_result_     spark_rdd_result_
                      *.tsv               *_combiner.tsv      *.tsv                *.tsv
       |                   |                    |                    |                   |
       +-------------------+--------------------+--------------------+-------------------+
                                                |
                      benchmark/verify_results.py: bukti kelima hasil sama
                      benchmark/run_benchmark.py : ukur waktu, CPU, memori
                                                |
                                                v
                                benchmark/results/benchmark.csv
```

Soal minta dua versi. Kenapa ada lima?

- **(B) Hadoop MapReduce** dan **(D) Spark DataFrame** adalah dua versi utama yang diminta soal.
- **(E) Spark RDD** karena soal menulis "RDD/DataFrame". Langkah RDD mirip mapper dan reducer, jadi mudah ditunjukkan bahwa logikanya sama dan hanya mesinnya yang berbeda.
- **(C) Hadoop + combiner** karena Spark selalu menjumlah sebagian di tiap partisi sebelum shuffle, sedangkan Hadoop tanpa combiner tidak. Tanpa varian ini, perbandingan shuffle Hadoop dan Spark tidak setara. Detail di 5.6.
- **(A) MapReduce lokal** menjalankan mapper dan reducer yang sama tanpa cluster. Gunanya untuk mengecek logika dengan cepat, dan di benchmark menunjukkan seberapa besar biaya cluster (YARN, HDFS, JVM) dibanding logikanya sendiri. Ini **bukan** Hadoop; jangan disebut sebagai versi Hadoop saat menjelaskan.

---

## 3. Cara menjalankan semuanya

Semua perintah dijalankan dari folder utama repo.

```bash
# 0. Siapkan Python (sekali saja). Spark 4 butuh Java 17 atau 21.
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1. Unduh dataset dan buat 4 file CSV
python preprocessing/prepare_dataset.py

# 2. MapReduce lokal (tanpa cluster)
python mapreduce/run_mapreduce.py data/taxi_100k.csv

# 3. Nyalakan cluster Hadoop, lalu jalankan job (tanpa dan dengan combiner)
docker compose -f infra/docker-compose.yml up -d --build
docker compose -f infra/docker-compose.yml exec namenode \
  bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv
docker compose -f infra/docker-compose.yml exec namenode \
  bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv combiner

# 4. Spark, dua mode
python spark/spark_job.py data/taxi_100k.csv --mode df
python spark/spark_job.py data/taxi_100k.csv --mode rdd

# 5. Bukti hasil semua engine sama
python benchmark/verify_results.py taxi_100k

# 6. Benchmark lengkap (4 dataset x 5 engine x 3 ulangan, sekitar 40 menit)
python benchmark/run_benchmark.py --reps 3

# 7. Matikan cluster
docker compose -f infra/docker-compose.yml down
```

Web UI saat cluster menyala: HDFS di http://localhost:9870, YARN (daftar job) di http://localhost:8088.

### 3.1 Urutan demo Senin (data 100k)

Demo memakai `taxi_100k` supaya semua engine selesai dalam beberapa menit. Data besar (500k, 1m, full) tidak dijalankan ulang di depan dosen; hasilnya sudah ada di `benchmark/results/benchmark.csv` dan dibahas di bagian 7. Kalau dosen bertanya "kenapa demo hanya 100k", jawabannya: alur dan kode yang dijalankan sama persis untuk semua ukuran, yang berbeda hanya argumen nama file; benchmark penuh butuh sekitar 40 menit, jadi hasilnya kita tunjukkan dari file.

Sebelum dosen masuk:

```bash
source .venv/bin/activate
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps    # tunggu 4 container berstatus running
```

Urutan di depan dosen:

| No | Perintah | Waktu kira-kira (hasil ukur 100k) | Yang ditunjukkan |
|---|---|---|---|
| 1 | `python mapreduce/run_mapreduce.py data/taxi_100k.csv` | di bawah 1 detik | map, sort, reduce tanpa cluster; isi `output/mapreduce/mr_result_taxi_100k.tsv` |
| 2 | `docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv` | sekitar 40-45 detik total, job sekitar 28 detik | job Hadoop Streaming di YARN; counter di `output/hadoop/hadoop_log_taxi_100k.txt` |
| 3 | perintah no. 2 ditambah argumen `combiner` | sama | bandingkan `Spilled Records` dan `Reduce shuffle bytes` dengan no. 2 |
| 4 | `python spark/spark_job.py data/taxi_100k.csv --mode df` | sekitar 15 detik (termasuk startup Spark) | versi DataFrame |
| 5 | `python spark/spark_job.py data/taxi_100k.csv --mode rdd` | sekitar 12-13 detik | versi RDD |
| 6 | `python benchmark/verify_results.py taxi_100k` | sekitar 1 detik | kelima hasil sama |
| 7 | buka `benchmark/results/benchmark.csv` | - | angka 4 ukuran, lalu bahas bagian 7 |

Perbandingan counter tanpa dan dengan combiner (no. 2 dan 3):

```bash
grep -E 'Map output records|Combine output records|Spilled Records|Reduce shuffle bytes' \
  output/hadoop/hadoop_log_taxi_100k.txt output/hadoop/hadoop_log_taxi_100k_combiner.txt
```

Opsional bila waktu cukup: daftar job di http://localhost:8088 dan file di HDFS di http://localhost:9870.

Selesai demo: `docker compose -f infra/docker-compose.yml down`.

Kalau ada perintah yang gagal saat demo, jangan panik dan jangan improvisasi angka. Hasil run sebelumnya tetap ada di `output/` dan `benchmark/results/`, jadi penjelasan bisa dilanjutkan dari file tersebut.

---

## 4. Penjelasan kode per file dan per baris

Urutan baca yang disarankan: `prepare_dataset.py`, `mapper.py`, `reducer.py`, `combiner.py`, `run_mapreduce.py`, folder `infra/`, `spark_job.py`, `verify_results.py`, `run_benchmark.py`.

### 4.1 `preprocessing/prepare_dataset.py`: menyiapkan data

| Baris | Kode | Penjelasan |
|---|---|---|
| 1-6 | `import ...` | `urllib` dan `shutil` untuk unduh, `pyarrow` untuk membaca Parquet dan menulis CSV. |
| 8-11 | `DATASET_URL`, `DATA_DIR`, `PARQUET_PATH` | Alamat file resmi NYC TLC dan lokasi simpan di `data/`. `os.path.dirname` dua kali naik dari `preprocessing/` ke folder utama repo. |
| 14-21 | `SELECTED_COLUMNS` | Hanya 6 kolom yang dibaca. Parquet menyimpan data per kolom, jadi kolom yang tidak diminta tidak perlu dibaca dari disk. |
| 24-29 | `SUBSETS` | Empat ukuran untuk uji skala. `100_000` sama dengan `100000`; garis bawah hanya pemisah angka. `None` untuk `taxi_full.csv` berarti seluruh baris. |
| 32-46 | `download_parquet()` | Kalau file sudah ada, lewati. Kalau belum, unduh dengan header `User-Agent` (beberapa CDN menolak request tanpa header ini). |
| 43 | `shutil.copyfileobj(response, out_file)` | Isi unduhan dialirkan ke file sepotong demi sepotong, tidak ditampung utuh di RAM dulu. |
| 49-69 | `convert_to_csv_subsets()` | Baca Parquet sekali (`pq.read_table`), lalu untuk tiap ukuran ambil N baris pertama (`table.slice(0, N)`; dengan `N = None`, `slice` mengambil sampai baris terakhir) dan tulis ke CSV. File yang sudah ada dilewati, jadi script aman dijalankan ulang. |
| 72-74 | `if __name__ == "__main__"` | Blok ini hanya jalan kalau file dijalankan langsung, tidak saat di-import. |

### 4.2 `mapreduce/mapper.py`: tahap MAP

Mapper membaca CSV baris demi baris dari **stdin** dan menulis pasangan **key-value** ke **stdout**. Aturan Hadoop Streaming: teks sebelum tab pertama adalah key, sisanya value.

| Baris | Kode | Penjelasan |
|---|---|---|
| 1 | `#!/usr/bin/env python3` | Shebang. Di Hadoop kita tetap memanggil `python3 mapper.py` secara eksplisit, jadi baris ini hanya berguna kalau file dijalankan langsung. |
| 8 | `for line in sys.stdin:` | Baca input baris per baris. Memori yang dipakai tetap kecil berapa pun ukuran file. |
| 9-11 | `strip()`, cek header | Hapus spasi/newline. Lewati baris kosong dan baris header. pyarrow menulis header dengan tanda kutip, jadi dua bentuk dicek. |
| 13-15 | `split(",")`, `len(parts) < 6` | Pecah baris menjadi kolom. Baris yang kolomnya kurang dari 6 dilewati. `split(",")` aman di sini karena tidak ada nilai yang mengandung koma; untuk CSV umum ini tidak cukup (lihat 5.2). |
| 17-23 | `try: ... except ValueError` | Ambil waktu, zona, jarak, tarif. Kalau jarak atau tarif kosong atau bukan angka, `float()` gagal dan baris dilewati, job tidak berhenti. `strip('"')` hanya berjaga-jaga kalau nilai diberi tanda kutip. |
| 25-27 | `if amount <= 0 or distance < 0` | Buang refund, pembatalan, dan jarak negatif. |
| 29-31 | `if not zone_id or len(pickup_dt) < 13` | Zona kosong atau waktu terpotong dibuang. Tanpa cek ini, `pickup_dt[11:13]` pada string pendek menghasilkan string kosong, dan baris itu akan masuk ke key `HOUR\|` yang salah. |
| 34 | `pickup_dt[11:13]` | Ambil jam. Dari `2024-01-01 00:57:55`, karakter index 11 dan 12 adalah `00`. |
| 38-39 | dua `print(...)` | **Inti mapper.** Setiap baris valid menghasilkan dua pasangan, contoh `ZONE\|186<TAB>1,22.70,1.72` dan `HOUR\|00<TAB>1,22.70,1.72`. Angka `1` adalah hitungan trip. Prefix `ZONE\|` dan `HOUR\|` membuat dua agregasi berjalan dalam satu job tanpa key bertabrakan (zona 1 dan jam 01 tetap berbeda). |

Jadi 1 juta baris input menghasilkan sekitar 2 juta pasangan. Di log Hadoop untuk `taxi_1m.csv`: `Map input records=1000001` (termasuk header) dan `Map output records=1973438`, artinya 986.719 baris lolos filter dan 13.281 baris dibuang.

Nilai uang dan jarak dicetak dengan 2 desimal (`:.2f`). Di data ini pembulatan itu tidak mengubah apa pun: semua nilai `total_amount` dan `trip_distance` di `taxi_1m.csv` paling banyak 2 desimal. Spark RDD dan DataFrame tidak membulatkan di tahap ini, dan hasilnya tetap sama (4.8). Pada data lain yang punya lebih dari 2 desimal, pembulatan di mapper sebaiknya dihindari.

### 4.3 `mapreduce/reducer.py`: tahap REDUCE

Sebelum masuk reducer, Hadoop (atau `sort` di versi lokal) **mengurutkan semua output mapper berdasarkan key**. Akibatnya semua baris dengan key sama pasti berdampingan. Reducer cukup menjumlah selama key-nya sama, lalu mencetak hasil ketika key berganti. Yang disimpan di memori hanya satu key yang sedang diproses.

| Baris | Kode | Penjelasan |
|---|---|---|
| 6-9 | `current_key = None`, `total_* = 0` | Penampung untuk key yang sedang dijumlah. |
| 11-14 | `for line in sys.stdin` | Baca output mapper yang sudah terurut, lewati baris kosong. |
| 16-23 | `split("\t", 1)`, `split(",")` | Pisahkan key dan value, lalu pecah value menjadi count, amount, distance. `count` dibaca sebagai `int` karena dari combiner nilainya bisa lebih dari 1. |
| 25-28 | `if key == current_key:` | Key masih sama: tambahkan ke total. |
| 29-39 | `else:` | Key berganti. Cetak hasil key lama (rata-rata = total / count), lalu isi penampung dengan nilai baris baru. |
| 31-32 | `... if total_count else 0.0` | Jaga pembagian dengan nol. Dengan data kita `total_count` selalu minimal 1, jadi ini hanya pengaman. |
| 41-46 | blok setelah loop | Key **terakhir** belum tercetak karena tidak ada key berikutnya yang memicu `else`. Kalau blok ini dihapus, satu key hilang dari hasil. |

Contoh jalannya:

```
HOUR|00  1,22.70,1.72   -> key baru, simpan: count=1, total=22.70
HOUR|00  1,18.70,1.80   -> key sama, tambah: count=2, total=41.40
HOUR|01  1,10.00,0.90   -> key berganti: cetak "HOUR|00 2 41.40 20.70 1.76", mulai HOUR|01
```

### 4.4 `mapreduce/combiner.py`: penjumlahan di sisi map

Combiner dijalankan Hadoop **di sisi map, setelah map dan sebelum shuffle**. Tugasnya menjumlah output mapper per key di tiap map task, sehingga yang dikirim ke reducer jauh lebih sedikit.

| Baris | Kode | Penjelasan |
|---|---|---|
| 2-7 | docstring | Dua aturan penting combiner, lihat di bawah tabel. |
| 11-12 | `emit(...)` | Cetak dalam format **yang sama dengan mapper**: `KEY<TAB>count,amount,distance`. |
| 16-19 | penampung | Sama seperti reducer. |
| 21-23 | baca baris | Input combiner berasal dari mapper kita sendiri, jadi tidak perlu `try/except` seperti reducer. |
| 25-29 | key berganti | Cetak total key lama, mulai key baru dari nol. |
| 31-33 | jumlahkan | `int(count_str)` karena input combiner bisa berupa output combiner sebelumnya (count lebih dari 1). |
| 35-36 | key terakhir | Sama seperti reducer. |

Dua aturan yang harus dipenuhi combiner:

1. **Format output = format input.** Hadoop boleh menjalankan combiner nol kali, sekali, atau beberapa kali, dan reducer tidak tahu mana yang terjadi. Karena itu output combiner harus bisa dibaca reducer sama seperti output mapper.
2. **Operasinya harus boleh dijalankan sebagian-sebagian.** Penjumlahan boleh: `(1+2)+3 = 1+(2+3)`. Rata-rata tidak boleh: rata-rata dari (rata-rata 3 trip) dan (rata-rata 1 trip) bukan rata-rata 4 trip. Karena itu combiner hanya menjumlah count, amount, distance, dan rata-rata dihitung sekali di reducer.

Kenapa tidak memakai `reducer.py` sebagai combiner? Karena reducer mengeluarkan format lain (`KEY count total avg_amt avg_dist`, dipisah tab) yang tidak bisa dibaca reducer lagi.

### 4.5 `mapreduce/run_mapreduce.py`: simulasi MapReduce lokal

File ini menjalankan mapper dan reducer yang **sama persis** dengan versi Hadoop, tanpa cluster. Tiga fase MapReduce ditiru dengan file sementara di disk.

| Baris | Kode | Penjelasan |
|---|---|---|
| 8-11 | `BASE_DIR`, `MAPPER_PATH`, ... | Path absolut supaya script bisa dijalankan dari folder mana pun. |
| 14-51 | `format_cli_report(output_lines, elapsed_sec, input_file, engine)` | Mencetak laporan di terminal: 10 zona dengan pendapatan terbesar dan tabel 24 jam dengan diagram batang `#`. Dipakai juga oleh `run_hadoop_job.sh` dan `spark_job.py`, jadi tampilan semua engine berasal dari kode yang sama. Parameter `engine` hanya mengisi judul laporan. |
| 23 | `key.split("\|")[1]` | Buang prefix: `ZONE\|132` menjadi `132`. |
| 29-30 | `zones.sort(...)`, `hours.sort(...)` | Zona diurutkan dari pendapatan terbesar, jam dari 00 sampai 23. |
| 46-49 | `max_trips`, `bar_len` | Panjang batang = proporsi trip jam itu terhadap jam tersibuk, maksimal 20 karakter. |
| 62-65 | `NamedTemporaryFile(..., delete=False)` | Buat dua file sementara: hasil map dan hasil sort. |
| 68-69 | `subprocess.run([python, mapper], stdin=CSV, stdout=file_map)` | **Fase MAP**: CSV dialirkan ke stdin mapper, output ditulis ke disk. |
| 71-74 | `LC_ALL=C`, `sort -t "\t" -k1,1` | **Fase SHUFFLE & SORT**: urutkan berdasarkan kolom pertama (key). Alasan `LC_ALL=C` ada di 5.12. |
| 76-79 | `subprocess.run([python, reducer], ...)` | **Fase REDUCE**: hasil sort dialirkan ke reducer, output ke `output/mapreduce/mr_result_<nama>.tsv`. |
| 81 | `elapsed = ...` | Waktu dari awal map sampai akhir reduce. |
| 86-89 | `finally: os.remove(...)` | File sementara selalu dihapus, termasuk saat terjadi error. |
| 92-98 | `__main__` | Ambil path dari argumen, default `taxi_100k.csv`. Berhenti dengan pesan jelas kalau file tidak ada. |

Bedanya dengan Hadoop sungguhan: tidak ada HDFS, input tidak dibagi ke beberapa map task, tidak ada YARN, tidak ada JVM, dan tidak ada combiner. Semua berjalan berurutan dalam satu proses demi satu proses.

### 4.6 Folder `infra/`: cluster Hadoop di Docker

Cluster ini **pseudo-distributed**: setiap komponen Hadoop berjalan sebagai container terpisah, tetapi semuanya di satu komputer.

**`infra/Dockerfile`**

| Baris | Penjelasan |
|---|---|
| 1 | Image resmi `apache/hadoop:3.4.1`. |
| 2, 6 | Pindah ke `root` untuk memasang paket, lalu kembali ke user `hadoop`. |
| 3-5 | Image ini berbasis CentOS 7 yang sudah EOL, mirror paketnya mati. `sed` mengarahkan `yum` ke `vault.centos.org` supaya `python3` bisa dipasang. Python yang terpasang adalah **3.6**, jadi `mapper.py`, `combiner.py`, dan `reducer.py` sengaja hanya memakai fitur Python 3.6 (f-string sudah ada sejak 3.6). |

**`infra/docker-compose.yml`**: empat container.

| Container | Peran |
|---|---|
| `namenode` | Pengelola HDFS: menyimpan daftar file dan lokasi blok-bloknya. Folder repo di-mount ke `/workspace` (baris 16), jadi `run_hadoop_job.sh` dijalankan dari sini. |
| `datanode` | Tempat isi blok file HDFS disimpan. |
| `resourcemanager` | Pengelola YARN: menerima job dan membagi memori ke task. |
| `nodemanager` | Pekerja YARN: menjalankan map task dan reduce task di dalam container JVM. |

Baris 2-5 (`x-hadoop: &hadoop`) adalah blok bersama yang dipakai ulang lewat `<<: *hadoop`, supaya `build`, `image`, dan `env_file` tidak ditulis empat kali.

**`infra/config`**: konfigurasi Hadoop dalam bentuk variabel lingkungan. Image resmi mengubah baris `CORE-SITE.XML_x=y` menjadi properti `x=y` di `core-site.xml`, dan seterusnya. Yang penting:

| Baris config | Arti |
|---|---|
| `fs.defaultFS=hdfs://namenode` | Path seperti `/data/x.csv` berarti file di HDFS. |
| `dfs.replication=1` | Hanya ada satu datanode, jadi setiap blok disimpan satu kali. |
| `mapreduce.framework.name=yarn` | Job MapReduce dijalankan oleh YARN, bukan mode lokal. |
| `yarn.nodemanager.resource.memory-mb=4096` dan `cpu-vcores=4` | Kapasitas NodeManager: 4096 MB dan 4 vcore. Catatan jujur soal vcore ada di 5.10. |
| `pmem-check-enabled=false`, `vmem-check-enabled=false` | Bawaan contoh konfigurasi resmi image `apache/hadoop`. Efeknya, YARN tidak mematikan container yang memorinya melewati jatah. Artinya batas memori per container tidak dipaksakan (lihat 5.10). |
| `yarn.nodemanager.aux-services=mapreduce_shuffle` | Layanan yang memindahkan output map ke reducer (fase shuffle). |
| `resource-calculator=DefaultResourceCalculator` | Juga bawaan contoh resmi. Scheduler hanya menghitung **memori** saat membagi container, vcore tidak ikut dihitung. |

**`infra/run_hadoop_job.sh`**: menjalankan satu job.

| Baris | Kode | Penjelasan |
|---|---|---|
| 3 | `set -euo pipefail` | Berhenti kalau ada perintah gagal, variabel belum diisi, atau bagian pipe gagal. |
| 6-7 | `INPUT`, `NAME` | Argumen file CSV (default `taxi_100k.csv`) dan namanya tanpa `.csv`. |
| 9-12 | `SUFFIX` | Kalau argumen kedua `combiner`, hasil disimpan dengan akhiran `_combiner` supaya tidak menimpa hasil tanpa combiner. |
| 17 | `hdfs dfsadmin -safemode wait` | Sesaat setelah dinyalakan, NameNode ada di *safe mode* (hanya baca). Tunggu sampai selesai. |
| 18-19 | `hdfs dfs -mkdir`, `-put -f` | Salin CSV dari disk lokal ke HDFS. Job Hadoop membaca input dari HDFS. |
| 20 | `hdfs dfs -rm -r -f` | Hapus folder output lama. Hadoop menolak job kalau folder output sudah ada. |
| 22, 29-30 | `START`, `END`, `ELAPSED` | Waktu job saja, tidak termasuk upload ke HDFS dan ambil hasil. |
| 24-28 | `hadoop jar hadoop-streaming-*.jar ...` | **Inti.** Hadoop Streaming menjalankan program apa pun yang membaca stdin dan menulis stdout. `-files` mengirim ketiga script ke setiap task. `${SUFFIX:+-combiner "python3 combiner.py"}` artinya: kalau `SUFFIX` terisi, tambahkan opsi `-combiner`; kalau kosong, tidak ditambahkan apa-apa. Log lengkap (termasuk counters) disimpan ke `output/hadoop/hadoop_log_<nama>.txt`. |
| 32-34 | `rm -f`, `hdfs dfs -getmerge` | Hapus hasil lokal lama beserta file checksum `.crc` pasangannya, lalu gabungkan semua file `part-*` dari HDFS menjadi satu file lokal. |
| 36-43 | `python3 - ... <<'PY'` | Cetak laporan memakai `format_cli_report` yang sama, dengan nama engine sebagai judul. |
| 44 | `grep -E "..."` | Tampilkan counters penting: record map, record combiner, Spilled Records, shuffle bytes, CPU, memori. |

### 4.7 `spark/spark_job.py`: versi Spark (DataFrame dan RDD)

Satu file, dua mode. Filter dan agregasinya sama dengan mapper dan reducer.

| Baris | Kode | Penjelasan |
|---|---|---|
| 15-17 | `from pyspark.sql import ...` | `SparkSession` adalah pintu masuk Spark. `functions as F` berisi fungsi kolom (`col`, `sum`, `count`, ...). |
| 21-22 | `sys.path.insert`, `import format_cli_report` | Pakai ulang fungsi laporan dari `run_mapreduce.py`. |
| 25-32 | `SCHEMA = StructType([...])` | Tipe tiap kolom ditulis manual. Waktu dan zona dibaca sebagai string, sama seperti mapper. Alasan di 5.9. |

**Mode DataFrame** (`run_dataframe`, baris 35-62):

| Baris | Kode | Penjelasan |
|---|---|---|
| 36 | `spark.read.csv(..., header=True, schema=SCHEMA)` | Baca CSV menjadi DataFrame. Baris pertama dianggap header. Nilai yang tidak bisa dibaca sebagai angka menjadi `null`. |
| 39-44 | `df.where(...)` | Filter yang sama dengan mapper: `isNotNull` setara `except ValueError`, lalu tarif `> 0`, jarak `>= 0`, zona tidak kosong, panjang waktu minimal 13 karakter. |
| 47-54 | `select(F.explode(F.array(...)))` | **Setara dua `print` di mapper.** `F.array` membuat array berisi dua key (`ZONE\|...` dan `HOUR\|...`), lalu `explode` memecah setiap baris menjadi dua baris, satu per key. `F.substring(..., 12, 2)` mengambil jam; posisi di Spark mulai dari 1, jadi posisi 12 sama dengan index 11 di Python. |
| 57-61 | `groupBy("key").agg(count, sum, sum)` | **Setara reducer.** Hitung jumlah trip, total tarif, total jarak per key. Rata-rata belum dihitung di sini. |
| 62 | `result.collect()` | Ambil hasil (271 baris untuk 1m) ke Python. **Sampai baris ini, Spark belum memproses apa pun.** Baris 36-61 hanya menyusun rencana (*lazy evaluation*). Eksekusi terjadi saat ada *action* seperti `collect()`. `collect()` aman karena hasilnya kecil; untuk hasil jutaan baris, hasil harus ditulis ke file, bukan dibawa ke driver. |

**Mode RDD** (`parse_line` dan `run_rdd`, baris 65-91):

| Baris | Kode | Penjelasan |
|---|---|---|
| 65-82 | `parse_line(line)` | **Setara mapper.py**, termasuk semua filternya. Satu baris CSV menjadi daftar pasangan `(key, (1, amount, distance))`. Bedanya hanya bentuk keluaran: mapper mencetak teks, fungsi ini mengembalikan tuple Python. Header terbuang sendiri karena `float("total_amount")` gagal. |
| 86-88 | `sc.textFile(input_csv, minPartitions=sc.defaultParallelism)` | Baca file sebagai baris teks. Tanpa `minPartitions`, `textFile` membuat paling sedikit 2 partisi saja (`min(defaultParallelism, 2)`), sehingga untuk file kita hanya 2 dari 4 core yang bekerja. Dengan `defaultParallelism` (= 4 pada `local[4]`), file dibagi minimal 4 partisi. |
| 88 | `.flatMap(parse_line)` | Jalankan `parse_line` pada setiap baris. `flatMap`, bukan `map`, karena satu baris menghasilkan 0 atau 2 pasangan dan semuanya diratakan menjadi satu daftar. |
| 90 | `reduceByKey(lambda a, b: ...)` | **Setara reducer.py.** Gabungkan dua value dengan key sama dengan menjumlah tiap komponen. Spark menjalankan fungsi ini dulu **di dalam tiap partisi sebelum shuffle**, jadi perannya juga seperti combiner. |
| 91 | `totals.collect()` | Action yang memicu eksekusi. |

**Bagian `main()`** (baris 94-130):

| Baris | Kode | Penjelasan |
|---|---|---|
| 95-98 | `argparse` | Argumen: path CSV dan `--mode df` atau `--mode rdd`. |
| 102-107 | `SparkSession.builder...` | `master("local[4]")`: Spark berjalan di satu mesin dengan 4 thread pekerja. `spark.driver.memory=4g`: heap JVM 4 GB. Dipilih supaya kira-kira setara dengan kapasitas YARN (4 vcore, 4096 MB). Kenapa "kira-kira" ada di 5.10. `showConsoleProgress=false` menyembunyikan progress bar. |
| 108 | `setLogLevel("ERROR")` | Sembunyikan log INFO/WARN supaya laporan terbaca. |
| 109 | `startup = ...` | Waktu menyalakan Spark (JVM dan context). Dicatat terpisah karena ini biaya tetap yang tidak tergantung ukuran data. |
| 111-114 | `if args.mode == "df": ...` | Pilih DataFrame atau RDD. |
| 115-116 | `elapsed`, `spark.stop()` | Waktu total (startup + proses), lalu matikan Spark. |
| 119-122 | `lines = [...]` | Ubah hasil ke format teks yang **sama persis** dengan output reducer, diurutkan berdasarkan key. Rata-rata dihitung di sini (total / count), seperti di reducer. |
| 123-126 | tulis file | Simpan ke `output/spark/spark_<mode>_result_<nama>.tsv`. |
| 128-130 | laporan | Laporan CLI yang sama dengan engine lain, ditambah pembagian waktu startup dan pemrosesan. |

### 4.8 `benchmark/verify_results.py`: bukti kebenaran

Membandingkan file hasil semua engine untuk satu dataset. Engine pertama yang filenya ada dijadikan acuan.

| Baris | Kode | Penjelasan |
|---|---|---|
| 13 | `TOLERANCE = 0.011` | Batas selisih untuk nilai desimal. Kenapa 0,011 dan bukan 0,01: di komputer, `22.93 - 22.92` menghasilkan `0.010000000000001563`, sedikit di atas 0,01. Dengan batas 0,01 selisih pembulatan normal akan dianggap salah. |
| 16-22 | `load(path)` | Baca file hasil menjadi dictionary `key -> (count, total, avg_amt, avg_dist)`. |
| 27-35 | `files`, `results` | Lokasi hasil kelima engine. Engine yang belum dijalankan dilewati. |
| 40-41 | `rows.keys() != ref.keys()` | Daftar key harus sama persis. |
| 42-45 | perbandingan per key | **Jumlah trip harus identik.** Total dan rata-rata boleh beda maksimal 0,011. |
| 50 | `sys.exit(0 if ok else 1)` | Exit code 1 kalau ada perbedaan, jadi bisa dipakai di script lain. |

Kenapa nilai desimal boleh berbeda sedikit: penjumlahan float tidak persis, dan `(a + b) + c` bisa berbeda di digit sangat kecil dengan `a + (b + c)`. Setiap engine menjumlah dalam urutan berbeda (Hadoop per split, Spark per partisi, lokal berurutan). Setelah dibulatkan ke 2 desimal, digit terakhir kadang bergeser 0,01. Contoh nyata: zona 136 di Spark rata-rata tarifnya `22.93`, di Hadoop `22.92`. Totalnya `91.70` untuk 4 trip, dan `91.70 / 4 = 22.925` tepat di batas pembulatan.

### 4.9 `benchmark/run_benchmark.py`: pengukuran performa

| Baris | Kode | Penjelasan |
|---|---|---|
| 37-45 | `command_for(engine, name)` | Perintah untuk kelima engine. Hadoop dijalankan lewat `docker compose exec` ke container namenode; varian combiner menambahkan argumen `combiner`. |
| 48-79 | `run_and_measure(cmd)` | Jalankan perintah, lalu setiap 0,1 detik catat CPU dan memori proses beserta semua proses anaknya (`children(recursive=True)`). Proses anak perlu dihitung karena Spark menjalankan JVM sebagai proses terpisah, dan versi lokal menjalankan mapper, sort, reducer sebagai proses terpisah. |
| 54 | `cpu_by_pid` | Simpan CPU terakhir per proses, jadi proses yang sudah selesai tetap terhitung. |
| 69 | `peak_rss = max(...)` | Memori puncak = jumlah RSS semua proses pada satu titik sampling, diambil yang terbesar. |
| 73-74 | `returncode != 0` | Kalau engine gagal, benchmark berhenti dengan pesan error, bukan mencatat angka palsu. |
| 75-79 | `getrusage(RUSAGE_CHILDREN)` | Proses yang sangat singkat bisa selesai di antara dua sampling. `rusage` dari sistem operasi mencatat CPU semua proses anak yang sudah selesai. Dipakai nilai yang lebih besar dari kedua cara. |
| 82-92 | `hadoop_counters(log_name)` | Proses task Hadoop berjalan di dalam container, bukan proses anak dari benchmark, jadi tidak terlihat oleh psutil. CPU dan memori diambil dari **Hadoop counters** di log job: `CPU time spent (ms)`, `Physical memory (bytes) snapshot`, `Spilled Records`, `Reduce shuffle bytes`. |
| 104-118 | loop | Untuk setiap dataset, setiap engine, setiap ulangan: jalankan, ukur, simpan. |
| 108 | `re.search(r"Waktu Eksekusi: ...")` | Ambil waktu yang dilaporkan engine sendiri dari laporan CLI. |
| 120-124 | tulis CSV | Semua run disimpan ke `benchmark/results/benchmark.csv`. |
| 126-134 | median | Ringkasan memakai median dari 3 ulangan. Alasan di 5.13. |

Dua ukuran waktu:

- `wall_sec`: waktu dari luar, dari perintah dijalankan sampai selesai. Untuk Hadoop termasuk `docker exec`, upload ke HDFS, dan `getmerge`. Untuk Spark termasuk menyalakan Python dan JVM.
- `job_sec`: waktu yang dilaporkan engine sendiri. Untuk Hadoop hanya `hadoop jar` (submit sampai job selesai). Untuk Spark termasuk startup SparkSession.

---

## 5. Alasan setiap pilihan dan alternatifnya

Setiap pilihan ditulis dengan pola yang sama: apa yang kita pakai, alternatifnya, kenapa memilih ini, dan kelemahannya. Kalau kita **tidak mengukur** alternatifnya, itu ditulis. Jangan menjawab dosen dengan "ini yang terbaik" kalau kita tidak punya angka pembandingnya.

### 5.1 Dataset: NYC Yellow Taxi

- **Alternatif**: log server, dataset e-commerce dari Kaggle, data buatan sendiri.
- **Kenapa ini**: data transaksi asli (sesuai contoh soal), dari sumber resmi pemerintah kota, bisa diunduh ulang siapa saja dengan URL yang sama, sehingga hasil kita bisa diulang. Jumlah barisnya (2,96 juta) cukup untuk dibuat tiga ukuran. Isinya cocok untuk agregasi group-by yang menjadi pola dasar MapReduce.
- **Kelemahan**: file terbesar kita 127 MB (satu bulan penuh). Itu kecil untuk ukuran "big data", bahkan masih di bawah satu blok HDFS (128 MB), jadi Hadoop tetap membuat 2 split. Hasil benchmark kita menggambarkan perilaku engine pada data kecil di satu mesin, bukan pada skala yang menjadi alasan Hadoop dan Spark dibuat.

### 5.2 Format input: CSV, bukan Parquet

- **Alternatif**: semua engine membaca Parquet asli.
- **Kenapa CSV**: Hadoop Streaming membaca input sebagai baris teks (`TextInputFormat`). Membaca Parquet dari Hadoop Streaming butuh InputFormat khusus dan library tambahan di container. Supaya semua engine membaca file yang sama persis, kita pakai CSV.
- **Kelemahan**: ini **merugikan Spark**. Spark DataFrame bisa membaca Parquet langsung, hanya kolom yang dibutuhkan, dan datanya sudah terkompresi dan bertipe. Dari CSV pun Spark hanya mengubah 4 kolom yang dipakai (terlihat di `FileScan csv` pada `explain()`), tetapi seluruh teks setiap baris tetap harus dibaca. Pada pemakaian nyata Spark hampir selalu dipakai dengan Parquet. Kita tidak mengukur Spark dengan Parquet, jadi kita tidak bisa bilang seberapa lebih cepat.
- **Catatan parsing**: mapper dan RDD memakai `line.split(",")`. Ini benar untuk file kita karena tidak ada nilai yang berisi koma atau tanda kutip, tapi tidak benar untuk CSV umum (misalnya alamat `"Jl. A, No. 5"`). Spark DataFrame memakai parser CSV yang menangani kasus itu. Untuk data kita hasilnya sama, dan `verify_results.py` membuktikannya.
- **Kolom `passenger_count` dan `payment_type`** ikut disimpan tapi tidak dipakai. Jujur saja: ini sisa rencana awal. Membuangnya tidak mengubah hasil, hanya membuat file sedikit lebih kecil.

### 5.3 Subset: N baris pertama, bukan sampel acak

- **Alternatif**: sampel acak 100k/500k/1m dari 2,96 juta baris.
- **Kenapa N baris pertama**: deterministik (setiap anggota yang menjalankan `prepare_dataset.py` mendapat file yang sama persis), sederhana (`table.slice`), dan file yang lebih besar **memuat** file yang lebih kecil. Jadi saat membandingkan 100k, 500k, 1m, dan full, yang berubah hanya jumlah data. Sudah dicek: 1.000.001 baris pertama `taxi_full.csv` identik byte per byte dengan `taxi_1m.csv` (`md5sum`).
- **Kelemahan**: tidak representatif untuk kesimpulan bisnis. File asli kira-kira terurut waktu, jadi 100k baris pertama kira-kira hanya 1-2 Januari (Tahun Baru) dan 1m kira-kira 1-12 Januari. Ada juga beberapa baris dengan tanggal di luar Januari 2024 (misalnya 2002, 2009, 31 Desember 2023) yang tidak kita buang karena soalnya membandingkan engine, bukan membersihkan data. Kesimpulan seperti "jam tersibuk" dari 100k jangan digeneralisasi ke satu bulan. Untuk benchmark ini tidak masalah, karena biaya agregasi tergantung jumlah baris, bukan isi tanggalnya.

### 5.4 Hadoop Streaming dengan Python, bukan Java atau mrjob

- **Alternatif 1, Java MapReduce** (`Mapper`/`Reducer` dari API Hadoop). Ini cara asli Hadoop. Data tetap di dalam JVM dalam bentuk `Writable`, tidak perlu proses lain.
- **Alternatif 2, mrjob**: library Python yang membungkus Hadoop Streaming.
- **Kenapa Streaming + Python**: mapper dan reducer bisa dibaca dalam bahasa yang sama dengan versi Spark (PySpark), jadi perbandingan logikanya baris demi baris mudah. Alurnya tetap MapReduce asli di YARN: input split di HDFS, map task, sort, shuffle, reduce task. Hadoop Streaming adalah bagian resmi distribusi Hadoop.
- **Kenapa bukan mrjob**: mrjob menyembunyikan langkah yang justru harus kita jelaskan (perintah `hadoop jar`, `-files`, cara key dipisah tab). Rilis terakhirnya 0.7.4 pada September 2020. Tidak ada yang ia tambahkan yang kita butuhkan.
- **Kelemahan**: Streaming lebih lambat dari Java. Setiap record diubah ke teks, dikirim lewat pipe ke proses Python, lalu output-nya dibaca balik oleh JVM. Jadi angka Hadoop kita **memuat biaya Streaming**. Kita tidak membuat versi Java, jadi kita tidak bisa bilang seberapa besar biaya itu. Saat membandingkan, sebut "Hadoop Streaming", bukan "Hadoop" secara umum.

### 5.5 Cluster: Docker pseudo-distributed

- **Alternatif**: mode lokal Hadoop (tanpa HDFS dan YARN), cluster multi-node sungguhan, layanan cloud (EMR, Dataproc).
- **Kenapa ini**: HDFS dan YARN yang dipakai adalah yang asli, bukan simulasi. Job benar-benar diajukan ke ResourceManager dan dijalankan di container YARN, sehingga counters (Spilled Records, shuffle bytes) adalah angka nyata. Kelima anggota bisa menjalankannya di laptop masing-masing tanpa biaya cloud.
- **Kelemahan**: semua container berbagi satu disk, CPU, dan RAM. Tidak ada transfer data antar node lewat jaringan, padahal shuffle di cluster nyata melewati jaringan. Satu datanode, replikasi 1, jadi toleransi kegagalan HDFS tidak teruji.

### 5.6 Combiner: kenapa ada varian dengan dan tanpa

- **Tanpa combiner** adalah bentuk MapReduce paling dasar: semua output mapper dikirim ke reducer. Ini memperlihatkan dengan jelas biaya disk dan shuffle di MapReduce.
- **Dengan combiner** adalah perbandingan yang lebih adil dengan Spark, karena `reduceByKey` dan `groupBy().agg()` di Spark selalu menjumlah dulu di tiap partisi. Membandingkan Spark (dengan agregasi parsial) melawan Hadoop tanpa combiner sama saja membandingkan dua hal yang tidak setara.
- **Efeknya pada `taxi_100k`** (dari log Hadoop): `Map output records=196804`, `Combine output records=449`, `Reduce shuffle bytes` turun dari sekitar 4,6 MB menjadi 12.443 byte, `Spilled Records` turun menjadi 898. Pengaruhnya ke waktu total ada di bagian 7.
- **Kenapa combiner aman di sini**: operasinya penjumlahan, dan formatnya sama dengan mapper (lihat 4.4). Hadoop tidak menjamin combiner dijalankan, dan hasil kita tetap benar kalau combiner tidak jalan sama sekali.

### 5.7 Satu job dengan prefix key, bukan dua job

- **Alternatif**: job pertama untuk agregasi zona, job kedua untuk agregasi jam.
- **Kenapa satu job**: input dibaca sekali, dan biaya startup YARN (puluhan detik per job, lihat bagian 7) dibayar sekali.
- **Kelemahan**: output zona dan jam bercampur dalam satu file, jadi harus dipisah lagi berdasarkan prefix (`format_cli_report` melakukannya). Mapper menghasilkan dua kali jumlah baris input.

### 5.8 Reducer streaming, bukan dictionary

- **Alternatif**: reducer menyimpan semua key di `dict` Python lalu mencetak di akhir.
- **Kenapa streaming**: ini kontrak MapReduce. Framework sudah mengurutkan input reducer, jadi reducer cukup menyimpan satu key. Memorinya tetap kecil berapa pun jumlah key.
- **Jujur**: dengan hanya 271 key (data 1m), versi `dict` juga akan bekerja dengan baik. Keuntungan versi streaming baru terasa kalau jumlah key jutaan.

### 5.9 Skema manual, bukan `inferSchema`

- **Alternatif**: `spark.read.csv(..., inferSchema=True)`.
- **Kenapa manual**: `inferSchema` membuat Spark membaca data tambahan hanya untuk menebak tipe, sebelum pemrosesan sebenarnya. Dengan skema manual, tipe juga terkendali: zona dibaca sebagai string (sama dengan mapper yang menghasilkan key teks `ZONE|132`), dan waktu dibaca sebagai string supaya jamnya bisa diambil dengan `substring` seperti `[11:13]` di mapper.
- **Kelemahan**: kalau urutan kolom CSV berubah, skema harus diubah manual. Kita tidak mengukur berapa detik yang dihemat dibanding `inferSchema`.

### 5.10 Batas resource: `local[4]` + 4 GB lawan YARN 4096 MB / 4 vcore

- **Tujuannya** supaya kedua engine kira-kira mendapat jatah mesin yang sama. **Kira-kira, bukan identik**, dan ini perlu dijelaskan kalau ditanya:
  1. YARN 4096 MB adalah total memori container yang dibagi ke ApplicationMaster, map task, dan reduce task. Spark 4 GB adalah heap JVM driver saja; proses Python worker di mode RDD berada di luar angka itu.
  2. Scheduler YARN kita memakai `DefaultResourceCalculator` (ada di `infra/config`), yang hanya menghitung memori. Angka 4 vcore tidak benar-benar membatasi CPU. Container Docker kita juga tidak diberi batas CPU.
  3. Hadoop membuat 2 split (`number of splits:2` di log), jadi paling banyak 2 map task berjalan bersamaan. Spark `local[4]` memakai sampai 4 thread.
- **Kenapa tidak disamakan persis**: dua engine ini membagi resource dengan cara berbeda, jadi tidak ada satu angka yang membuat keduanya identik. Kita memilih kapasitas yang sama di atas kertas dan mencatat perbedaannya di sini.

### 5.11 Spark DataFrame: `explode` + satu `groupBy`

- **Versi sebelumnya** memakai `cache()`, dua `groupBy` (zona dan jam), lalu `unionByName`. Kita ganti karena `cache()` menyimpan seluruh data hasil filter di memori, dan kita tidak pernah mengukur apakah itu lebih cepat. Mengklaim "cache membuat Spark cepat" tanpa angka tidak bisa dipertanggungjawabkan.
- **Versi sekarang**: `explode` memecah setiap baris menjadi dua key, lalu satu `groupBy`. CSV dibaca sekali dan hanya ada satu shuffle. Strukturnya sama dengan mapper (dua output per baris) dan reducer (jumlah per key), jadi mudah dibandingkan langsung.
- **Alternatif lain**: `GROUPING SETS` di Spark SQL juga bisa menghitung dua pengelompokan dalam satu kali baca, tetapi hasilnya berisi `null` di kolom yang tidak dikelompokkan dan perlu diolah lagi supaya formatnya sama dengan reducer.
- **Jujur**: kita tidak membandingkan waktu ketiga cara ini. Alasan memilih `explode` adalah kesetaraan logika dengan MapReduce, bukan karena terbukti paling cepat.

### 5.12 `sort` dengan `LC_ALL=C`

- **Alternatif**: `sort` dengan bahasa bawaan komputer (misalnya `en_US.UTF-8`).
- **Kenapa `LC_ALL=C`**: urutan berdasarkan nilai byte, sama dengan cara Hadoop mengurutkan key teks. Hasilnya sama di komputer mana pun, dan biasanya lebih cepat karena tidak memakai aturan bahasa.
- **Jujur**: reducer hanya butuh key yang sama berdampingan, dan `sort` dengan bahasa lain pun memenuhi itu. Jadi `LC_ALL=C` di sini soal urutan yang konsisten dengan Hadoop dan bisa diulang, bukan soal hasil yang salah.

### 5.13 Median dari 3 ulangan

- **Alternatif**: rata-rata, atau lebih banyak ulangan.
- **Kenapa median**: satu run yang lambat (misalnya run pertama saat file belum ada di cache disk) tidak menggeser median, tetapi menggeser rata-rata.
- **Kelemahan**: 3 ulangan itu sedikit. Kita tidak menghitung simpangan atau selang kepercayaan. Semua angka per ulangan ada di `benchmark.csv`, jadi variasinya bisa dilihat langsung. Selisih kecil antar engine (di bawah sekitar 1 detik) jangan ditafsirkan sebagai perbedaan nyata.

### 5.14 Cara mengukur resource: psutil + rusage untuk lokal, counters untuk Hadoop

- **Kenapa dua cara**: proses task Hadoop berjalan di dalam container, sebagai anak dari daemon YARN, bukan anak dari script benchmark. psutil tidak bisa menghubungkannya ke run kita. Hadoop sendiri mencatat CPU dan memori setiap task di counters.
- **Kelemahan**: angkanya tidak setara persis. Counters Hadoop hanya menghitung map task dan reduce task (termasuk proses Python anaknya), tidak menghitung ApplicationMaster, ResourceManager, NodeManager, NameNode, DataNode. Pengukuran Spark menghitung seluruh JVM driver dan semua proses Python. Bandingkan **pola** (mana yang naik seiring ukuran data), jangan bandingkan angka memori Hadoop lawan Spark secara mentah.

### 5.15 Satu fungsi laporan untuk semua engine

`format_cli_report` dipakai oleh kelima engine. Tujuannya supaya perbedaan tampilan tidak mungkin berasal dari kode laporan yang berbeda, dan perbedaan hasil hanya bisa berasal dari engine-nya. Bukti kebenaran tetap dari `verify_results.py`, bukan dari membandingkan laporan dengan mata.

---

## 6. MapReduce dan Spark: apa bedanya di balik layar

### 6.1 Data antara: disk lawan memori

| | Hadoop MapReduce | Spark |
|---|---|---|
| Output map | Ditulis ke **disk lokal** (spill), lalu dibaca lagi oleh reducer | Output shuffle juga ditulis ke file lokal, tetapi data di dalam satu tahap mengalir di memori tanpa ditulis |
| Bukti di proyek kita | Tanpa combiner sampai 1m, `Spilled Records` tepat 2x `Map output records` (1m: 3.946.876 = 2 x 1.973.438): setiap record antara ditulis ke disk dua kali, sekali di sisi map dan sekali saat merge di sisi reduce. Di data full menjadi **3x** (17.572.224 = 3 x 5.857.408): output map tidak lagi muat di buffer sort (`mapreduce.task.io.sort.mb`, default 100 MB, mulai ditulis saat 80% penuh), jadi setiap map menulis beberapa file spill lalu menggabungkannya lagi di disk. Semakin besar data, semakin banyak tulis-baca disk per record | Filter, `explode`/`flatMap`, dan agregasi parsial terjadi dalam satu tahap, baru hasil parsialnya yang di-shuffle |
| Pipeline multi-tahap | Setiap tahap adalah job terpisah, hasil antar job ditulis ke HDFS | Banyak tahap dirangkai dalam satu DAG |

Hati-hati: jangan bilang "Spark tidak pakai disk". Spark juga menulis file shuffle ke disk lokal. Bedanya, Spark tidak menulis data antara di setiap langkah, dan untuk pipeline banyak tahap hasil antar tahap tidak perlu ditulis ke HDFS.

### 6.2 Shuffle dan agregasi parsial

- Hadoop tanpa combiner: untuk 1m baris, sekitar 2 juta pasangan dikirim ke reducer (`Reduce shuffle bytes=46392925`, sekitar 44 MB), hampir sebesar file input (43 MB).
- Hadoop dengan combiner: yang dikirim hanya satu baris per key per map task. Untuk 100k, `Reduce shuffle bytes=12443`.
- Spark `reduceByKey` (RDD) dan `groupBy().agg()` (DataFrame) melakukan agregasi parsial di tiap partisi secara otomatis. Di Hadoop, combiner harus ditulis dan dipasang sendiri.

### 6.3 Model pemrograman

| | Hadoop Streaming | Spark RDD | Spark DataFrame |
|---|---|---|---|
| Cara menulis | Program terpisah (mapper, combiner, reducer) yang berkomunikasi lewat teks stdin/stdout | Fungsi Python (`flatMap`, `reduceByKey`) | Operasi deklaratif mirip SQL (`where`, `explode`, `groupBy`, `agg`) |
| Parsing | Manual | Manual | Dari skema |
| Optimasi | Urutan langkah tetap | Spark menjalankan fungsi Python apa adanya | Optimizer Catalyst menyusun rencana eksekusi |
| Tempat logika dijalankan | Proses Python di setiap task | Proses Python worker | Di dalam JVM |

Catatan untuk RDD di PySpark: `parse_line` dan lambda `reduceByKey` adalah kode Python, sedangkan Spark berjalan di JVM. Setiap baris dikirim dari JVM ke proses Python worker dan hasilnya dikirim balik. DataFrame tidak mengalami ini karena `where`, `substring`, `explode`, `sum` dijalankan langsung di JVM. Hadoop Streaming punya biaya yang mirip: data berpindah antara JVM task dan proses Python lewat pipe.

### 6.4 Overhead tetap (startup)

- **Hadoop/YARN**: setiap job meminta container ke ResourceManager, memulai ApplicationMaster (JVM), lalu memulai JVM untuk setiap map task dan reduce task. Biaya ini puluhan detik dan hampir tidak tergantung ukuran data.
- **Spark**: menyalakan JVM dan SparkSession, dicetak sebagai `Startup SparkSession`.
- **MapReduce lokal**: hanya memulai beberapa proses Python dan `sort`.

Akibatnya, untuk data kecil overhead mendominasi. Keunggulan Hadoop dan Spark (paralelisme lintas mesin, toleransi kegagalan, data yang tidak muat di satu mesin) tidak terlihat di pengujian kita, dan itu wajar untuk ukuran data ini.

### 6.5 Keterbatasan pengujian kita (sampaikan apa adanya)

1. **Satu mesin.** Tidak ada transfer data antar node lewat jaringan.
2. **Data kecil** (paling besar 127 MB, masih di bawah satu blok HDFS, dan muat di RAM).
3. **Input CSV**, yang merugikan Spark (5.2).
4. **Hadoop lewat Streaming**, yang lebih lambat dari Java MapReduce (5.4).
5. **Batas resource kira-kira setara, bukan identik** (5.10).
6. **Cara ukur resource berbeda** antara Hadoop dan engine lokal (5.14).
7. **Mesin dipakai bersama** aplikasi lain. Karena itu setiap konfigurasi diulang 3 kali dan dipakai median, tapi 3 ulangan tetap sedikit (5.13).
8. **Hadoop di Docker, Spark langsung di host.**

---

## 7. Hasil benchmark dan cara membacanya

Semua angka di bawah adalah **median dari 3 ulangan** (`python benchmark/run_benchmark.py --reps 3`, Sabtu 24 Oktober 2026). Angka setiap ulangan ada di `benchmark/results/benchmark.csv`, dan log lengkapnya di `benchmark/results/benchmark_log.txt`. Mesin: 16 core logis, 15 GB RAM, dipakai bersama aplikasi lain.

### 7.1 Kebenaran dulu

`verify_results.py` dijalankan setelah benchmark untuk ketiga dataset: **15 dari 15 perbandingan `OK`** (250 key untuk 100k, 264 untuk 500k, 271 untuk 1m). Kecepatan baru boleh dibandingkan setelah hasilnya terbukti sama.

### 7.2 Tabel hasil

| Dataset | Engine | wall (s) | job (s) | CPU (s) | Memori (MB) |
|---|---|---:|---:|---:|---:|
| 100k (4,5 MB) | MapReduce lokal | 0,55 | 0,43 | 0,51 | 22 |
| | Hadoop YARN | 40,27 | 28,00 | 7,26 | 802 |
| | Hadoop YARN + combiner | 43,70 | 27,56 | 6,14 | 757 |
| | Spark DataFrame | 16,53 | 15,57 | 46,36 | 744 |
| | Spark RDD | 14,10 | 12,54 | 38,16 | 820 |
| 500k (22,4 MB) | MapReduce lokal | 2,63 | 2,53 | 3,65 | 149 |
| | Hadoop YARN | 45,68 | 32,11 | 11,13 | 892 |
| | Hadoop YARN + combiner | 40,59 | 27,96 | 8,49 | 804 |
| | Spark DataFrame | 15,89 | 15,05 | 44,65 | 1100 |
| | Spark RDD | 12,71 | 11,25 | 36,63 | 819 |
| 1m (44,9 MB) | MapReduce lokal | 4,76 | 4,64 | 6,51 | 290 |
| | Hadoop YARN | 47,82 | 34,77 | 12,95 | 953 |
| | Hadoop YARN + combiner | 41,68 | 30,78 | 10,29 | 837 |
| | Spark DataFrame | 14,98 | 14,01 | 45,93 | 1219 |
| | Spark RDD | 13,50 | 12,05 | 38,63 | 823 |

Arti kolom (detail di 4.9):

- **job**: untuk Hadoop, hanya perintah `hadoop jar` (submit sampai job selesai). Untuk Spark, dari sebelum `SparkSession` dibuat sampai `collect()` selesai, jadi **termasuk startup Spark**.
- **wall**: seluruh proses dari luar. Untuk Hadoop, ditambah `docker exec`, `hdfs dfs -put`, `-rm`, dan `-getmerge`. Setiap perintah `hdfs` menyalakan JVM sendiri, jadi selisih wall dan job Hadoop sekitar 11-16 detik.
- **CPU dan memori**: Hadoop dari counter task, engine lokal dari psutil. Keduanya **tidak bisa dibandingkan langsung** (5.14).

Counter Hadoop untuk 1m (sama di ketiga ulangan):

| Counter | Tanpa combiner | Dengan combiner |
|---|---:|---:|
| Map output records | 1.973.438 | 1.973.438 |
| Combine output records | 0 | 531 |
| Spilled Records | 3.946.876 | 1.062 |
| Reduce shuffle bytes | 46.392.925 | 15.887 |

Pembagian waktu Spark, dari baris `Startup SparkSession` yang dicetak `spark_job.py`. Ini **satu kali jalan**, bukan median, jadi hanya untuk gambaran kasar:

| | DF 100k | RDD 100k | DF 1m | RDD 1m |
|---|---:|---:|---:|---:|
| Startup SparkSession (s) | 7,85 | 8,21 | 8,42 | 9,78 |
| Pemrosesan (s) | 6,02 | 3,20 | 7,45 | 4,98 |

### 7.3 Cara membacanya

**1. Pada ukuran data ini, biaya tetap mendominasi.** Hadoop butuh 28 detik untuk 100k, padahal datanya hanya 4,5 MB. Hampir semua waktu itu habis untuk meminta container ke YARN, menyalakan ApplicationMaster, dan menyalakan JVM per task (6.4). Spark menghabiskan sekitar 8-10 detik hanya untuk menyalakan JVM dan SparkSession. Urutan "lokal tercepat, lalu Spark, lalu Hadoop" sebagian besar adalah urutan biaya startup, bukan urutan kemampuan mengolah data.

**2. Data 10 kali lebih besar (100k ke 1m), kenaikan `job`:**

| Engine | 100k | 1m | Kenaikan |
|---|---:|---:|---:|
| MapReduce lokal | 0,43 | 4,64 | +4,21 |
| Hadoop YARN | 28,00 | 34,77 | +6,77 |
| Hadoop YARN + combiner | 27,56 | 30,78 | +3,22 |
| Spark DataFrame | 15,57 | 14,01 | -1,56 |
| Spark RDD | 12,54 | 12,05 | -0,49 |

- MapReduce lokal naik hampir sebanding dengan data (sekitar 10 kali), karena berjalan di satu proses tanpa paralelisme.
- Hadoop naik beberapa detik di atas biaya tetap sekitar 28 detik.
- Spark tidak terlihat naik. Ini **bukan** berarti data lebih besar diproses lebih cepat. Kenaikan biaya pengolahan Spark lebih kecil dari variasi antar waktu pengujian. Run 100k dijalankan lebih dulu dibanding run 1m, di saat beban aplikasi lain di laptop bisa berbeda. Selisih di bawah sekitar 2 detik tidak kami tafsirkan.
- Kami tidak menguji data yang lebih besar dari RAM atau lebih dari satu mesin, jadi tren ini **tidak boleh diekstrapolasi**. MapReduce lokal tercepat di sini karena 45 MB muat di memori satu mesin; ia tidak bisa membagi kerja ke mesin lain.

**3. Efek combiner terlihat jelas di counter, kecil di waktu.**

- Di 1m, shuffle turun dari 46.392.925 byte menjadi 15.887 byte, dan Spilled Records turun dari 3.946.876 menjadi 1.062.
- CPU task turun dari 12,95 menjadi 10,29 detik.
- Waktu job turun sekitar 4 detik di 500k (32,11 ke 27,96) dan di 1m (34,77 ke 30,78), tetapi tidak berbeda berarti di 100k (28,00 lawan 27,56).
- Combiner menghapus biaya per record (tulis ke disk, sort, kirim ke reducer), tetapi tidak menyentuh biaya tetap YARN. Jadi manfaatnya bertambah seiring jumlah record.

**4. Hadoop lawan Spark di 1m.** Spark DataFrame 14,01 detik dan RDD 12,05 detik, Hadoop tanpa combiner 34,77 detik, Hadoop dengan combiner 30,78 detik. Spark sekitar 2,2 sampai 2,9 kali lebih cepat, **untuk data ini, di mesin ini, dengan konfigurasi ini**. Penyebabnya bercampur dan tidak kami pisahkan:

- biaya YARN yang hanya dibayar Hadoop (5.10);
- data antara Hadoop yang ditulis ke disk;
- Hadoop di Docker dengan Java 8, Spark di host dengan Java 21.

**5. CPU Spark jauh di atas wall dan hampir tidak berubah dengan ukuran data.** DF sekitar 45 detik CPU dan RDD sekitar 38 detik, di 100k maupun 1m. Karena angkanya tetap, sebagian besar CPU itu kemungkinan biaya tetap: JVM startup, kompilasi JIT, codegen, dan proses Python worker. Ini dugaan dari polanya; kami tidak memprofil CPU. Untuk Hadoop, counter CPU hanya menghitung task (7-13 detik), tidak termasuk daemon.

**6. DataFrame lebih lambat dari RDD di semua ukuran (2-4 detik).**

- *Diukur*:
  - Startup keduanya mirip; selisihnya ada di pemrosesan (satu kali jalan: DF 6,02 lawan RDD 3,20 detik di 100k).
  - Plan DF menunjukkan dua job: AQE menjalankan stage map sebagai job tersendiri, lalu stage reduce yang sudah digabung menjadi 1 task.
- *Dugaan*, tidak kami profil: DF membayar perencanaan Catalyst dan kompilasi codegen di setiap run, dan biaya itu belum terbayar di data sekecil ini.
- Di PySpark, DataFrame biasanya lebih cepat dari RDD pada data besar, karena RDD harus mengirim setiap baris dari JVM ke proses Python dan kembali. Kondisi itu tidak kami uji, jadi kami tidak mengklaimnya untuk proyek ini.

**7. Memori.**

- MapReduce lokal naik dari 22 MB ke 290 MB. Hampir semuanya dari `sort`, yang menahan seluruh data antara (40 MiB untuk 1m) di memori: diukur terpisah dengan `/usr/bin/time`, `sort` mencapai maxRSS 284 MiB, sedangkan mapper dan reducer sekitar 9 MB.
- Spark DF naik dari 744 MB ke 1219 MB, sedangkan RDD tetap sekitar 820 MB. Kami belum memeriksa penyebabnya. Kemungkinannya, heap JVM ikut membesar sebelum garbage collector berjalan.
- Semua angka Spark jauh di bawah batas 4 GB.

### 7.4 Kalimat kesimpulan yang aman diucapkan

- "Kelima engine memberi hasil yang sama untuk ketiga dataset; dibuktikan `verify_results.py` dan dicek silang dengan `awk` (8.6)."
- "Pada data sampai 45 MB di satu mesin, waktu Hadoop dan Spark didominasi biaya tetap startup, bukan pengolahan data."
- "Di 1m, Spark sekitar 2,2 sampai 2,9 kali lebih cepat dari Hadoop di pengujian kami; penyebabnya gabungan overhead YARN, I/O disk data antara, dan perbedaan lingkungan."
- "Combiner memperkecil shuffle Hadoop sekitar 2.900 kali dan memangkas sekitar 4 detik di 500k dan 1m."

Kalimat yang **tidak** boleh diucapkan:

- "Spark 2,5 kali lebih cepat dari Hadoop" tanpa syarat. Angka itu hanya berlaku untuk pengujian ini.
- "MapReduce lokal paling efisien, jadi tidak perlu Hadoop." Ia tidak bisa berjalan di lebih dari satu mesin.
- "Spark tidak memakai disk." Shuffle Spark juga menulis ke disk (6.1).

---

## 8. Bank pertanyaan kritis dosen

Dosen memahami kode dan akan menanyakan detail. Bagian ini berisi pertanyaan yang paling mungkin muncul, dikelompokkan per topik. Setiap jawaban punya tiga bagian:

- **Jawab**: yang diucapkan, singkat dan langsung.
- **Tunjukkan**: file dan baris, counter di log, atau perintah yang membuktikan jawaban.
- **Hindari**: kalimat yang terdengar meyakinkan tapi bisa dipatahkan.

Aturan menjawab:

1. Jawab yang ditanya dulu dalam satu atau dua kalimat, baru tunjukkan buktinya.
2. Kalau pilihan kita punya kelemahan, sebut kelemahannya sendiri sebelum dosen yang menyebut.
3. Kalau sesuatu tidak kita ukur, katakan "itu tidak kami ukur", lalu sebut dugaan dan cara membuktikannya. Jangan mengarang angka.
4. Bedakan dengan jelas **hasil ukur** (ada di log atau `benchmark.csv`) dan **perkiraan** (dari cara kerja engine).

### 8.1 Infrastruktur dan batas resource

**Kenapa YARN dibatasi 4096 MB dan 4 vcore? Laptop kalian punya lebih dari itu.**

- **Jawab**: Laptop yang dipakai punya 16 core logis dan 15 GB RAM, dan sekitar 10 GB sudah terpakai oleh sistem operasi dan aplikasi lain saat pengujian. Batas 4 GB dan 4 core dipilih supaya: (1) cluster berjalan tanpa kehabisan RAM, (2) Spark bisa diberi jatah nominal yang sama (`local[4]`, 4 GB), (3) anggota dengan laptop lebih kecil bisa menjalankan hal yang sama. Angka ini bukan hasil tuning; kami tidak mencari konfigurasi tercepat.
- **Tunjukkan**: `infra/config` (`memory-mb=4096`, `cpu-vcores=4`), `spark/spark_job.py` baris 101-107.
- **Hindari**: "supaya perbandingannya adil 100%". Batasnya hanya setara di atas kertas (5.10).

**Dengan 4096 MB, berapa task Hadoop yang berjalan bersamaan?**

- **Jawab**: Setiap map dan reduce container mendapat 1024 MB. ApplicationMaster memakai nilai default Hadoop 1536 MB (tidak kami ubah). 1536 + 2 x 1024 = 3584 MB, masih di bawah 4096, jadi kedua map task bisa berjalan bersamaan. Reduce container baru mendapat tempat setelah salah satu map selesai.
- **Tunjukkan**: di log, `Total megabyte-milliseconds taken by all map tasks` dibagi `Total time spent by all map tasks (ms)`. Untuk 500k: 13408256 / 13094 = 1024 MB per map container. Web UI YARN (http://localhost:8088) menampilkan memori yang terpakai saat job berjalan.

**Katanya 4 vcore, apakah CPU Hadoop benar-benar dibatasi 4 core?**

- **Jawab**: Tidak. Scheduler memakai `DefaultResourceCalculator`, yang hanya menghitung memori, dan container Docker kita tidak diberi batas CPU. Angka vcore hanya tercatat, tidak ditegakkan. Dalam praktiknya Hadoop kita menjalankan paling banyak 2 map task bersamaan, masing-masing satu JVM dan satu proses Python.
- **Tunjukkan**: `infra/config`, baris `resource-calculator=...DefaultResourceCalculator`.
- **Hindari**: "Hadoop dan Spark sama-sama dibatasi 4 core".

**Kenapa pengecekan memori (`pmem-check`, `vmem-check`) dimatikan? Bukankah itu berbahaya?**

- **Jawab**: Itu bawaan contoh konfigurasi resmi image `apache/hadoop`. Pengecekan memori virtual sering mematikan container JVM karena memori virtual JVM jauh lebih besar dari memori fisiknya. Konsekuensinya, batas 1024 MB per container tidak ditegakkan. Di pengujian kita ini tidak berpengaruh: puncak memori fisik per task sekitar 270-340 MB. Di cluster produksi, `pmem-check` sebaiknya dinyalakan.
- **Tunjukkan**: counter `Peak Map Physical memory (bytes)` dan `Peak Reduce Physical memory (bytes)` di `output/hadoop/hadoop_log_*.txt`.

**Kenapa replikasi HDFS 1, bukan 3?**

- **Jawab**: Hanya ada satu datanode. Replikasi 3 tidak bisa dipenuhi; blok akan tercatat kurang replika. Konsekuensinya tidak ada toleransi kegagalan, dan bagian itu memang tidak kami uji.

**Kenapa Hadoop jalan di Docker, sedangkan Spark tidak? Tidak setara.**

- **Jawab**: Benar, ini perbedaan dan kami catat sebagai keterbatasan. Hadoop butuh empat daemon dan konfigurasi jaringan, dan Docker membuat semua anggota bisa menyalakannya dengan satu perintah. Spark mode lokal cukup dipasang lewat `pip`. Di Linux, container memakai kernel host langsung (bukan mesin virtual), jadi biaya CPU-nya kecil, tetapi biaya Docker terhadap waktu Hadoop tidak kami ukur.

**Kenapa Spark tidak dijalankan di atas YARN yang sama?**

- **Jawab**: Image Hadoop memakai Java 8 (`openjdk 1.8.0_212`), sedangkan Spark 4.2 butuh Java 17 atau lebih baru. Spark di YARN perlu image baru dengan Java 17 dan distribusi Spark di setiap node. Kami memilih mode lokal. Konsekuensi yang harus diakui: **Spark lokal tidak membayar biaya meminta container ke YARN dan menyalakan ApplicationMaster, sedangkan Hadoop membayarnya.** Sebagian selisih waktu Hadoop dan Spark berasal dari sini, bukan hanya dari perbedaan disk dan memori. Versi Java juga berbeda: Hadoop berjalan di Java 8, Spark di Java 21.
- **Tunjukkan**: `docker compose -f infra/docker-compose.yml exec namenode java -version` dan `java -version` di host.
- **Hindari**: "Spark lebih cepat karena in-memory" sebagai satu-satunya alasan.

**Kenapa Hadoop 3.4.1 dan Spark 4.2.0?**

- **Jawab**: Hadoop 3.4.1 tersedia sebagai image resmi `apache/hadoop`, jadi tidak perlu membangun Hadoop sendiri. Spark 4.2.0 adalah versi PySpark yang terpasang dari pip saat proyek dikerjakan. Kedua pilihan itu soal kemudahan memasang, bukan karena fitur khusus versi tersebut.

**Kenapa hanya 2 map task? Bisa ditambah?**

- **Jawab**: Nilai default `mapreduce.job.maps` adalah 2, jadi file dibagi dua split. Bisa ditambah dengan `-D mapreduce.job.maps=4`, tetapi dengan container 1024 MB dan AM 1536 MB, hanya 2 map yang muat bersamaan dalam 4096 MB. Map tambahan akan antre. Efeknya terhadap waktu tidak kami ukur.
- **Tunjukkan**: log `number of splits:2`, `Launched map tasks=2`.

**Kenapa hanya 1 reducer?**

- **Jawab**: Default `mapreduce.job.reduces` adalah 1. Dengan 271 key (data 1m), satu reducer cukup, dan hasilnya satu file `part-00000`. Kalau reducer lebih dari satu, partitioner hash menjamin key yang sama selalu ke reducer yang sama, dan `getmerge` tetap menggabungkan semua file hasil.
- **Tunjukkan**: log `Launched reduce tasks=1`.

### 8.2 Data dan format CSV

**Kenapa Spark diberi CSV, bukan Parquet? Kalian sengaja melemahkan Spark?**

- **Jawab**: Supaya input kedua engine sama persis, file yang sama byte per byte. Hadoop Streaming membaca teks per baris. Kami akui pilihan ini merugikan Spark: CSV harus dibaca utuh sebagai teks dan di-parse di setiap baris. Spark DataFrame memang hanya mengubah 4 kolom yang dipakai (`FileScan csv [tpep_pickup_datetime, PULocationID, trip_distance, total_amount]` di `explain()`), tetapi seluruh baris tetap harus dibaca dari disk. Dengan Parquet, kolom yang tidak dipakai tidak dibaca sama sekali. Kalau tujuannya membuat Spark secepat mungkin, kami akan memakai Parquet. Tujuan kami membandingkan engine pada input yang sama.
- **Tunjukkan**: bagian 5.2.
- **Hindari**: "CSV lebih cepat" atau "Parquet tidak didukung Spark".

**Kenapa Spark membaca dari disk lokal, sedangkan Hadoop dari HDFS?**

- **Jawab**: Benar, sumber datanya berbeda. Job Hadoop di cluster membaca input dari HDFS. Spark mode lokal membaca file lokal; port RPC NameNode (8020) juga tidak dibuka ke host. Di satu mesin, HDFS menyimpan bloknya di disk yang sama, jadi perbedaannya ada di lapisan HDFS (komunikasi ke NameNode dan DataNode), bukan di disknya. Waktu upload ke HDFS (`hdfs dfs -put`) tidak masuk `job_sec`, tetapi masuk `wall_sec`.

**Data paling besar cuma satu bulan (127 MB). Itu bukan big data, kan?**

- **Jawab**: Betul, bukan. `taxi_full.csv` berisi 2.964.624 baris, 126,7 MiB, satu file bulanan dari sumbernya. Ukuran ini masih di bawah satu blok HDFS (128 MiB), jadi Hadoop tetap memakai 2 split seperti data yang lebih kecil, dan Spark masih menampung semuanya di memori 4 GB. Artinya hasil kami menggambarkan perilaku engine di data kecil sampai menengah di satu mesin. Pada skala itu overhead tetap (startup JVM, YARN, SparkSession) masih menyumbang porsi besar dari waktu total (lihat bagian 7). Kami tidak menguji gabungan beberapa bulan, jadi kami tidak membuat klaim tentang data yang lebih besar dari satu blok atau cluster multi-node.
- **Tunjukkan**: `ls -lh data/`, bagian 6.5, dan log Hadoop full (`Launched map tasks=2`).
- **Hindari**: "di data besar pasti Spark menang jauh". Itu tidak kami ukur.

**Bagaimana kalau satu baris CSV terpotong di batas dua split?**

- **Jawab**: `LineRecordReader` di Hadoop menangani ini. Setiap split selain yang pertama melewati baris pertamanya yang tidak utuh, dan setiap split tetap membaca melewati batas akhirnya sampai barisnya selesai. Jadi setiap baris dibaca tepat satu kali oleh satu mapper. Spark memakai mekanisme yang sama saat membaca file teks.
- **Tunjukkan**: log 1m: `Map input records=1000001`, yaitu 1.000.000 baris data ditambah 1 header. Tidak ada yang hilang atau terbaca dua kali.

**Apakah header ikut diproses mapper?**

- **Jawab**: Header hanya ada di split pertama, dan mapper melewatinya di `mapper.py` baris 10. Kalau tidak dilewati pun, `float("trip_distance")` akan gagal dan barisnya terbuang di `except ValueError`. Di RDD, header terbuang dengan cara kedua. Di DataFrame, header dibuang oleh `header=True`.

**Kenapa memakai `total_amount`, bukan `fare_amount`?**

- **Jawab**: `total_amount` adalah jumlah yang benar-benar dibayar penumpang (tarif argo, tip, tol, dan biaya tambahan), cocok untuk pertanyaan "pendapatan". Kolom "Avg Tarif" di laporan artinya rata-rata total bayar per trip, bukan tarif argo saja. Kalau yang ditanya tarif argo, kolomnya `fare_amount`.

**Berapa baris yang dibuang, dan kenapa?**

- **Jawab**: Di `taxi_1m.csv`, 13.281 baris dibuang, semuanya karena `total_amount <= 0`. Tidak ada jarak negatif, zona kosong, atau waktu terpotong di data ini; filter itu hanya pengaman.
- **Tunjukkan**: perintah ini menghitung langsung dari CSV, tanpa engine mana pun:
  ```bash
  awk -F, 'NR>1 && $6<=0' data/taxi_1m.csv | wc -l      # 13281
  ```

**Trip dengan jarak 0 kok tetap dihitung?**

- **Jawab**: Di 1m ada 14.415 trip dengan jarak 0. Aturan filter kami hanya membuang jarak negatif. Jarak 0 bisa berarti trip dibatalkan setelah penjemputan, tarif tetap, atau meter yang error. Kami tidak membersihkan outlier karena fokus tugas ini membandingkan engine, tetapi baris ini menurunkan rata-rata jarak. Kalau mau dibuang, ubah `distance < 0` menjadi `distance <= 0` di tiga tempat: `mapper.py`, `parse_line`, dan `df.where`.

**Format `:.2f` di mapper membuang presisi?**

- **Jawab**: Untuk data ini tidak. Semua nilai `trip_distance` dan `total_amount` di 1m paling banyak 2 desimal, jadi tidak ada nilai yang berubah. Itu sifat data ini; secara umum, membulatkan di mapper sebaiknya dihindari.

**Ada tanggal di luar Januari 2024?**

- **Jawab**: Ada beberapa (misalnya 2002, 2009, 31 Desember 2023). Tidak kami buang karena tugas ini membandingkan engine, bukan membersihkan data. Baris itu tetap masuk ke agregasi per jam berdasarkan jam di timestamp-nya.

### 8.3 Kode MapReduce

**Bagaimana Hadoop Streaming tahu mana key dan mana value?**

- **Jawab**: Default-nya, teks sebelum tab pertama adalah key, sisanya value. Key diurutkan sebagai byte. Karena itu mapper mencetak `ZONE|132<TAB>1,80.35,15.76`.

**Kenapa reducer diam saja (`continue`) kalau ada baris rusak? Itu menyembunyikan error.**

- **Jawab**: Benar, itu kelemahan. Kalau ada baris yang terbuang di reducer, kita tidak akan tahu dari output. Untuk data ini tidak ada yang terbuang: total trip semua key sama dengan jumlah baris valid yang dihitung `awk` (lihat 8.6). Di sistem produksi, baris rusak sebaiknya dihitung dengan Hadoop counter (Streaming mendukung baris `reporter:counter:...` di stderr) supaya terlihat di log.

**Apakah combiner benar-benar dijalankan?**

- **Jawab**: Ya. Di log varian combiner, `Combine input records` sama dengan `Map output records` (100k: 196804), dan `Combine output records=449`. Artinya semua output map melewati combiner.

**Kenapa combiner tidak membuat Hadoop jauh lebih cepat?**

- **Jawab**: Combiner mengurangi data shuffle dari jutaan baris menjadi ratusan, tetapi menambah satu proses Python di setiap spill. Pada data kecil, waktu Hadoop didominasi biaya tetap YARN dan JVM (puluhan detik), jadi penghematan shuffle hanya sebagian kecil dari total. Angkanya ada di bagian 7.

**Kenapa tidak menulis rata-rata langsung di mapper atau combiner?**

- **Jawab**: Rata-rata tidak bisa digabung tanpa tahu jumlah datanya. Rata-rata dari [10, 20, 30] adalah 20; kalau dipecah menjadi rata-rata([10]) = 10 dan rata-rata([20, 30]) = 25, lalu dirata-rata lagi, hasilnya 17,5, salah. Yang dibawa sampai reducer adalah count dan total, lalu rata-rata dihitung sekali di akhir.

### 8.4 Kode Spark

**`explode` menggandakan jumlah baris. Bukankah itu boros?**

- **Jawab**: Barisnya memang menjadi dua kali, sama seperti mapper yang mencetak dua baris. Tetapi penggandaan itu terjadi di memori dalam satu tahap, lalu langsung dijumlah sebagian di tiap partisi sebelum shuffle. Yang di-shuffle hanya hasil jumlahan per key per partisi.
- **Tunjukkan**: panggil `result.explain()` setelah `collect()`. Hasil untuk 1m, dari bawah ke atas: `FileScan csv`, `Filter`, `Generate explode(...)`, `HashAggregate(... partial_count(1), partial_sum(...))`, `Exchange hashpartitioning(key, 200)`, `AQEShuffleRead coalesced`, `HashAggregate(... count(1), sum(...))`. Agregasi parsial terjadi sebelum `Exchange`.

**Default `spark.sql.shuffle.partitions` itu 200. Untuk 271 key, bukankah itu terlalu banyak?**

- **Jawab**: Benar, defaultnya 200. Spark 3.2 ke atas menyalakan Adaptive Query Execution (AQE) secara default, yang menggabungkan partisi shuffle yang kecil-kecil setelah tahap map selesai. Di pengujian kami, tahap reduce digabung menjadi **1 task** untuk ketiga dataset. Kami tidak mengubah setting ini.
- **Tunjukkan**: `explain()` setelah `collect()` diawali `AdaptiveSparkPlan isFinalPlan=true` dan berisi `AQEShuffleRead coalesced`. Di Spark UI (http://localhost:4040 selama job berjalan), stage reduce tercatat 1 task.

**Kenapa `collect()`? Bukankah itu membawa semua data ke driver?**

- **Jawab**: `collect()` hanya membawa hasil akhir, 271 baris untuk 1m. Data mentahnya tidak dibawa. Kalau hasilnya jutaan baris, kami akan memakai `write.csv(...)`.

**`spark.driver.memory=4g`, lalu memori executor di mana?**

- **Jawab**: Di mode `local`, executor berjalan di dalam JVM driver yang sama, jadi `spark.executor.memory` tidak berpengaruh. Yang menentukan adalah memori driver. Angka 4g adalah batas maksimum heap, bukan memori yang benar-benar terpakai; puncak memori yang terukur di bagian 7 jauh di bawah 4 GB.

**Kenapa ada `minPartitions` di mode RDD, tapi tidak di DataFrame?**

- **Jawab**: `textFile` tanpa argumen membuat paling sedikit `min(defaultParallelism, 2)` = 2 partisi, jadi hanya 2 dari 4 core yang bekerja (diukur: 2 partisi tanpa `minPartitions`, 4 dengan `minPartitions`, untuk ketiga dataset). DataFrame membagi file berdasarkan ukuran dan jumlah core, dengan ukuran potongan minimal 4 MB (`spark.sql.files.openCostInBytes`). Hasilnya 4 partisi untuk 500k dan 1m, tetapi **hanya 2 partisi untuk 100k** karena filenya cuma 4,5 MB. Jadi di 100k DataFrame juga hanya memakai 2 core. Kami tidak mengubahnya; dengan data sekecil itu efeknya tidak berarti dibanding startup.

**Bukankah DataFrame seharusnya lebih cepat dari RDD? Di hasil kalian justru lebih lambat.**

- **Jawab**: Di data kami, DF memang 2-4 detik lebih lambat di ketiga ukuran. Yang kami ukur: startup keduanya mirip, selisihnya di pemrosesan; plan DF juga menjalankan dua job (stage map dijalankan AQE sebagai job tersendiri). Dugaan kami, belum diprofil: biaya perencanaan Catalyst dan kompilasi codegen di setiap run belum terbayar oleh data sekecil ini. Keunggulan DF di PySpark biasanya muncul di data besar, karena RDD mengirim setiap baris dari JVM ke proses Python dan kembali. Itu tidak kami uji.
- **Tunjukkan**: bagian 7.3 poin 6.
- **Hindari**: "DataFrame selalu lebih cepat dari RDD" atau sebaliknya.

**Kenapa CPU time Spark jauh lebih besar dari waktu berjalannya?**

- **Jawab**: CPU time adalah jumlah waktu CPU dari semua thread dan proses. Spark menjalankan 4 thread task bersamaan, ditambah thread JVM untuk kompilasi JIT dan garbage collector, dan DataFrame juga mengompilasi kode (whole-stage codegen) saat runtime. Jadi CPU time bisa beberapa kali lebih besar dari waktu berjalan. Pembagian persisnya tidak kami profil.

**Buktikan Spark itu lazy.**

- **Jawab**: Di `run_dataframe`, baris 36-61 berjalan hampir seketika; waktu proses yang sebenarnya terjadi di `collect()` baris 62. Bisa dibuktikan dengan mencetak waktu sebelum dan sesudah `collect()`, atau melihat Spark UI: job baru muncul setelah `collect()` dipanggil.

### 8.5 Benchmark dan pengukuran

**Waktu mana yang kalian pakai untuk kesimpulan, `wall_sec` atau `job_sec`?**

- **Jawab**: Keduanya dilaporkan karena mengukur hal berbeda. `job_sec` adalah waktu engine bekerja, termasuk startup engine (YARN dan JVM task untuk Hadoop, SparkSession untuk Spark), tetapi tanpa upload input. `wall_sec` adalah yang dirasakan pengguna dari awal sampai akhir. Untuk membandingkan engine, kami memakai `job_sec`; `wall_sec` menunjukkan biaya tambahan di luar engine (upload HDFS, `docker exec`).

**Memori Hadoop dan Spark diukur dengan cara berbeda. Bagaimana bisa dibandingkan?**

- **Jawab**: Tidak bisa dibandingkan secara mentah, dan kami tidak melakukannya. Counter Hadoop hanya menghitung map task dan reduce task, tidak termasuk ApplicationMaster dan daemon. psutil untuk Spark menghitung seluruh JVM dan proses Python. Yang kami bandingkan adalah pola: bagaimana angka tiap engine berubah dari 100k ke 1m.

**Kenapa memori MapReduce lokal naik dari 22 MB ke 290 MB?**

- **Jawab**: Karena `sort`. GNU `sort` menahan seluruh data antara di memori sebelum menulis hasil terurut. Untuk 1m, data antara 40 MiB dan `sort` mencapai maxRSS 284 MiB, sedangkan mapper dan reducer hanya sekitar 9 MB, karena keduanya membaca baris per baris.
- **Tunjukkan**: `/usr/bin/time -f "%M KB" sort -t "$(printf '\t')" -k1,1 mapped.tsv > /dev/null`.

**Kenapa Spark di 1m lebih cepat daripada di 100k?**

- **Jawab**: Selisihnya sekitar 1,5 detik, lebih kecil dari variasi antar waktu pengujian. Run 100k dijalankan lebih dulu dibanding run 1m, di saat beban aplikasi lain di laptop bisa berbeda. Kami tidak menafsirkan selisih sekecil itu. Kesimpulan yang bisa diambil hanya bahwa biaya pengolahan Spark di ukuran ini lebih kecil dari biaya startup-nya.

**Saat Spark dibenchmark, apakah cluster Hadoop masih menyala?**

- **Jawab**: Ya. Container Hadoop menyala selama seluruh benchmark, memakai RAM dan sedikit CPU walaupun tidak ada job. Kondisi ini sama untuk semua run Spark dan MapReduce lokal, jadi tidak membuat satu engine lebih diuntungkan dari yang lain, tetapi angkanya bisa sedikit lebih lambat dibanding mesin yang kosong.

**Kenapa tidak mengukur I/O disk?**

- **Jawab**: Tidak kami ukur secara langsung. Sebagai gantinya kami memakai counter Hadoop `Spilled Records` dan `Reduce shuffle bytes`, yang menunjukkan berapa data antara yang ditulis dan dipindahkan. Untuk Spark, angka yang setara bisa dilihat di Spark UI (Shuffle Write), tetapi tidak kami catat di `benchmark.csv`.

**Tiga ulangan cukup?**

- **Jawab**: Untuk melihat perbedaan besar (misalnya 40 detik lawan 15 detik), cukup. Untuk perbedaan kecil (di bawah sekitar 1 detik), tidak cukup, dan kami tidak menarik kesimpulan dari selisih sekecil itu. Semua angka per ulangan ada di `benchmark.csv`.

### 8.6 Kebenaran hasil

**`verify_results.py` hanya membandingkan engine satu sama lain. Kalau semuanya salah dengan cara yang sama?**

- **Jawab**: Itu sebabnya kami juga mengecek dengan cara yang tidak memakai engine mana pun. `awk` menghitung langsung dari CSV: 986.719 trip valid dengan total 27.823.629,78 dolar untuk 1m. Jumlah semua key `ZONE|` di hasil Hadoop sama persis, dan jumlah semua key `HOUR|` juga sama persis.
- **Tunjukkan**:
  ```bash
  awk -F, 'NR>1 && $6>0 && $4>=0 {n++; s+=$6} END{printf "%d trip, total %.2f\n", n, s}' data/taxi_1m.csv
  awk -F'\t' '{split($1,k,"|"); c[k[1]]+=$2; t[k[1]]+=$3} END{for(x in c) printf "%s: %d trip, total %.2f\n", x, c[x], t[x]}' output/hadoop/hadoop_result_taxi_1m.tsv
  ```

**Kenapa memakai float untuk uang, bukan Decimal atau sen dalam bilangan bulat?**

- **Jawab**: Kami memakai tipe bawaan tiap engine (float Python, `DoubleType` Spark) supaya kode tetap sederhana dan sama di semua engine. Akibatnya digit terakhir hasil pembulatan kadang beda 0,01 antar engine. Kalau hasilnya harus identik sampai sen, nilai sebaiknya diubah ke sen dalam bilangan bulat (misalnya `round(amount * 100)`) di mapper dan di Spark. Ini tidak kami lakukan.

**Kenapa toleransinya 0,011, bukan 0,01?**

- **Jawab**: Karena `22.93 - 22.92` di komputer menghasilkan `0.010000000000001563`, sedikit di atas 0,01. Dengan batas 0,01, selisih pembulatan yang normal akan dianggap salah.


---

## 9. Pembagian penjelasan untuk 5 anggota

Penilaian dilakukan dengan membaca dan menjelaskan kode, jadi setiap anggota memegang satu bagian kode dan harus bisa menjawab pertanyaan di bagian 8 untuk topiknya, termasuk alasan pilihannya di bagian 5.

| Anggota | Bagian | File | Bahan baca |
|---|---|---|---|
| Ketua (penjelas utama) | Gambaran besar, alur data, mapper, reducer, combiner, perbandingan MapReduce lawan Spark, kesimpulan benchmark | `mapper.py`, `reducer.py`, `combiner.py`, `spark_job.py` | Bagian 1, 2, 4.2-4.4, 4.7, 5.4, 5.6, 5.7, 5.11, 6, 7, 8.3, 8.4 |
| Anggota 2 | Dataset dan preprocessing | `prepare_dataset.py` | Bagian 1, 4.1, 5.1-5.3, 8.2 |
| Anggota 3 | Simulasi MapReduce lokal dan laporan CLI | `run_mapreduce.py` | Bagian 4.5, 5.8, 5.12, 5.15, 8.3 (pertanyaan reducer) |
| Anggota 4 | Cluster Hadoop: Docker, HDFS, YARN, Hadoop Streaming | folder `infra/` | Bagian 4.6, 5.5, 5.10, 6.1, 6.2, 8.1 |
| Anggota 5 | Verifikasi hasil dan benchmark | `verify_results.py`, `run_benchmark.py` | Bagian 4.8, 4.9, 5.13, 5.14, 6.5, 7, 8.5, 8.6 |

Latihan untuk setiap anggota: jalankan sendiri perintah di bagian 3 untuk file yang dipegang, lalu jelaskan output-nya dengan kata-kata sendiri tanpa melihat dokumen ini. Setelah itu, minta anggota lain bertanya "kenapa tidak pakai X?" untuk setiap pilihan di bagian 5 yang terkait.
