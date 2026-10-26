# Script Presentasi Kelompok 10

Urutan bicara mengikuti alur kode: data masuk, logika MapReduce, eksekusi lokal, eksekusi klaster, Spark, verifikasi, penutup. Teks dalam kutipan adalah yang diucapkan. Teks dalam kurung siku `[...]` adalah penanda untuk layar dan tidak dibacakan. Baris yang diawali **[ADIT]** adalah aksi di layar: Adit yang membagikan layar dan menjalankan kode.

Setiap angka di bawah berasal dari `README.md`, `docs/RINGKASAN_PRESENTASI.md`, log di `output/`, atau kode di repo. Penjelasan sebab yang belum diukur ditandai **[dugaan]** dan harus diucapkan sebagai dugaan.

Demo memakai data 100k, mengikuti `docs/PANDUAN.md` bagian 3.1. Total waktu sekitar 18 sampai 22 menit, termasuk demo.

| No | Pembicara | Bagian | File di layar | Demo |
|---|---|---|---|---|
| 1 | Ale | Pembuka | `README.md` | - |
| 2 | Ale | Dataset dan preprocessing | `preprocessing/prepare_dataset.py` | - |
| 3 | Adit | Mapper, combiner, reducer | `mapreduce/mapper.py`, `combiner.py`, `reducer.py` | - |
| 4 | Akram | Runner MapReduce lokal | `mapreduce/run_mapreduce.py` | `run_mapreduce.py` |
| 5 | Yoga | Klaster Hadoop dan Streaming | `infra/docker-compose.yml`, `infra/run_hadoop_job.sh` | Hadoop, lalu Hadoop + combiner |
| 6 | Adit | Spark DataFrame dan RDD | `spark/spark_job.py` | `--mode df`, lalu `--mode rdd` |
| 7 | Ferdinan | Verifikasi dan benchmark | `benchmark/verify_results.py`, `benchmark/results/benchmark.csv`, tabel di `README.md` | `verify_results.py` |
| 8 | Adit | Kesimpulan dan keterbatasan | `README.md` | - |

---

## 0. Sebelum dosen masuk

**Adit.**

```bash
source .venv/bin/activate
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps    # tunggu 4 container berstatus running
```

Siapkan:

- Editor dengan tab terbuka: `README.md`, `preprocessing/prepare_dataset.py`, `mapreduce/mapper.py`, `combiner.py`, `reducer.py`, `run_mapreduce.py`, `infra/docker-compose.yml`, `infra/run_hadoop_job.sh`, `spark/spark_job.py`, `benchmark/verify_results.py`.
- Satu terminal di root repo dengan `.venv` aktif dan font diperbesar.
- Notifikasi dimatikan.

---

## 1. Pembuka

**Ale.** **[ADIT]** Tampilkan `README.md`, bagian atas.

> "Selamat pagi, Bapak Dosen dan rekan-rekan. Kami Kelompok 10. Kami membandingkan Hadoop MapReduce dengan Apache Spark pada satu pekerjaan yang sama.
>
> Pekerjaannya: dari data NYC Yellow Taxi Januari 2024, hitung jumlah trip, total tarif, rata-rata tarif, dan rata-rata jarak, per zona penjemputan dan per jam. Kami menjalankannya di empat ukuran data, dari 100 ribu baris sampai 2,96 juta baris.
>
> Ada lima engine: MapReduce lokal, Hadoop YARN, Hadoop YARN dengan combiner, Spark DataFrame, dan Spark RDD. Semuanya dijalankan di satu laptop. Urutannya: data, logika MapReduce, tiap engine, lalu bukti bahwa hasilnya sama, dan terakhir kesimpulan. Demo hari ini memakai data 100 ribu baris supaya selesai dalam beberapa menit. Hasil ukuran lain sudah kami simpan dan akan ditunjukkan di bagian benchmark."

---

## 2. Dataset dan preprocessing

**Ale.** **[ADIT]** Buka `preprocessing/prepare_dataset.py`. Sorot baris 14–21, lalu 24–29, lalu 63.

> "Sumber datanya file Parquet resmi dari NYC TLC: 2.964.624 baris dan 19 kolom. Kami hanya butuh enam kolom. [baris 14–21] Kolomnya: waktu jemput, ID zona jemput, jumlah penumpang, jarak, jenis pembayaran, dan total tarif. PyArrow membaca enam kolom itu saja, jadi kolom lain tidak dimuat.
>
> Dari tabel itu kami membuat empat file CSV: 100 ribu, 500 ribu, 1 juta, dan seluruh baris. [baris 24–29] File terbesar berukuran 132,8 MB.
>
> Kami memakai CSV karena Hadoop Streaming membaca teks per baris lewat `stdin`. Mapper Python kami tidak perlu pustaka Parquet.
>
> Subset diambil dengan `table.slice` dari baris pertama. [baris 63] Ini bukan sampel acak. Alasannya determinisme: setiap anggota yang menjalankan skrip mendapat file yang sama, dan file yang lebih besar memuat file yang lebih kecil. Kami sudah memeriksanya: 1.000.001 baris pertama `taxi_full.csv` identik byte per byte dengan `taxi_1m.csv`. Jadi yang berubah antar-ukuran hanya jumlah data. Karena bukan sampel acak, hasil bisnis seperti zona teramai hanya kami ambil dari data penuh.
>
> Selanjutnya Mas Adit menjelaskan logika MapReduce-nya."

---

## 3. Mapper, combiner, reducer

**Adit.** **[ADIT]** Buka `mapper.py` (sorot 25–31, lalu 38–39), lalu `combiner.py` (31–33), lalu `reducer.py` (31–35).

> "Mapper membaca satu baris CSV dan membuang baris yang tidak layak: tarif nol atau negatif, jarak negatif, zona kosong, waktu terpotong, atau kolom yang tidak bisa dibaca sebagai angka. [mapper baris 25–31]
>
> Untuk setiap baris yang lolos, mapper mencetak dua pasangan key-value. [baris 38–39] Satu dengan key `ZONE|` diikuti ID zona, satu lagi dengan key `HOUR|` diikuti jam. Isinya sama: satu trip, tarif, dan jarak. Prefix itu penting: zona 10 dan jam 10 akan bentrok kalau tidak dibedakan. Dengan dua key per baris, dua agregasi selesai dalam satu job dan satu kali baca data.
>
> Combiner berjalan di sisi map sebelum shuffle. [combiner baris 31–33] Isinya hanya penjumlahan: jumlah trip, total tarif, total jarak. Combiner sengaja tidak menghitung rata-rata, karena rata-rata dari rata-rata menghasilkan angka yang salah saat jumlah trip per kelompok berbeda. Ada alasan kedua: Hadoop boleh menjalankan combiner nol, satu, atau beberapa kali. Karena itu format keluaran combiner kami samakan dengan mapper, supaya hasilnya benar berapa kali pun combiner dipanggil.
>
> Reducer menerima input yang sudah diurutkan per key oleh framework. [reducer baris 31–35] Ia menyimpan akumulator untuk satu key saja, dan menghitung rata-rata ketika key berganti. Memori yang dipakai tetap, tidak bergantung pada jumlah baris.
>
> Selanjutnya Mas Akram menunjukkan bagaimana ketiga file ini dijalankan di lokal."

---

## 4. Runner MapReduce lokal

**Akram.** **[ADIT]** Buka `mapreduce/run_mapreduce.py`. Sorot baris 56–79, lalu 72.

> "Sebelum ke klaster, kami menjalankan mapper dan reducer yang sama di mesin lokal lewat `run_mapreduce.py`. Hasilnya menjadi pembanding untuk semua engine lain.
>
> Konsepnya `mapper | sort | reducer`, tetapi kodenya tidak memakai pipe. [baris 56–79] Ia menjalankan tiga proses terpisah dan menyambungkannya lewat file sementara: mapper menulis ke file, `sort` membaca file itu dan menulis file terurut, lalu reducer membaca file terurut. Kami memilih cara ini agar tiap fase bisa diukur terpisah dan meniru sifat MapReduce yang menulis hasil antara ke disk.
>
> Perintah `sort` dijalankan dengan `LC_ALL=C`. [baris 72] Ini memaksa pengurutan berdasarkan nilai byte, sehingga urutan key sama dengan urutan yang dipakai Hadoop untuk key teks, dan tidak bergantung pada locale komputer. Reducer bergantung pada urutan ini: kalau key yang sama tidak berdekatan, hasilnya salah.
>
> Kita jalankan sekarang."

**[ADIT]** Terminal (selesai dalam waktu di bawah satu detik):

```bash
python mapreduce/run_mapreduce.py data/taxi_100k.csv
```

> "Selesai dalam waktu kurang dari satu detik. Yang tercetak adalah ringkasan dari fungsi `format_cli_report`: zona dengan trip terbanyak, dan jumlah trip per jam dalam bentuk batang `#`. Fungsi yang sama dipakai oleh Hadoop dan Spark, jadi bentuk laporannya seragam. Hasil lengkapnya tersimpan di `output/mapreduce/mr_result_taxi_100k.tsv`.
>
> Selanjutnya Mas Yoga menjalankan kode yang sama di klaster Hadoop."

---

## 5. Klaster Hadoop dan Hadoop Streaming

**Yoga.** Job Hadoop butuh sekitar 25–45 detik. Supaya tidak ada jeda kosong, jalankan job dulu, lalu jelaskan kode sambil job berjalan.

**[ADIT]** Terminal:

```bash
docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv
```

Begitu job mulai, pindah ke editor dan buka `infra/docker-compose.yml`.

> "Job Hadoop sedang berjalan. Sambil menunggu, ini klasternya.
>
> Klaster Hadoop 3.4.1 kami jalankan dengan Docker Compose. Ada empat service: NameNode dan DataNode untuk HDFS, ResourceManager dan NodeManager untuk YARN. Jumlah DataNode satu, jadi replikasi blok satu. Artinya klaster ini tidak punya toleransi kegagalan, dan bagian itu tidak kami uji.
>
> Kapasitas NodeManager diatur 4 vcore dan 4096 MB di konfigurasi YARN. [`infra/config`, baris 10–11] Angka ini kapasitas yang dilaporkan ke YARN, bukan batas yang dipaksakan Docker."

**[ADIT]** Buka `infra/run_hadoop_job.sh`. Sorot baris 17–20, 24–28, lalu 34.

> "Skrip `run_hadoop_job.sh` menunggu HDFS keluar dari safe mode, lalu mengunggah CSV ke `/data`. [baris 17–20] Setelah itu ia memanggil jar `hadoop-streaming`. [baris 24–28] Opsi `-files` mengirim `mapper.py`, `combiner.py`, dan `reducer.py` ke container. Opsi `-combiner` hanya dipasang kalau kami meminta varian dengan combiner. Hasilnya diambil kembali dengan `getmerge` menjadi satu file TSV. [baris 34]"

**[ADIT]** Kembali ke terminal. Kalau job belum selesai, Yoga mengisi:

> "Waktu yang lama ini sebagian besar adalah biaya tetap: pengajuan job ke YARN, menyalakan ApplicationMaster, dan JVM container. Itu dugaan kami, belum kami urai per komponen. **[dugaan]** Datanya sendiri hanya 4,5 MB."

**[ADIT]** Setelah selesai, jalankan varian dengan combiner:

```bash
docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv combiner
```

> "Sekarang job yang sama dengan combiner. Kodenya sama persis, yang berbeda hanya argumen `combiner` di akhir. Log job kami simpan, karena berisi counter Hadoop seperti Spilled Records dan Reduce shuffle bytes. Counter itu yang kami pakai untuk melihat efek combiner."

**[ADIT]** Setelah selesai:

```bash
grep -E 'Map output records|Combine output records|Spilled Records|Reduce shuffle bytes' \
  output/hadoop/hadoop_log_taxi_100k.txt output/hadoop/hadoop_log_taxi_100k_combiner.txt
```

> "Ini perbandingannya di 100 ribu baris. Tanpa combiner, Reduce shuffle bytes sekitar 4,6 juta byte. Dengan combiner, tinggal sekitar 12 ribu byte. Spilled Records turun dari 393.608 menjadi 898. Data yang dipindahkan ke reducer turun hampir seluruhnya. Tetapi waktunya tidak turun di data sekecil ini; angka waktunya dibahas di bagian benchmark.
>
> Selanjutnya Mas Adit menjelaskan Spark."

Catatan untuk Yoga: 4.630.415 dan 12.443 byte, serta 393.608 dan 898, berasal dari log run sebelumnya. Kalau angka di layar sedikit berbeda setelah run ulang, bacakan angka yang ada di layar.

---

## 6. Spark DataFrame dan RDD

**Adit.** **[ADIT]** Buka `spark/spark_job.py`. Sorot baris 103–104, lalu 24, 35–62, 48, lalu 85–91, 87–88.

> "Spark kami jalankan dalam mode lokal dengan `local[4]` dan memori driver 4 GB. [baris 103–104] Angka ini kami samakan secara nominal dengan NodeManager: 4 vcore dan 4096 MB. Perlu diingat bahwa ini hanya kira-kira setara. Memori YARN adalah total untuk semua container, sedangkan 4 GB di Spark adalah heap driver, dan proses Python worker pada mode RDD berada di luar angka itu.
>
> Mode DataFrame [baris 35–62] memakai skema yang kami tulis sendiri, tanpa `inferSchema`, supaya Spark tidak membaca file dua kali. [baris 24] Dua key per baris dibuat dengan `explode` pada sebuah array. [baris 48]
>
> Mode RDD [baris 85–91] memakai `flatMap(parse_line)` lalu `reduceByKey`. `reduceByKey` menjumlahkan di tiap partisi sebelum shuffle, jadi perannya mirip combiner. Kami menetapkan `minPartitions` sama dengan jumlah core, [baris 87–88] karena default `textFile` hanya dua partisi dan dua core akan menganggur.
>
> Seluruh transformasi baru berjalan ketika `collect()` dipanggil di akhir. Kita jalankan."

**[ADIT]** Terminal (sekitar 6–16 detik):

```bash
python spark/spark_job.py data/taxi_100k.csv --mode df
```

> "Beberapa detik pertama adalah startup Spark dan JVM. Itu biaya tetap, bukan waktu memproses data."

**[ADIT]** Setelah selesai (sekitar 5–13 detik):

```bash
python spark/spark_job.py data/taxi_100k.csv --mode rdd
```

> "Ini versi RDD. Bentuk laporannya sama dengan MapReduce lokal dan Hadoop. Apakah angkanya benar-benar sama, itu yang dibuktikan Pak Ferdinan di bagian berikutnya."

---

## 7. Verifikasi dan benchmark

**Ferdinan.** **[ADIT]** Buka `benchmark/verify_results.py`, sorot baris 14. Lalu terminal:

```bash
python benchmark/verify_results.py taxi_100k
```

> "Kecepatan baru boleh dibandingkan setelah hasilnya terbukti sama. `verify_results.py` membandingkan keluaran Hadoop, Hadoop dengan combiner, Spark DataFrame, dan Spark RDD terhadap MapReduce lokal. Yang di layar adalah hasil untuk 100 ribu baris barusan. Satu baris adalah MapReduce lokal dibandingkan dengan dirinya sendiri, jadi yang bermakna empat.
>
> Untuk keempat ukuran data, totalnya 16 perbandingan, dan semuanya OK. Jumlah key yang dibandingkan: 250, 264, 271, dan 284 untuk data penuh, yaitu 260 zona dan 24 jam. Jumlah trip harus sama persis. Selisih total tarif dan jarak diperbolehkan sampai 0,011. [baris 14]
>
> Batas 0,011 ada alasannya: `22.93 - 22.92` di floating point bernilai `0.010000000000001563`, sedikit di atas 0,01. Dengan batas 0,01, selisih pembulatan satu sen yang wajar akan terhitung salah."

**[ADIT]** Buka `benchmark/results/benchmark.csv`, lalu tabel benchmark di `README.md`.

> "Benchmark: tiap kombinasi dijalankan tiga kali dan kami ambil median waktu job. Semua ukuran dijalankan ulang dalam satu sesi, supaya tidak ada selisih antar-sesi.
>
> Pada 100 ribu baris, MapReduce lokal selesai dalam 0,53 detik. Hadoop YARN butuh 17,34 detik, dan Spark sekitar 6 detik. Pada data sekecil ini yang terukur adalah biaya tetap, bukan pemrosesan. Kami menduga biaya tetap Hadoop berasal dari pengajuan job ke YARN dan penyalaan ApplicationMaster serta JVM container, tetapi kami belum mengurainya per komponen. **[dugaan]**
>
> Pada data penuh, 2,96 juta baris: Spark 7,30 sampai 7,43 detik. Hadoop tanpa combiner 33,40 detik, sekitar 4,5 sampai 4,6 kali lebih lambat dari Spark. Hadoop dengan combiner 24,51 detik, sekitar 3,3 kali lebih lambat. MapReduce lokal 10,91 detik. Semua ini berlaku di satu laptop dan dalam satu sesi benchmark.
>
> Efek combiner di data penuh: data shuffle turun dari 137,6 MB menjadi 32,5 KB, atau 99,98 persen. Waktu job turun dari 33,40 menjadi 24,51 detik, 27 persen. Di 100 ribu baris, combiner justru lebih lambat: 19,54 detik lawan 17,34 detik. Dugaan kami, data yang dihemat terlalu kecil untuk menutup biaya proses Python dan sort tambahan. **[dugaan]**
>
> Kesimpulannya disampaikan Mas Adit."

---

## 8. Kesimpulan dan keterbatasan

**Adit.** **[ADIT]** Tampilkan `README.md`.

> "Kesimpulan kami ada tiga.
>
> Pertama, kelima engine menghasilkan angka yang sama: 16 perbandingan, semuanya OK.
>
> Kedua, di mesin dan sesi benchmark ini, MapReduce lokal tercepat sampai 1 juta baris, dan Spark tercepat di data penuh. Titik silangnya ada di antara 1 juta dan 2,96 juta baris. Hadoop paling lambat di semua ukuran, dan sebagian besar selisihnya adalah biaya tetap.
>
> Ketiga, combiner memangkas data shuffle Hadoop hampir seluruhnya dan mempercepat job di data penuh, tetapi tidak membantu di data kecil.
>
> Beberapa batasan yang perlu kami sampaikan sendiri. Data terbesar 132,8 MB, masih di bawah satu blok HDFS 128 MiB, jadi ini bukan skala yang membuat Hadoop dan Spark diciptakan. Hadoop tetap membuat dua split karena nilai default `mapreduce.job.maps` adalah 2. Semua engine berjalan di satu laptop, HDFS hanya punya satu DataNode, dan alasan Spark lebih cepat di sini belum kami ukur: kami hanya mengamati bahwa ia lebih cepat. Subset diambil dari baris pertama, bukan acak.
>
> Terima kasih. Kami siap menerima pertanyaan."

---

## Pembagian penjawab saat tanya jawab

| Topik pertanyaan | Penjawab |
|---|---|
| Data, Parquet, subset tidak acak | Ale |
| Mapper, combiner, rata-rata dari rata-rata, reducer | Adit |
| `sort`, `LC_ALL=C`, file sementara | Akram |
| Docker, HDFS, YARN, counter, Spilled Records | Yoga |
| Spark, `explode`, AQE, `collect()`, lazy, DataFrame lawan RDD | Adit |
| Toleransi 0,011, 16 lawan 20, angka benchmark, memori | Ferdinan |
| Hal yang belum diukur | Adit: "Itu belum kami ukur. Dugaan kami ..., tetapi belum bisa kami buktikan." |

---

## Antisipasi pertanyaan

| Pertanyaan | Jawaban singkat | Rujukan |
|---|---|---|
| Kenapa 16, bukan 20? | `verify_results.py` mencetak 20 baris: 4 ukuran × 5 engine. Empat di antaranya MapReduce lokal dibandingkan dengan dirinya sendiri. Yang bermakna 16. | `RINGKASAN_PRESENTASI.md` poin 29 |
| Apakah Spark selalu lebih cepat? | Tidak. Sampai 1 juta baris MapReduce lokal lebih cepat (4,15 detik lawan 5,25 sampai 6,37 detik). Spark unggul di data penuh. | `README.md` tabel benchmark |
| Kenapa Hadoop 17 detik di data 4,5 MB? | Biaya tetap YARN, dugaan kami. Belum diuraikan per komponen. | `RINGKASAN_PRESENTASI.md` bagian 4 |
| Kenapa Spilled Records 3× output map di data penuh? | Buffer sort 100 MB terlampaui, sehingga tiap map menulis dua file spill lalu menggabungkannya. 17.572.224 = 3 × 5.857.408. | `PANDUAN.md` 6.1, `RINGKASAN_PRESENTASI.md` poin 26 |
| Apakah memori Hadoop dan Spark bisa dibandingkan? | Tidak. Hadoop memakai counter "Physical memory snapshot", engine lokal memakai RSS puncak dari psutil. Satuannya MiB walau kolom bertuliskan MB. | `RINGKASAN_PRESENTASI.md` poin 27 |
| Apakah resource Hadoop dan Spark benar-benar sama? | Hanya setara secara nominal. YARN 4096 MB untuk semua container, Spark 4 GB heap driver saja. | `PANDUAN.md` 5.10 |
| Kenapa pakai float untuk uang? | Supaya kode sama di semua engine. Akibatnya digit terakhir kadang beda 0,01. Cara yang lebih ketat adalah menyimpan sen sebagai bilangan bulat, dan itu tidak kami lakukan. | `PANDUAN.md` 8.6 |
| Kenapa subset bukan acak? | Untuk uji skala yang deterministik. Hasil bisnis hanya dari data penuh. | `RINGKASAN_PRESENTASI.md` poin 28 |
| Apakah combiner benar-benar dijalankan? | Ya. Di log 100k, `Combine input records` sama dengan `Map output records` (196804), dan `Combine output records=449`. | `PANDUAN.md` 8.3 |
| Bukankah DataFrame seharusnya lebih cepat dari RDD? | Di data kami tidak ada pemenang yang terbukti. Selisihnya 0,1 sampai 1,7 detik, sebesar variasi antar ulangan. Sebabnya belum diprofil. | `PANDUAN.md` 8.4 |
| Kenapa demo hanya 100k? | Kode dan alurnya sama untuk semua ukuran, yang berbeda hanya nama file. Benchmark penuh butuh sekitar 15 sampai 40 menit, jadi hasilnya ditunjukkan dari file. | `PANDUAN.md` 3.1 |

---

## Kalau demo gagal

Jangan memperbaiki di depan dosen. Katakan bahwa hasil run sebelumnya sudah tersimpan, lalu buka file cadangan.

| Demo | File cadangan |
|---|---|
| MapReduce lokal | `output/mapreduce/mr_result_taxi_100k.tsv` |
| Hadoop | `output/hadoop/hadoop_log_taxi_100k.txt`, `output/hadoop/hadoop_result_taxi_100k.tsv` |
| Hadoop + combiner | `output/hadoop/hadoop_log_taxi_100k_combiner.txt` |
| Spark | `output/spark/spark_df_result_taxi_100k.tsv` |
| Benchmark | `benchmark/results/benchmark.csv` |

Selesai presentasi: `docker compose -f infra/docker-compose.yml down`.

---

## Perubahan dari draf awal

Draf yang ditempel sebelumnya diperiksa terhadap kode. Perbedaannya:

| Draf awal | Di sini | Alasan |
|---|---|---|
| "3,3 hingga 4,6 kali lebih cepat dibanding Hadoop (33,40 detik)" | 4,5 sampai 4,6× terhadap tanpa combiner, 3,3× terhadap dengan combiner | 3,3× berasal dari 24,51 detik, bukan 33,40 detik |
| Resource "disamakan persis" | "Setara secara nominal", dengan penjelasan perbedaannya | `PANDUAN.md` 5.10 menyebut "kira-kira setara" |
| "Parquet kolom tunggal" | "19 kolom, enam dipakai" | Parquet berformat kolom, filenya punya 19 kolom |
| "UNIX standard pipe" | Tiga proses lewat file sementara | `run_mapreduce.py` baris 56–79 tidak memakai pipe |
| `LC_ALL=C` "jauh lebih cepat" | Hanya alasan urutan byte | Kecepatan tidak diukur |
| Overhead ApplicationMaster dan JVM sebagai fakta | Ditandai dugaan | Belum diuraikan per komponen |
| "Spark unggul berkat in-memory dan Catalyst" | "Di mesin dan sesi ini Spark lebih cepat", sebabnya belum diukur | Tidak ada pengukuran penyebab |
| "Explode vectorized di Catalyst" | Dihapus | Tidak ada bukti di repo |
| Titik silang "di atas 1 juta" | "Antara 1 juta dan 2,96 juta" | Di 1 juta MapReduce lokal masih lebih cepat |
| Tanpa keterbatasan | Ditambahkan di bagian 8 | Satu mesin, satu DataNode, data kecil, subset bukan acak |
| `README.md` menulis 20/20 | Script dan `README.md` memakai 16 perbandingan | 4 dari 20 baris adalah perbandingan dengan diri sendiri |
