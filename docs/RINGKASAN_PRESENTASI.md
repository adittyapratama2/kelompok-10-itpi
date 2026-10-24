# Ringkasan Presentasi Kelompok 10 (pegangan ketua)

Catatan persiapan pribadi, belum di-commit. Penjelasan lengkap ada di `docs/PANDUAN.md`
(bagian 4 kode per baris, 5 alasan, 6 MR vs Spark, 6.5 keterbatasan, 7 hasil, 8 bank pertanyaan, 9 pembagian anggota).

Label yang dipakai di bawah:
- **[diukur]** ada di `benchmark/results/benchmark.csv`, log Hadoop, atau output `verify_results.py`.
- **[dugaan]** penjelasan yang masuk akal tetapi tidak diurai dengan pengukuran. Ucapkan dengan "kemungkinan besar".
- **[tidak diuji]** konsep yang benar secara teori, tetapi tidak dicoba di proyek ini.

---

## 1. Inti dalam satu menit

Satu agregasi yang sama dijalankan di lima engine pada empat ukuran data NYC Yellow Taxi Januari 2024.
Agregasinya: per zona penjemputan (`ZONE|`) dan per jam (`HOUR|`), hitung jumlah trip, total tarif,
rata-rata tarif, dan rata-rata jarak.

Lima engine: MapReduce lokal (mapper | sort | reducer), Hadoop YARN, Hadoop YARN + combiner,
Spark DataFrame, Spark RDD.

Dua bukti yang ditunjukkan:
1. **Benar**: `verify_results.py` membandingkan keempat engine lain dengan MapReduce lokal. Semua OK di keempat ukuran. [diukur]
2. **Cepat atau lambat, dan kenapa**: `run_benchmark.py`, 3 ulangan per kombinasi, diambil median. [diukur]

Empat kesimpulan yang boleh diucapkan:
1. Kelima engine menghasilkan angka yang sama. Count identik, total dan rata-rata berbeda paling banyak 0,01 (satu sen atau 0,01 mil).
2. Di data kecil, biaya startup mendominasi. MR lokal paling cepat (0,53 s), Hadoop paling lambat (17,34 s).
3. Di data penuh (2,96 juta baris, 133 MB), Spark 3,3–4,6× lebih cepat dari Hadoop. Ini berlaku di mesin dan sesi benchmark ini saja.
4. Combiner memangkas shuffle Hadoop 99,98% dan waktu job 27% di data penuh, tetapi memperlambat di 100k.

---

## 2. Angka yang wajib hafal

### Dataset [diukur]

| Subset | Baris data | Ukuran | Trip valid | Dibuang filter | Key hasil |
|---|---|---|---|---|---|
| taxi_100k | 100.000 | 4,5 MB | 98.402 | 1.598 | 250 |
| taxi_500k | 500.000 | 22,4 MB | 492.939 | 7.061 | 264 |
| taxi_1m | 1.000.000 | 44,9 MB | 986.719 | 13.281 | 271 |
| taxi_full | 2.964.624 | 132,8 MB | 2.928.704 | 35.920 (1,2%) | 284 = 260 zona + 24 jam |

- "Trip valid" = jumlah count semua key `HOUR|`. Tiap trip valid muncul tepat satu kali di satu jam.
- Hadoop log full: `Map input records=2964625` (2.964.624 baris + 1 header), `Map output records=5857408` (2 × 2.928.704).
- Top zona penuh menurut total tarif: zona 132 ($11.338.772,72), zona 138, zona 161. Jam tersibuk: 18.00 (210.308 trip).
  Nama zona (132 = JFK Airport, 138 = LaGuardia) berasal dari tabel zona TLC, yang tidak ada di repo. Sebut sebagai ID saja kecuali yakin.

### Waktu job, median 3 ulangan, detik [diukur]

| Engine | 100k | 500k | 1m | full |
|---|---|---|---|---|
| MR lokal | 0,53 | 2,19 | 4,15 | 10,91 |
| Hadoop YARN | 17,34 | 19,40 | 23,73 | 33,40 |
| Hadoop + combiner | 19,54 | 19,58 | 20,82 | 24,51 |
| Spark DataFrame | 6,25 | 7,13 | 6,37 | 7,43 |
| Spark RDD | 6,14 | 5,45 | 5,25 | 7,30 |

### Data penuh, kolom lain [diukur]

| Engine | wall_sec | cpu_sec | peak_mem (MiB) |
|---|---|---|---|
| MR lokal | 11,00 | 14,51 | 836,4 |
| Hadoop YARN | 41,37 | 19,54 | 1347,1 |
| Hadoop + combiner | 33,47 | 10,47 | 898,8 |
| Spark DataFrame | 8,33 | 23,38 | 1218,7 |
| Spark RDD | 8,76 | 25,48 | 844,5 |

CPU dan memori Hadoop diambil dari Hadoop counters. CPU dan memori engine lokal diambil dari psutil/rusage.
**Dua cara ukur ini tidak boleh dibandingkan langsung.**

### Shuffle Hadoop [diukur]

| Ukuran | Shuffle bytes tanpa combiner | Dengan combiner | Spilled tanpa | Spilled dengan |
|---|---|---|---|---|
| 100k | 4.630.415 | 12.443 | 393.608 | 898 |
| 500k | 23.176.901 | 15.062 | 1.971.756 | 1.036 |
| 1m | 46.392.925 | 15.887 | 3.946.876 | 1.062 |
| full | 137.602.572 | 32.506 | 17.572.224 | 3.231 |

---

## 3. Per file: apa, baris kunci, alasan

### `preprocessing/prepare_dataset.py`
- **Apa**: mengunduh Parquet resmi NYC TLC, mengambil 6 kolom, lalu menulis 4 CSV. Subsetnya `table.slice(0, n)` (baris 63), jadi berisi n baris pertama, bukan sampel acak.
- **Alasan CSV**: Hadoop Streaming dan mapper membaca teks per baris. Parquet tidak bisa dibaca per baris oleh `sys.stdin`.
- **Alasan 6 kolom**: CSV lebih kecil. `passenger_count` dan `payment_type` ikut diambil tetapi **tidak dipakai** agregasi. Akui jujur bila ditanya.
- **Satuan**: ukuran dicetak dalam MiB (dibagi 1024²). Tabel di atas memakai MB (dibagi 10⁶).

### `mapreduce/mapper.py`
- Baris 10: lewati header.
- Baris 14: lewati baris dengan kolom kurang.
- Baris 17–23: lewati nilai yang gagal diubah ke angka.
- Baris 26: filter tarif ≤ 0 atau jarak negatif. Transaksi seperti ini adalah refund atau data rusak.
- Baris 30: buang zona kosong atau waktu terpotong.
- Baris 34: jam diambil dengan `pickup_dt[11:13]`, yaitu potongan string. Tidak perlu parse datetime, jadi lebih murah.
- Baris 38–39: setiap trip menghasilkan **dua** pasangan key-value.
- **Alasan prefix `ZONE|`/`HOUR|`**: satu job menghasilkan dua agregasi sekaligus. Tanpa prefix, zona 10 dan jam 10 akan jatuh ke key yang sama. Alternatifnya dua job terpisah, tetapi itu berarti data dibaca dua kali.
- **Alasan value `1,amount,distance`**: count ikut dikirim agar combiner dan reducer cukup menjumlah. Mereka tidak perlu tahu apakah input datang dari mapper atau dari combiner.

### `mapreduce/combiner.py`
- Format input sama dengan format output (`KEY \t count,amount,distance`). Alasannya: Hadoop boleh menjalankan combiner nol, satu, atau beberapa kali.
- **Tidak menghitung rata-rata**, karena rata-rata dari rata-rata salah. Contoh: (10 + 20)/2 tidak sama dengan rata-rata 1 trip $10 dan 3 trip $20.

### `mapreduce/reducer.py`
- Inputnya sudah terurut, jadi key yang sama selalu berdampingan.
- Reducer hanya menyimpan satu key di memori. Begitu key berganti (baris 25), hasil key lama dicetak (baris 30–35).
- Baris 41–46: flush key terakhir.
- Rata-rata dihitung di sini sebagai total/count. Hanya reducer yang melihat total akhir.

### `mapreduce/run_mapreduce.py`
- Menjalankan tiga tahap berurutan: map ke file, `sort` ke file, reduce ke output.
- `sort` meniru tahap shuffle & sort Hadoop.
- `LC_ALL=C` (baris 72): urutan per byte. Sama dengan urutan key Hadoop dan lebih cepat dari urutan yang mengikuti locale.
- `-t '\t' -k1,1`: urutkan berdasarkan key saja.
- Waktu yang dicatat (baris 56–81) mencakup map, sort, dan reduce. Mencetak laporan tidak termasuk.

### `infra/` (Docker, `config`, `run_hadoop_job.sh`)
- Hadoop 3.4.1 pseudo-distributed dengan 4 container: namenode, datanode, resourcemanager, nodemanager.
- Dockerfile memasang python3 karena image resmi tidak punya. Mirror CentOS 7 sudah mati, jadi diambil dari vault.
- `config`:
  - `dfs.replication=1` karena hanya ada satu datanode.
  - NodeManager diberi 4096 MB dan 4 vcore.
  - `DefaultResourceCalculator` artinya YARN hanya menghitung memori. Vcore tidak dibatasi.
  - pmem/vmem check dimatikan. [dugaan] Tanpa ini proses Python anak bisa membuat container dibunuh karena batas virtual memory.
- `run_hadoop_job.sh`:
  - Baris 17: tunggu safe mode selesai.
  - Baris 19: `put` data ke HDFS.
  - Baris 25: `-files` mengirim ketiga script ke direktori kerja setiap container, sehingga perintah `python3 mapper.py` bisa menemukannya.
  - Baris 26: combiner hanya dipasang bila ada argumen `combiner`.
  - Baris 34: `getmerge` menggabungkan file `part-*` menjadi satu TSV.
  - Timer (baris 22–29) hanya membungkus `hadoop jar`. Upload HDFS dan `getmerge` tidak termasuk.

### `spark/spark_job.py`
- Baris 24–32, skema manual: tanpa `inferSchema`, Spark tidak perlu membaca data dua kali.
- Mode DataFrame:
  - Baris 39–44: filter yang sama dengan mapper.
  - Baris 47–54: `explode` satu trip menjadi dua key, setara mapper yang mencetak dua baris.
  - Baris 57–61: satu `groupBy`, setara reducer.
  - Baris 62: `collect()` adalah satu-satunya action. Sebelum itu semuanya lazy.
- Mode RDD:
  - Baris 65–82: `parse_line` adalah salinan logika mapper.
  - Baris 88: `minPartitions=defaultParallelism`. Default `textFile` hanya 2 partisi, sehingga 2 dari 4 core menganggur.
  - Baris 90: `reduceByKey` juga menjumlah di dalam tiap partisi sebelum shuffle, jadi sudah seperti combiner bawaan.
- Baris 101–104: `local[4]` dengan memori 4g sengaja disamakan dengan batas NodeManager (4 vcore, 4096 MB).
- Timer (baris 100–115) **termasuk** startup SparkSession. Penulisan TSV (9.335 byte) dan `spark.stop()` **tidak** termasuk.

### `benchmark/run_benchmark.py`
- `wall_sec`: waktu dari luar proses, termasuk `docker exec`, upload HDFS, dan startup JVM/Python.
- `job_sec`: angka "Waktu Eksekusi" yang dilaporkan engine sendiri.
- CPU dan memori engine lokal: psutil mengambil sampel tiap 0,1 s untuk proses beserta semua child-nya. Hasilnya dibandingkan dengan `getrusage` dan diambil yang lebih besar (baris 81–85), supaya proses yang sangat singkat tetap terhitung.
- CPU dan memori Hadoop diambil dari counters di log (baris 88–98), karena prosesnya berjalan di dalam container.
- Median dari 3 ulangan dipakai supaya satu run yang lambat tidak menyeret hasil.
- Setiap baris langsung ditulis ke CSV, jadi run yang terhenti tidak menghapus hasil sebelumnya.

### `benchmark/verify_results.py`
- Referensinya MapReduce lokal.
- Himpunan key harus sama dan count harus identik.
- Total dan rata-rata boleh berbeda paling banyak `TOLERANCE = 0.011`.
- Selisih terbesar yang benar-benar terjadi adalah 0,01 (di 100k, 1m, dan full). Di 500k selisihnya 0. [diukur]

---

## 4. Membaca hasil (ini yang paling sering ditanya)

1. **Biaya tetap Hadoop.** Hadoop butuh 17,34 s untuk 4,5 MB, sedangkan MR lokal 0,53 s. Logika per baris keduanya sama. [dugaan] Selisihnya adalah biaya tetap YARN: submit job, menyalakan ApplicationMaster, dan menyalakan JVM container. Biaya ini tidak diurai per komponen.
2. **Spark nyaris datar (5–7 s) di semua ukuran.** Waktunya didominasi startup SparkSession/JVM. Urutannya tidak naik rapi: DataFrame 500k (7,13) lebih lambat dari 1m (6,37). Selisih sekitar 1 s ini masih dalam variasi antar-run dengan 3 ulangan.
3. **Titik silang.** MR lokal lebih cepat dari Spark sampai 1m. Di data penuh, Spark lebih cepat (10,91 vs 7,30–7,43). Jadi titik silangnya ada di antara 1m dan full, di mesin ini.
4. **Spark vs Hadoop di data penuh**: rasio terendah 24,51/7,43 = 3,3× (combiner vs DataFrame), tertinggi 33,40/7,30 = 4,6× (tanpa combiner vs RDD).
   - [dugaan] Penyebabnya:
     - Hadoop menulis data antara ke disk (lihat poin 5).
     - Hadoop memakai 2 map task, sedangkan Spark 4 core.
     - Setiap task Hadoop punya JVM sendiri.
     - Spark DataFrame membaca CSV di JVM tanpa Python per baris.
   - Tidak diurai mana yang paling besar.
5. **Spilled Records** menunjukkan berapa kali record ditulis ke disk.
   - Pada 100k sampai 1m: spilled = 2× map output records.
   - Pada full: spilled = **3×** (17.572.224 = 3 × 5.857.408). [diukur] Log task menunjukkan setiap map task melakukan 2 spill ("Finished spill 0", "Finished spill 1"), karena buffer `mapreduce.task.io.sort.mb` 100 MB dengan batas lunak 80 MiB terlampaui. Dua spill itu lalu harus di-merge.
6. **Combiner.**
   - Data penuh: shuffle turun dari 137,6 MB ke 32,5 KB (−99,98%), spilled turun dari 17,57 juta ke 3.231 record, job_sec turun dari 33,40 ke 24,51 s (−27%), dan CPU counter turun dari 19,54 ke 10,47 s.
   - 100k: justru lebih lambat (19,54 vs 17,34). [dugaan] Combiner menambah proses Python dan satu kali sort, sedangkan data yang dihemat terlalu kecil untuk menutup biaya itu.
7. **cpu_sec lebih besar dari waktu job.**
   - Spark: CPU 23 s untuk job 7 s, artinya ±3 core aktif bersamaan.
   - MR lokal: CPU 14,51 s untuk job 10,91 s, padahal map, sort, dan reduce berjalan berurutan. [dugaan] GNU `sort` memakai beberapa thread.
8. **Rasio berbeda antar-sesi.** Arsip sesi terpisah (`benchmark_arsip_sesi_terpisah.csv`) memberi Spark 2,2–2,9× lebih cepat dari Hadoop di 1m. Sesi ini memberi 3,3–4,5× di 1m. Karena itu semua ukuran dijalankan ulang dalam **satu sesi**, dan setiap klaim terikat pada sesi itu.

---

## 5. Kalimat aman dan yang dihindari

Aman:
- "Di mesin ini, pada sesi benchmark ini, median 3 ulangan menunjukkan ..."
- "Hasil kelima engine identik untuk count dan berbeda paling banyak 0,01 untuk nilai desimal."
- "Ini dugaan kami. Kami tidak mengurai waktu per komponen."
- "Itu tidak kami uji di proyek ini."

Hindari:
- "Spark selalu X kali lebih cepat." Rasionya bergantung pada ukuran data dan sesi.
- "Hadoop lambat." Yang benar: biaya tetapnya besar untuk data sekecil ini.
- "Ini big data." 133 MB muat di RAM laptop. Yang dibahas adalah perilaku engine, bukan skala produksi.
- "Hadoop lebih hemat CPU/memori dari Spark." Cara ukurnya berbeda.
- "Fault tolerance sudah teruji."
- Menyebut angka yang tidak ada di CSV atau log.

---

## 6. Kemungkinan pertanyaan Dr. Ferry dan jawaban singkat

### A. Konsep

1. **Bedanya HDFS, YARN, dan MapReduce?**
   - HDFS menyimpan file dalam blok (namenode menyimpan metadata, datanode menyimpan blok).
   - YARN membagi sumber daya: resourcemanager memberi container, nodemanager menjalankannya.
   - MapReduce adalah model pemrograman yang berjalan di atas YARN. Di proyek ini map dan reduce ditulis dalam Python lewat Hadoop Streaming.
2. **Kenapa Hadoop Streaming, bukan Java?** Mapper dan reducer yang sama bisa dipakai di simulasi lokal dan di Hadoop, jadi perbandingannya adil di level logika. Konsekuensinya ada biaya pipe dan konversi teks per record. [dugaan]
3. **Apa yang terjadi di shuffle?**
   - Output map dipartisi per reducer, diurutkan per key, ditulis (spill) ke disk, lalu diambil oleh reducer dan di-merge.
   - Di sini hanya ada 1 reducer (default), karena hasilnya kecil (284 key) dan cukup satu file output.
4. **Bedanya combiner dan reducer?** Combiner berjalan di sisi map, sebelum shuffle, dan boleh dijalankan 0 sampai n kali. Karena itu output combiner harus berformat sama dengan inputnya dan tidak boleh menghitung rata-rata.
5. **Kenapa rata-rata tidak di combiner?** Rata-rata dari rata-rata salah bila jumlah datanya berbeda. Yang dikirim adalah count dan total, lalu reducer menghitung total/count.
6. **Lazy evaluation di Spark?** `where`, `select`, dan `groupBy` hanya menyusun rencana eksekusi. Eksekusi baru terjadi saat `collect()` (baris 62).
7. **DataFrame vs RDD?**
   - DataFrame dioptimasi oleh Catalyst, dan filter serta agregasinya berjalan di JVM.
   - RDD menjalankan fungsi Python (`parse_line`) per baris di worker Python.
   - Di data penuh hasilnya hampir sama (7,43 vs 7,30 s), jadi bedanya tertutup oleh biaya startup.
8. **Fault tolerance?** [tidak diuji]
   - Hadoop menjalankan ulang task attempt yang gagal. HDFS juga punya replikasi blok, tetapi di sini replikasinya 1.
   - Spark menghitung ulang partisi yang hilang dari lineage.
9. **Kapan pilih MapReduce, kapan Spark?**
   - MapReduce: batch sekali jalan pada data jauh lebih besar dari RAM klaster, atau ketika klaster Hadoop sudah ada.
   - Spark: query iteratif atau interaktif, SQL, dan data yang sebagian besar muat di memori.
   - Jangan klaim satu engine selalu lebih baik.

### B. Kode

10. **Kenapa ada prefix `ZONE|` dan `HOUR|`?** Agar dua agregasi bisa dibuat dalam satu job tanpa bentrok key (zona 10 dan jam 10).
11. **Kenapa ada `LC_ALL=C`?** Supaya urutannya byte-order, sama dengan Hadoop, dan lebih cepat. Reducer hanya butuh key yang sama berdampingan, dan C locale menjamin itu secara konsisten.
12. **Kenapa reducer butuh input terurut?** Reducer hanya mengingat satu key. Kalau tidak terurut, satu key bisa muncul di beberapa baris output.
13. **Fungsi `-files`?** Mengirim `mapper.py`, `combiner.py`, dan `reducer.py` ke direktori kerja setiap container.
14. **Reducer punya try/except, combiner tidak. Kenapa beda?**
    - Combiner hanya menerima output mapper dalam job yang sama, jadi formatnya terjamin.
    - try/except di reducer bersifat defensif.
    - Akui bahwa gayanya tidak seragam. Kebenarannya tetap terbukti oleh verifikasi.
15. **Kenapa toleransinya 0,011, bukan 0,01?**
    - Penjumlahan float dalam urutan berbeda bisa menggeser pembulatan digit terakhir sebesar 0,01.
    - Selisih 0,01 dalam biner bisa terhitung sedikit di atas 0,01 (misalnya 0,0100000001).
    - Jadi 0,011 berarti "boleh beda maksimal 1 sen".
    - Selisih terbesar yang benar-benar terjadi adalah 0,01. Count selalu identik.
16. **Kenapa skema Spark ditulis manual?** Agar `inferSchema` tidak membaca seluruh file dua kali, dan tipe kolomnya pasti.
17. **Kenapa ada `minPartitions=defaultParallelism`?** Default `textFile` hanya 2 partisi. Dengan `local[4]`, 2 core akan menganggur.
18. **Kenapa ada `strip('"')`?** pyarrow menulis header CSV dengan tanda kutip. Data saat ini tidak berkutip, jadi `strip` ini bersifat defensif.
19. **Kolom `passenger_count` dan `payment_type` dipakai di mana?** Tidak dipakai di agregasi. Kolom itu ikut diekstrak saat preprocessing. Akui saja.

### C. Eksperimen dan angka

20. **Apakah perbandingan waktunya adil?**
    - `job_sec` Hadoop tidak termasuk upload HDFS. `job_sec` Spark termasuk startup SparkSession.
    - `wall_sec` mencakup semuanya, dan urutan cepat-lambat di data penuh tetap sama di `wall_sec`.
    - Keterbatasan:
      - Hadoop berjalan di Docker dengan Java 8, sedangkan Spark di host dengan Java 21.
      - Satu mesin, bukan klaster.
21. **Kenapa median, dan kenapa hanya 3 ulangan?** Median tahan terhadap satu run yang menyimpang. Tiga ulangan adalah kompromi dengan waktu (5 engine × 4 ukuran × 3 = 60 run). Variasi sekitar 1 s di Spark menunjukkan 3 ulangan belum cukup untuk membedakan selisih kecil.
22. **Kenapa Spark 500k lebih lambat dari 1m?** Selisih sekitar 1 s ada di dalam noise. Waktu Spark didominasi startup, bukan ukuran data. Jangan menarik kesimpulan dari urutan itu.
23. **Kenapa combiner lebih lambat di 100k?** Lihat bagian 4 poin 6. [dugaan] Biaya proses tambahan lebih besar dari shuffle yang dihemat.
24. **Kenapa ada 2 split?**
    - `mapreduce.job.maps` default bernilai 2. FileInputFormat membagi file menjadi sekitar total/2, dibatasi ukuran blok 128 MB.
    - Di data penuh (132,8 MB), ukuran blok juga menghasilkan 2 blok. Hasilnya sama-sama 2.
    - Log full: "number of splits:2". [diukur]
25. **Di log ada `Killed map tasks=1`. Apakah ada yang gagal?** Tidak. [diukur, dicek di log ApplicationMaster job full]
    - Hadoop menjalankan *speculative execution*: untuk task `m_000001` ia meminta attempt cadangan (`_1`).
    - Attempt asli selesai lebih dulu, sehingga attempt cadangan dibunuh saat masih `UNASSIGNED` dan belum mendapat container. Karena itu `Launched map tasks` tetap 2.
    - `Shuffled Maps=2` dan `Failed Shuffles=0`, dan hasilnya lolos verifikasi.
    - Counter yang sama juga muncul di log 1m dan full + combiner. Yang dicek sampai ke log ApplicationMaster hanya job full.
26. **Kenapa Spilled Records data penuh 3× output map?** Lihat bagian 4 poin 5. Buffer sort 100 MB terlampaui, sehingga setiap map spill 2 kali lalu merge.
27. **Bisakah memori Hadoop dibandingkan dengan Spark?** Tidak. Hadoop memakai counter "Physical memory snapshot" dari task, sedangkan engine lokal memakai RSS puncak dari psutil. Kolomnya bertuliskan MB, tetapi satuannya MiB.
28. **Subset diambil acak?** Tidak. Subset adalah n baris pertama. Tujuannya uji skala, bukan inferensi statistik. Hasil analisis bisnis (top zona, jam sibuk) hanya dipakai dari data penuh.
29. **"Verifikasi 20/20" itu apa saja?** 4 ukuran × 5 baris. Satu baris per ukuran adalah MR lokal dibandingkan dengan dirinya sendiri. Perbandingan yang benar-benar bermakna ada 16 (4 engine × 4 ukuran). Sebut "16 perbandingan, semua OK" supaya tidak dikoreksi.
30. **Kenapa angkanya berbeda dari versi sebelumnya?** Lihat bagian 4 poin 8. Antar-sesi berbeda, jadi semua ukuran dijalankan ulang dalam satu sesi (commit `d6dacc7`).

### D. Kelompok dan proyek

31. **Pembagian kerja anggota?** Pakai PANDUAN bagian 9. Setiap anggota harus bisa menjelaskan file bagiannya sampai ke nomor baris.
32. **Kenapa dataset NYC Taxi?** Dataset publik resmi, cukup besar untuk melihat perbedaan engine, dan punya kolom waktu, lokasi, serta nilai uang untuk agregasi.
33. **Kalau dikembangkan?** Klaster multi-node, replikasi lebih dari 1, lebih banyak reducer, ukuran data melebihi RAM, lebih dari 3 ulangan, dan urai waktu startup vs proses (Spark sudah mencetak "Startup SparkSession" terpisah di baris 130).

---

## 7. Urutan demo singkat

```bash
python mapreduce/run_mapreduce.py data/taxi_100k.csv
```

```bash
docker compose -f infra/docker-compose.yml exec namenode bash /workspace/infra/run_hadoop_job.sh data/taxi_100k.csv combiner
```

```bash
python spark/spark_job.py data/taxi_100k.csv --mode df
```

```bash
python benchmark/verify_results.py taxi_100k
```

Pakai 100k saat demo supaya cepat. Untuk angka data penuh, tunjukkan `benchmark/results/benchmark.csv` dan log di `output/hadoop/`.
Sebelum presentasi, pastikan cluster sudah menyala (`docker compose -f infra/docker-compose.yml up -d`) dan safe mode sudah lewat.
