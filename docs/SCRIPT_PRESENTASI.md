# Script Presentasi Kelompok 10

Urutan bicara mengikuti alur kode: data masuk, logika MapReduce, eksekusi lokal, eksekusi klaster, Spark, verifikasi, penutup. Teks dalam kutipan adalah yang diucapkan. Teks dalam kurung siku `[...]` adalah penanda untuk layar dan tidak dibacakan.

Setiap angka di bawah berasal dari `README.md`, `docs/RINGKASAN_PRESENTASI.md`, atau kode di repo. Penjelasan sebab yang belum diukur ditandai **[dugaan]** dan harus diucapkan sebagai dugaan.

| No | Pembicara | Bagian | File di layar |
|---|---|---|---|
| 1 | Ketua | Pembuka | `README.md` |
| 2 | Anggota 2 | Dataset dan preprocessing | `preprocessing/prepare_dataset.py` |
| 3 | Ketua | Mapper, combiner, reducer | `mapreduce/mapper.py`, `combiner.py`, `reducer.py` |
| 4 | Anggota 3 | Runner MapReduce lokal | `mapreduce/run_mapreduce.py` |
| 5 | Anggota 4 | Klaster Hadoop dan Streaming | `infra/docker-compose.yml`, `infra/run_hadoop_job.sh` |
| 6 | Ketua | Spark DataFrame dan RDD | `spark/spark_job.py` |
| 7 | Anggota 5 | Verifikasi dan benchmark | `benchmark/verify_results.py`, tabel di `README.md` |
| 8 | Ketua | Kesimpulan dan keterbatasan | `README.md` |

---

## 1. Pembuka

**Ketua.** Layar: `README.md`.

> "Selamat pagi, Bapak Dosen dan rekan-rekan. Kelompok 10 membandingkan Hadoop MapReduce dengan Apache Spark pada satu pekerjaan yang sama.
>
> Pekerjaannya: dari data NYC Yellow Taxi Januari 2024, hitung jumlah trip, total tarif, rata-rata tarif, dan rata-rata jarak, per zona penjemputan dan per jam. Kami menjalankannya di empat ukuran data, dari 100 ribu baris sampai 2,96 juta baris.
>
> Ada lima engine: MapReduce lokal, Hadoop YARN, Hadoop YARN dengan combiner, Spark DataFrame, dan Spark RDD. Semuanya dijalankan di satu laptop. Kami akan mulai dari data, lalu logika MapReduce, lalu tiap engine, dan terakhir bukti bahwa hasilnya sama."

---

## 2. Dataset dan preprocessing

**Anggota 2.** Layar: `preprocessing/prepare_dataset.py`.

> "Sumber datanya file Parquet resmi dari NYC TLC: 2.964.624 baris dan 19 kolom. Kami hanya butuh enam kolom. [baris 14–21] Kolomnya: waktu jemput, ID zona jemput, jumlah penumpang, jarak, jenis pembayaran, dan total tarif. PyArrow membaca enam kolom itu saja, jadi kolom lain tidak dimuat.
>
> Dari tabel itu kami membuat empat file CSV: 100 ribu, 500 ribu, 1 juta, dan seluruh baris. [baris 24–29] File terbesar berukuran 132,8 MB.
>
> Kami memakai CSV karena Hadoop Streaming membaca teks per baris lewat `stdin`. Mapper Python kami tidak perlu pustaka Parquet.
>
> Subset diambil dengan `table.slice` dari baris pertama. [baris 63] Ini bukan sampel acak. Alasannya determinisme: setiap anggota yang menjalankan skrip mendapat file yang sama, dan file yang lebih besar memuat file yang lebih kecil. Kami sudah memeriksanya: 1.000.001 baris pertama `taxi_full.csv` identik byte per byte dengan `taxi_1m.csv`. Jadi yang berubah antar-ukuran hanya jumlah data. Karena bukan sampel acak, hasil bisnis seperti zona teramai hanya kami ambil dari data penuh."

---

## 3. Mapper, combiner, reducer

**Ketua.** Layar: `mapper.py`, lalu `combiner.py`, lalu `reducer.py`.

> "Mapper membaca satu baris CSV dan membuang baris yang tidak layak: tarif nol atau negatif, jarak negatif, zona kosong, waktu terpotong, atau kolom yang tidak bisa dibaca sebagai angka. [mapper baris 25–31]
>
> Untuk setiap baris yang lolos, mapper mencetak dua pasangan key-value. [baris 38–39] Satu dengan key `ZONE|` diikuti ID zona, satu lagi dengan key `HOUR|` diikuti jam. Isinya sama: satu trip, tarif, dan jarak. Prefix itu penting: zona 10 dan jam 10 akan bentrok kalau tidak dibedakan. Dengan dua key per baris, dua agregasi selesai dalam satu job dan satu kali baca data.
>
> Combiner berjalan di sisi map sebelum shuffle. [combiner baris 31–33] Isinya hanya penjumlahan: jumlah trip, total tarif, total jarak. Combiner sengaja tidak menghitung rata-rata, karena rata-rata dari rata-rata menghasilkan angka yang salah saat jumlah trip per kelompok berbeda. Ada alasan kedua: Hadoop boleh menjalankan combiner nol, satu, atau beberapa kali. Karena itu format keluaran combiner kami samakan dengan mapper, supaya hasilnya benar berapa kali pun combiner dipanggil.
>
> Reducer menerima input yang sudah diurutkan per key oleh framework. [reducer baris 31–35] Ia menyimpan akumulator untuk satu key saja, dan menghitung rata-rata ketika key berganti. Memori yang dipakai tetap, tidak bergantung pada jumlah baris."

---

## 4. Runner MapReduce lokal

**Anggota 3.** Layar: `mapreduce/run_mapreduce.py`, lalu terminal.

> "Sebelum ke klaster, kami menjalankan mapper dan reducer yang sama di mesin lokal lewat `run_mapreduce.py`. Hasilnya menjadi pembanding untuk semua engine lain.
>
> Konsepnya `mapper | sort | reducer`, tetapi kodenya tidak memakai pipe. [baris 56–79] Ia menjalankan tiga proses terpisah dan menyambungkannya lewat file sementara: mapper menulis ke file, `sort` membaca file itu dan menulis file terurut, lalu reducer membaca file terurut. Kami memilih cara ini agar tiap fase bisa diukur terpisah dan meniru sifat MapReduce yang menulis hasil antara ke disk.
>
> Perintah `sort` dijalankan dengan `LC_ALL=C`. [baris 72] Ini memaksa pengurutan berdasarkan nilai byte, sehingga urutan key sama dengan urutan yang dipakai Hadoop untuk key teks, dan tidak bergantung pada locale komputer. Reducer bergantung pada urutan ini: kalau key yang sama tidak berdekatan, hasilnya salah.
>
> Fungsi `format_cli_report` mencetak ringkasan: zona dengan trip terbanyak, dan jumlah trip per jam dalam bentuk batang `#`. Fungsi yang sama dipakai oleh Hadoop dan Spark, jadi bentuk laporannya seragam."

---

## 5. Klaster Hadoop dan Hadoop Streaming

**Anggota 4.** Layar: `infra/docker-compose.yml`, lalu `infra/run_hadoop_job.sh`.

> "Klaster Hadoop 3.4.1 kami jalankan dengan Docker Compose. Ada empat service: NameNode dan DataNode untuk HDFS, ResourceManager dan NodeManager untuk YARN. Jumlah DataNode satu, jadi replikasi blok satu. Artinya klaster ini tidak punya toleransi kegagalan, dan bagian itu tidak kami uji.
>
> Kapasitas NodeManager diatur 4 vcore dan 4096 MB di konfigurasi YARN. [`infra/config`, baris 10–11] Angka ini kapasitas yang dilaporkan ke YARN, bukan batas yang dipaksakan Docker.
>
> Skrip `run_hadoop_job.sh` menunggu HDFS keluar dari safe mode, lalu mengunggah CSV ke `/data`. [baris 17–20] Setelah itu ia memanggil jar `hadoop-streaming`. [baris 24–28] Opsi `-files` mengirim `mapper.py`, `combiner.py`, dan `reducer.py` ke container. Opsi `-combiner` hanya dipasang kalau kami meminta varian dengan combiner. Hasilnya diambil kembali dengan `getmerge` menjadi satu file TSV. [baris 34]
>
> Log job kami simpan, karena berisi counter Hadoop seperti Spilled Records dan Reduce shuffle bytes. Counter itu yang kami pakai untuk menjelaskan efek combiner."

---

## 6. Spark DataFrame dan RDD

**Ketua.** Layar: `spark/spark_job.py`.

> "Spark kami jalankan dalam mode lokal dengan `local[4]` dan memori driver 4 GB. [baris 103–104] Angka ini kami samakan secara nominal dengan NodeManager: 4 vcore dan 4096 MB. Perlu diingat bahwa ini hanya kira-kira setara. Memori YARN adalah total untuk semua container, sedangkan 4 GB di Spark adalah heap driver, dan proses Python worker pada mode RDD berada di luar angka itu.
>
> Mode DataFrame [baris 35–62] memakai skema yang kami tulis sendiri, tanpa `inferSchema`, supaya Spark tidak membaca file dua kali. [baris 24] Dua key per baris dibuat dengan `explode` pada sebuah array. [baris 48]
>
> Mode RDD [baris 85–91] memakai `flatMap(parse_line)` lalu `reduceByKey`. `reduceByKey` menjumlahkan di tiap partisi sebelum shuffle, jadi perannya mirip combiner. Kami menetapkan `minPartitions` sama dengan jumlah core, [baris 87–88] karena default `textFile` hanya dua partisi dan dua core akan menganggur.
>
> Seluruh transformasi baru berjalan ketika `collect()` dipanggil di akhir."

---

## 7. Verifikasi dan benchmark

**Anggota 5.** Layar: `benchmark/verify_results.py`, lalu tabel di `README.md`.

> "Kecepatan baru boleh dibandingkan setelah hasilnya terbukti sama. `verify_results.py` membandingkan keluaran Hadoop, Hadoop dengan combiner, Spark DataFrame, dan Spark RDD terhadap MapReduce lokal, di keempat ukuran data. Itu 16 perbandingan, dan semuanya OK. Jumlah key yang dibandingkan: 250, 264, 271, dan 284 untuk data penuh, yaitu 260 zona dan 24 jam. Jumlah trip harus sama persis. Selisih total tarif dan jarak diperbolehkan sampai 0,011. [baris 14]
>
> Batas 0,011 ada alasannya: `22.93 - 22.92` di floating point bernilai `0.010000000000001563`, sedikit di atas 0,01. Dengan batas 0,01, selisih pembulatan satu sen yang wajar akan terhitung salah.
>
> Benchmark: tiap kombinasi dijalankan tiga kali dan kami ambil median waktu job. Semua ukuran dijalankan ulang dalam satu sesi, supaya tidak ada selisih antar-sesi.
>
> Pada 100 ribu baris, MapReduce lokal selesai dalam 0,53 detik. Hadoop YARN butuh 17,34 detik, dan Spark sekitar 6 detik. Pada data sekecil ini yang terukur adalah biaya tetap, bukan pemrosesan. Kami menduga biaya tetap Hadoop berasal dari pengajuan job ke YARN dan penyalaan ApplicationMaster serta JVM container, tetapi kami belum mengurainya per komponen. **[dugaan]**
>
> Pada data penuh, 2,96 juta baris: Spark 7,30 sampai 7,43 detik. Hadoop tanpa combiner 33,40 detik, sekitar 4,5 sampai 4,6 kali lebih lambat dari Spark. Hadoop dengan combiner 24,51 detik, sekitar 3,3 kali lebih lambat. MapReduce lokal 10,91 detik. Semua ini berlaku di satu laptop dan dalam satu sesi benchmark.
>
> Efek combiner di data penuh: data shuffle turun dari 137,6 MB menjadi 32,5 KB, atau 99,98 persen. Waktu job turun dari 33,40 menjadi 24,51 detik, 27 persen. Di 100 ribu baris, combiner justru lebih lambat: 19,54 detik lawan 17,34 detik. Dugaan kami, data yang dihemat terlalu kecil untuk menutup biaya proses Python dan sort tambahan. **[dugaan]**"

---

## 8. Kesimpulan dan keterbatasan

**Ketua.** Layar: `README.md`.

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
| `README.md` menulis 20/20 | Script memakai 16 perbandingan | `README.md` baris 26 masih menyebut 20/20 dan sebaiknya disamakan |
