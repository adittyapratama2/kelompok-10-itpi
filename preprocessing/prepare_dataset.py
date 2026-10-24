import os
import shutil
import time
import urllib.request
import pyarrow.parquet as pq
import pyarrow.csv as pv

# URL resmi NYC TLC Yellow Taxi (Januari 2024 - ~2.96 juta baris, ~50 MB Parquet)
DATASET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
PARQUET_PATH = os.path.join(DATA_DIR, "yellow_tripdata_2024-01.parquet")

# Kolom yang relevan untuk analisis transaksi & jam sibuk
SELECTED_COLUMNS = [
    "tpep_pickup_datetime",
    "PULocationID",
    "passenger_count",
    "trip_distance",
    "payment_type",
    "total_amount",
]

# Ukuran subset baris untuk uji skala (scalability benchmark)
SUBSETS = {
    "taxi_100k.csv": 100_000,
    "taxi_500k.csv": 500_000,
    "taxi_1m.csv": 1_000_000,
    "taxi_full.csv": None,  # seluruh bulan Januari 2024
}


def download_parquet():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(PARQUET_PATH):
        size_mb = os.path.getsize(PARQUET_PATH) / (1024 * 1024)
        print(f"[INFO] File Parquet sudah ada: {PARQUET_PATH} ({size_mb:.2f} MB)")
        return

    print(f"[1/2] Mengunduh NYC Yellow Taxi dataset dari {DATASET_URL} ...")
    start = time.time()
    req = urllib.request.Request(DATASET_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response, open(PARQUET_PATH, "wb") as out_file:
        shutil.copyfileobj(response, out_file)  # alirkan per potongan, tidak ditampung utuh di RAM
    elapsed = time.time() - start
    size_mb = os.path.getsize(PARQUET_PATH) / (1024 * 1024)
    print(f"[DONE] Unduhan selesai dalam {elapsed:.2f} detik ({size_mb:.2f} MB)")


def convert_to_csv_subsets():
    print("[2/2] Membaca file Parquet dan mengekstrak kolom penting...")
    table = pq.read_table(PARQUET_PATH, columns=SELECTED_COLUMNS)
    total_rows = table.num_rows
    print(f"[INFO] Total baris di file Parquet asli: {total_rows:,} baris")

    for filename, num_rows in SUBSETS.items():
        out_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(out_path):
            size_mb = os.path.getsize(out_path) / (1024 * 1024)
            print(f"[SKIP] {filename} sudah ada ({size_mb:.2f} MB)")
            continue

        start = time.time()
        sliced = table.slice(0, num_rows)  # length None = sampai baris terakhir
        pv.write_csv(sliced, out_path)
        elapsed = time.time() - start
        size_mb = os.path.getsize(out_path) / (1024 * 1024)
        print(
            f"[OK] {filename} ({sliced.num_rows:,} baris) berhasil dibuat -> {size_mb:.2f} MB ({elapsed:.2f} dtk)"
        )


if __name__ == "__main__":
    download_parquet()
    convert_to_csv_subsets()
