#!/usr/bin/env python3
import sys

def main():
    # Format kolom CSV:
    # 0: tpep_pickup_datetime, 1: PULocationID, 2: passenger_count,
    # 3: trip_distance, 4: payment_type, 5: total_amount
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('"tpep_pickup_datetime"') or line.startswith("tpep_pickup_datetime"):
            continue

        parts = line.split(",")
        if len(parts) < 6:
            continue

        try:
            pickup_dt = parts[0].strip('"')
            zone_id = parts[1].strip('"')
            distance = float(parts[3])
            amount = float(parts[5])
        except ValueError:
            continue

        # Filter transaksi tidak valid (tarif <= 0 atau jarak negatif)
        if amount <= 0 or distance < 0:
            continue

        # Zona kosong atau waktu terpotong tidak bisa dikelompokkan dengan benar, jadi dibuang.
        if not zone_id or len(pickup_dt) < 13:
            continue

        # Ekstrak jam (HH) dari "YYYY-MM-DD HH:MM:SS"
        hour = pickup_dt[11:13]

        # Emit Key-Value untuk Agregasi Zona dan Jam Sibuk
        # Format: KEY \t count,total_amount,trip_distance
        print(f"ZONE|{zone_id}\t1,{amount:.2f},{distance:.2f}")
        print(f"HOUR|{hour}\t1,{amount:.2f},{distance:.2f}")


if __name__ == "__main__":
    main()
