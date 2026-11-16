# Hasil Eksekusi & Analisis: Volume (Skala Data & Efisiensi)

## 1. Skala Data Nyata
- **Total Baris:** 13,734,415 encounter terintegrasi.
- **Total Kolom:** 66 atribut (termasuk hasil agregasi relasional).
- **Jumlah Partisi Fisik:** 16 file Parquet.
- **Ukuran File Parquet Lokal:** 326.32 MB.

## 2. Efisiensi Reduksi Penyimpanan
- Raw Mentah Total: **4,66 GB** (5.008.326.279 bytes).
- Curated Parquet Total: **478,47 MB** (501.710.538 bytes).
- **Persentase Penghematan Storage:** **89,98%** berkat kompresi Snappy dan columnar format Parquet.

## 3. Kecepatan Agregasi Tunggal
- Eksekusi `groupBy("readmitted").count()` pada 13,7 juta baris selesai dalam **1.812 detik**.
