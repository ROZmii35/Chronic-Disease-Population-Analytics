# Hasil Eksekusi & Analisis: Velocity (Kecepatan & Aliran Data)

## 1. Catatan Limitasi Penting (Aturan Keras)
Dataset `encounter_analytics` **TIDAK MEMILIKI KOLOM TIMESTAMP**. Oleh karena itu, analisis deret waktu (*time-series trend*) longitudinal tidak dapat dilakukan secara faktual. Sebagai gantinya, modul ini mendemonstrasikan aspek Velocity melalui **Simulasi Aliran Micro-Batch (*SIMULATED STREAMING*)**.

---

## 2. Hasil Simulasi Micro-Batch Ingestion
- Total Batch Disimulasikan: 8 partisi.
- Total Baris Diproses: 6,860,970 baris.
- Rata-rata Throughput Pemrosesan: **1,501,516.73 baris/detik**.
- Rata-rata Latensi per Batch: **1.1729 detik**.

---

## 3. Hasil Benchmark Partisi Spark (Data Engineer Ground Truth)
Berdasarkan `reports/partition_benchmark.csv`:
- **2 Partisi:** Median 4,421 detik (Throughput: 3,11M records/s).
- **4 Partisi:** Median 3,151 detik (Throughput: 4,36M records/s).
- **8 Partisi:** Median **2,444 detik** (Throughput: **5,62M records/s**) $ightarrow$ **Paling Optimal**.
