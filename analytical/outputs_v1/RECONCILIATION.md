# Rekonsiliasi Angka, Satuan, dan Skema (RECONCILIATION.md)

Dokumen ini menyajikan rekonsiliasi teknis atas seluruh perbedaan terminologi, jumlah kolom skema, ukuran penyimpanan, dan konvensi satuan antara catatan Data Engineer (`reports/`), file fisik di disk, dan runtime analisis analitis PySpark.

---

## 1. Rekonsiliasi Jumlah Kolom Skema (58 vs 63 vs 66 Kolom)

| Sumber Skema | Jumlah Kolom | Penjelasan Teknis |
|:---|:---:|:---|
| **File Parquet Asli di Disk (`encounter_analytics`)** | **58 Kolom** | Skema fisik yang dihasilkan oleh Script 09 lokal (`scripts/09_integrate_analytics_dataset.py`). Berisi 50 atribut encounter UCI, ditambah 6 agregasi (`activity_count`, `diagnosis_count`, `medication_count`, `laboratory_count`, `metric_count`, `event_count`) dan 2 kolom provenance (`data_origin`, `synthetic_flag`). |
| **Laporan Pipeline HDFS Data Engineer** | **63 Kolom** | Skema yang dihasilkan oleh Script 13 (`scripts/13_integrate_analytics_hdfs.py`) yang memuat beberapa variasi nama kolom agregat dan metadata tambahan. |
| **DataFrame Runtime di `loader.py`** | **66 Kolom** | Modul `common/loader.py` mengimplementasikan **Adapter Skema** yang menormalisasi nama kolom lokal dan HDFS ke standar universal. Loader menambahkan 8 kolom turunan: `total_event_count`, `total_activity_count`, `diagnosis_activity_count`, `medication_activity_count`, `laboratory_activity_count`, `metric_activity_count`, `has_clinical_activity`, `has_patient_event`. Sehingga: $58 + 8 = 66$ kolom. |

---

## 2. Rekonsiliasi Ukuran Penyimpanan & Konvensi Satuan (MB vs MiB, GB vs GiB)

Laporan Data Engineer menggunakan singkatan "GB" dan "MB", namun perhitungannya menggunakan pembagi biner $1024$ (IEC standard: GiB dan MiB).

### Perbandingan Nilai Aktual:
1. **Raw Mentah (3 Dataset Kumulatif):**
   - **Byte Aktual:** `5.008.326.279 bytes`
   - **Biner (IEC):** $5.008.326.279 \div 1024^3 = \mathbf{4,664\text{ GiB}}$ (Tertulis di laporan DE sebagai: *4.66 GB*).
   - **Desimal (SI):** $5.008.326.279 \div 1000^3 = \mathbf{5,008\text{ GB}}$.

2. **Curated Parquet (3 Dataset Kumulatif):**
   - **Byte Aktual:** `501.710.538 bytes`
   - **Biner (IEC):** $501.710.538 \div 1024^2 = \mathbf{478,47\text{ MiB}}$ (Tertulis di laporan DE sebagai: *478.47 MB*).
   - **Desimal (SI):** $501.710.538 \div 1000^2 = \mathbf{501,71\text{ MB}}$.

3. **Persentase Penghematan Penyimpanan (89,98%):**
   $$\text{Reduksi} = \frac{5.008.326.279 - 501.710.538}{5.008.326.279} \times 100\% = \mathbf{89,9818\%} \approx \mathbf{89,98\%}$$
   > **Cakupan Reduksi:** Angka 89,98% adalah penghematan storage kumulatif dari 3 dataset mentah (`encounters` CSV, `patient_events` NDJSON, `clinical_activity` CSV) menjadi 3 dataset terkurasi Parquet Snappy di layer curated, **BUKAN** ukuran tabel `encounter_analytics` saja.

4. **Ukuran Parquet `encounter_analytics` di Disk Lokal (326,32 MB vs 328,87 MB):**
   - **326,32 MB (342.170.835 bytes):** Ukuran murni 16 file `.parquet` data di folder `data/curated/parquet/encounter_analytics`.
   - **328,87 MB:** Ukuran total folder yang dilaporkan Data Engineer pada filesystem yang menyertakan metadata partisi (`_SUCCESS`, file `.crc` checksum, dan overhead alokasi blok disk).

---

## 3. Rekonsiliasi Kolom Waktu / Timestamp (R5)

1. **`data/curated/parquet/encounter_analytics`:**
   - **Hasil `printSchema()`:** TIDAK ADA kolom waktu (`event_timestamp` tidak ada).
   - **Penyebab:** Script pipeline 09/13 melakukan agregasi count per encounter (`count("*").alias("event_count")`), sehingga atribut waktu tidak dipertahankan.
2. **`data/curated/parquet/patient_events`:**
   - **Hasil `printSchema()`:** MEMILIKI kolom `event_timestamp` bertipe string ISO 8601 UTC (contoh: `2026-01-07T02:33:31Z`), dengan volume 4.406.252 event.
   - **Solusi Analisis:** Aspek temporal dievaluasi pada dataset `patient_events` dengan label eksplisit *"Simulated Timestamp dari patient_events"*.

---

## 4. Konvensi Satuan Baku yang Dipakai di Seluruh Modul Analisis

Untuk menghindari ambiguitas, seluruh modul analisis `analytical/` menerapkan konvensi berikut:
- **Ukuran File:** Menyebutkan satuan IEC (MiB / GiB) dan jumlah byte riil jika relevan.
- **Waktu Eksekusi:** Detik (s) dengan presisi 3 desimal.
- **Proporsi:** Persentase (%) dengan 2 desimal, dan selisih proporsi dinyatakan dalam poin persen (**pp**).
- **Ukuran Efek:** Cramér's V (4 desimal), Risk Ratio (3 desimal), Cohen's d (2 desimal).
