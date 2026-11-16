# Hasil Eksekusi & Interpretasi: Veracity (Kualitas & Keandalan Data)

Modul ini membuktikan tingkat keandalan data (*Veracity*) dengan membandingkan langsung kondisi **sebelum** kurasi pada dataset UCI asli (101.766 baris) terhadap kondisi **sesudah** kurasi pada dataset Apache Spark Parquet `encounter_analytics` (13.734.415 baris).

---

## 1. Tiga Masalah Kualitas Data Utama (Before vs After Terukur)

| No | Isu Kualitas Data | Sebelum: UCI Asli (101.766 baris) | Sesudah: Curated Sintetis (13.734.415 baris) | Dampak Klinis & Integritas |
|:---|:---|:---:|:---:|:---|
| 1 | **Eliminasi Karakter Kotor '?' $ightarrow$ NULL Sejati** | 98,569 baris kotor (96,86% pada `weight`) | **0 baris (0,00%)** sisa '?'; 100% terstandarisasi menjadi `NULL` | **PENTING:** Cleaning hanya menstandarkan representasi ketiadaan nilai (noise format), **BUKAN melakukan imputasi (mengisi nilai)**. Ketiadaan data tetap transparan. |
| 2 | **Noise Kategori Gender 'Unknown/Invalid'** | 3 baris (0.0029%) | 411 baris (0.003%) | Proporsi noise diwariskan secara presisi oleh generator bootstrap; volume sangat kecil sehingga aman dipertahankan tanpa distorsi. |
| 3 | **Pasien Meninggal / Hospice (Discharge 11, 13, 14, 19, 20, 21)** | 2,423 baris (2.38%) | 326,515 baris (2.38%) | Pasien meninggal atau di hospice secara medis **tidak mungkin readmisi**. Memasukkannya dalam penyebut (*denominator*) membiaskan tingkat readmisi menjadi lebih rendah (*underestimation*). |

---

## 2. Tabel Keputusan Penanganan Kualitas Data (*Handling Decisions*)

| Kolom | % Missing | Keputusan | Alasan Metodologis |
|:---|:---:|:---|:---|
| `weight` | 96,86% | **Dikeluarkan dari analisis utama** | *Severe data loss* (>96%); pemaksaan analisis akan menimbulkan seleksi bias berat. |
| `max_glu_serum` | 94,74% | **Diberi flag / Analisis sub-populasi** | 94,74% bukan missing acak, melainkan protokol dokter yang hanya menguji kasus akut (5,26%). |
| `A1Cresult` | 83,27% | **Diberi flag / Analisis sub-populasi** | Tes HbA1c hanya dilakukan pada 16,73% encounter; dianalisis sebagai indikator kepatuhan protokol. |
| `medical_specialty` | 49,09% | **Diberi flag 'Missing/Unknown'** | Separuh dokter tidak tercatat spesialisasi; dievaluasi sebagai evaluasi operasional rumah sakit. |
| `payer_code` | 39,56% | **Dikeluarkan dari analisis klinis** | Atribut administratif pembayaran, tidak relevan terhadap luaran klinis readmisi. |
| `race` | 2,23% | **Diberi flag 'Unknown'** | Missing minimal (2,23%), dipertahankan sebagai kategori terpisah tanpa imputasi buatan. |
| `diag_1` | 0,02% | **Diberi flag 'Unknown'** | Hanya 0,02% missing; dimasukkan ke dalam kategori ICD-9 'Unknown'. |

---

## 3. Validasi Integritas Relasional & Batas Klinis

1. **Keunikan Primary Key:** **0 duplikasi** pada 13.734.415 record `encounter_id`.
2. **Kelengkapan Foreign Key:** **0 baris NULL** pada pasangan kunci `patient_nbr` dan `encounter_id`.
3. **Validitas Batas Klinis:**
   - `time_in_hospital` (1–14 hari): **100% valid** (0 baris di luar rentang).
   - `number_diagnoses` (1–16 diagnosis): **100% valid** (0 baris di luar rentang).
4. **Efek Bootstrap:** Terdapat **9.650.229 pasien unik**; sebanyak **2.349.707 pasien** memiliki lebih dari 1 encounter (rata-rata 1,423 kunjungan per pasien).
