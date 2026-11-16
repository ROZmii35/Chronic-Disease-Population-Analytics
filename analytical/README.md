# Chronic Disease Population Analytics - Modul Analisis (Segmen 5)

Repositori modul analisis analitis independen untuk **Segmen 5: Analytical Results & Healthcare Interpretation** pada Ujian Tengah Semester (UTS) IFB-451 Big Data. Modul ini berfokus pada pembuktian **Veracity** (kualitas data) dan ekstraksi **Value** (wawasan klinis) menggunakan Apache Spark 3.5.1.

---

## 1. Prasyarat Sistem & Konfigurasi

### A. Java Runtime Environment (JRE / JDK)
Apache Spark memerlukan Java 64-bit (build 1.8.0, 11, atau 17).
```bash
java -version
```
*Catatan Windows:* Modul ini telah dilengkapi biner native Windows Apache Hadoop 3.3.5 (`winutils.exe` dan `hadoop.dll` di `analytical/hadoop/bin/`) yang otomatis dikonfigurasi oleh `common/spark_session.py`.

### B. Python Environment
- Python 3.9 (Virtual environment: `analytical/.venv`)
- Pustaka utama: `pyspark==3.5.1`, `pandas==2.2.2`, `pyarrow==16.1.0`, `numpy==1.26.4`, `scipy==1.13.1`, `matplotlib==3.8.4`, `plotly==5.22.0`, `pytest==8.2.2`.

---

## 2. Cara Menjalankan Analisis & Verifikasi

### A. Eksekusi Seluruh Pipeline Analisis
```powershell
analytical\.venv\Scripts\python analytical/run_all.py
```

### B. Menjalankan Modul Tertentu
```powershell
# 1. Veracity (Kualitas Data: Before/After, Keputusan Missing, Pasien Hospice)
analytical\.venv\Scripts\python analytical/run_all.py --only veracity

# 2. Value (Wawasan Kesehatan: Effect Size, 3 Analisis Sensitivitas, Cohen's d)
analytical\.venv\Scripts\python analytical/run_all.py --only value

# 3. Computational Benchmark (Op A vs Op A2 vs Op B, Explain Plans)
analytical\.venv\Scripts\python analytical/run_all.py --only computational

# 4. Volume (Skala Data, Reduksi Storage 89,98%, Rekonsiliasi Satuan)
analytical\.venv\Scripts\python analytical/run_all.py --only volume

# 5. Velocity (Simulasi Aliran Micro-batch & Analisis Temporal patient_events)
analytical\.venv\Scripts\python analytical/run_all.py --only velocity

# 6. Variety (Integrasi Format Multi-source ke Parquet)
analytical\.venv\Scripts\python analytical/run_all.py --only variety
```

### C. Audit Klaim Analitis Otomatis
```powershell
# Jalankan audit klaim otomatis terhadap seluruh tabel bukti fisik
analytical\.venv\Scripts\python analytical/verify_claims.py
```

---

## 3. Struktur Direktori Output

```
analytical/
├── common/                    # Spark session, data loader adapter, icd9, plotting, stats helper
├── volume/                    # 01_volume.py (Skala data & komparasi penyimpanan)
├── velocity/                  # 02_velocity.py (Aliran micro-batch & analisis temporal)
├── variety/                   # 03_variety.py (Integrasi format multi-source ke Parquet)
├── veracity/                  # 04_veracity.py (Kualitas data, missing value before/after, integritas PK)
├── value/                     # 05_value.py (Insight klinis, effect size, 3 uji sensitivitas)
├── computational/             # 06_benchmark_op_a_vs_b.py (Benchmark kompleksitas Spark Op A vs A2 vs B)
├── verify_claims.py           # Script audit otomatis verifikasi ledger klaim
└── outputs/
    ├── volume/                # tables/, charts/, logs/, RESULT.md
    ├── velocity/              # tables/, charts/, logs/, RESULT.md
    ├── variety/               # tables/, charts/, logs/, RESULT.md
    ├── veracity/              # tables/, charts/, logs/, RESULT.md
    ├── value/                 # tables/, charts/, logs/, RESULT.md
    ├── computational/         # tables/, charts/, logs/, RESULT.md
    ├── appendix_for_segment4/ # Salinan benchmark komputasi khusus Developer Segmen 4
    ├── CLAIMS_LEDGER.csv      # Ledger verifikasi seluruh 24 angka klaim analitis
    ├── RECONCILIATION.md      # Rekonsiliasi kolom skema, ukuran penyimpanan, dan satuan
    ├── ENVIRONMENT.md         # Catatan spesifikasi hardware & software eksekusi
    └── SUMMARY.md             # Ringkasan menyeluruh, matriks 5V, dan naskah penjelasan
```

---

## 4. Pemetaan Segmen Presentasi Video (75 Detik / 1:15 Menit)

Segmen 5 difokuskan secara tajam pada pilar **Veracity** dan **Value**:

| Alokasi Waktu | Topik Pembahasan | Slide Visual | Poin Kunci |
|:---:|:---|:---|:---|
| **00:00 - 00:25** (25s) | **Veracity:** Preservasi missing data, eliminasi 100% noise '?', standardisasi NULL tanpa imputasi, integritas PK. | `slide1_veracity.png` | 0 karakter '?' tersisa; 0 duplikasi encounter ID; rentang klinis 100% valid. |
| **00:25 - 00:55** (40s) | **Value:** Peringkat komorbiditas ICD-9, readmisi dini Diabetes (13,00%), selisih pp & Risk Ratio, durasi rawat & polifarmasi. | `slide2_value.png` | Sirkulasi Rank 1 (29,91%); Diabetes Rank 5 (8,61%) tertinggi readmisi dini (+1,83 pp vs baseline 11,17%, RR 1,164x); LOS +0,52 hari; Obat +1,25 jenis. |
| **00:55 - 01:15** (10s) | **Limitasi & Penutup:** Data sintetis bootstrap, ketiadaan timestamp longitudinal, asosiasi bukan kausalitas. | `slide3_limitations_takeaway.png` | Kausalitas vs asosiasi; rekomendasi pemantauan transisi kepulangan pasien diabetes. |

---

## 5. Limitasi Penting Dataset (Wajib Disampaikan)

1. **Sifat Data Sintetis:** Dataset 13,7 juta baris ini dibangkitkan menggunakan metode *patient-level bootstrap sampling* dari 101.766 data mentah UCI Diabetes 130-US Hospitals. Seluruh korelasi adalah simulasi matematis dan **bukan kesimpulan medis dari pasien riil**.
2. **Ketiadaan Timestamp di `encounter_analytics`:** Dataset analitis tidak memuat atribut waktu individual akibat agregasi integrasi pipeline. Aspek waktu dievaluasi via dataset `patient_events` (4.406.252 baris) bertanda *simulated timestamp*.
3. **Cakupan Aktivitas Klinis 1,86%:** Hanya 255.062 encounter (1,86%) yang memiliki pencatatan clinical activity karena batas sampling generator 5%. Nilai 0 pada kolom aktivitas klinis adalah konsekuensi desain sampling, bukan kelalaian pencatatan perawat.
4. **Pasien Meninggal / Hospice:** Sebanyak 326.515 pasien (2,38%) dengan kode discharge 11, 13, 14, 19, 20, 21 tidak mungkin mengalami readmisi secara klinis. Memasukkannya dalam denominator menimbulkan bias *underestimation* yang telah dibuktikan via analisis sensitivitas (+0,23 pp kenaikan readmisi saat dieksklusi).
