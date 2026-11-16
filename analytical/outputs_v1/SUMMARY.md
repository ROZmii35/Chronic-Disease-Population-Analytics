# Ringkasan Eksekutif Analisis Big Data: 5Vs & Wawasan Kesehatan (Segmen 5)

Dokumen ini menyajikan konsolidasi menyeluruh hasil eksekusi analitis pada dataset terkurasi Apache Spark Parquet `encounter_analytics` (13.734.415 baris), yang ditargetkan untuk **Segmen 5: Analytical Results & Healthcare Interpretation (1:15 menit / 75 detik, fokus Veracity & Value)** pada presentasi UTS IFB-451 Big Data.

Seluruh metrik dan visualisasi di bawah ini diperoleh secara terukur dari eksekusi nyata PySpark di lingkungan lokal (8 CPU Cores, Driver 4 GB, 8 Shuffle Partitions).

---

## 1. Pemetaan Lengkap Framework 5Vs & Benchmark Komputasi

| Dimensi 5V | File Bukti & Sumber Kebenaran | Angka Kunci Terverifikasi (Beserta Satuan & Penyebut) | Visualisasi Rekomendasi Video | Catatan & Limitasi Metodologis |
|:---|:---|:---|:---|:---|
| **Volume** | `outputs/volume/tables/volume_summary.csv` | **13.734.415 baris** encounter; **66 kolom** runtime (58 fisik); Reduksi penyimpanan **89,98%** (5.008.326.279 bytes [4,66 GiB / 5,01 GB] $\rightarrow$ 501.710.538 bytes [478,47 MiB / 501,71 MB]); Ukuran lokal **326,32 MB** (16 partisi). | `outputs/volume/charts/volume_storage_reduction.png` | Reduksi 89,98% mencakup 3 tabel sumber (encounters, events, activities) secara kumulatif, bukan tabel encounter_analytics saja. |
| **Velocity** | `outputs/velocity/tables/simulated_micro_batches.csv` & `simulated_monthly_event_trend.csv` | Throughput simulasi mikro-batch **1,50M baris/detik** (latensi 1,17 s); Optimalitas partisi: **8 partisi** tercepat (**2,444 s** median); Tren bulanan: **~425.000 event/bulan** (Jan–Okt 2026). | `outputs/velocity/charts/velocity_partition_benchmark.png` & `velocity_monthly_event_trend.png` | *Tabel `encounter_analytics` tidak memiliki timestamp*. Dimensi waktu dievaluasi via `patient_events` (4.406.252 event bertimestamp simulasi). |
| **Variety** | `outputs/variety/tables/source_vs_target_formats.csv` | Unifikasi 3 format heterogen: CSV Tabular (Encounters, 13,73M baris), NDJSON Semi-terstruktur (Events, 4,41M baris), CSV Long-form (Activity, 8,72M baris) ke dalam 1 skema Parquet Snappy. | `outputs/variety/charts/variety_multi_source_composition.png` | Seluruh data bertanda `synthetic_flag = true` dan `data_origin = synthetic_bootstrap`. |
| **Veracity** *(Prioritas)* | `outputs/veracity/tables/missing_value_comparison.csv` & `data_integrity_validation.csv` | Sisa karakter kotor `?`: **TEPAT 0 baris (0,00%)**; Standardisasi `NULL` sejati (`weight` 96,86%, `max_glu` 94,74%, `A1C` 83,27%); Duplikat Primary Key: **0 baris**; Rentang klinis valid: **100%**. Pasien hospice/meninggal: **326.515 baris (2,38%)**. | `outputs/veracity/charts/veracity_missing_values.png` | **Pembersihan menstandarkan missing menjadi NULL, bukan mengisinya (tanpa imputasi)**. Pasien hospice berpotensi menimbulkan bias *underestimation* readmisi. |
| **Value** *(Prioritas)* | `outputs/value/tables/icd9_diagnosis_vs_readmission.csv` & `clinical_intensity_vs_readmission.csv` | **Sirkulasi (Rank 1 Volume):** 4.108.120 encounter (**29,91%**), readmit <30d: 11,46%; **Pernapasan (Rank 3):** 1.946.180 (**14,17%**); **Diabetes (Rank 5):** 1.182.906 (**8,61%**), readmit <30d: **13,00%** (**+1,83 pp vs baseline 11,17%, Risk Ratio 1,164x**); Lama rawat: **4,78 vs 4,25 hari** (Cohen's d = 0,18); Polifarmasi: **16,92 vs 15,67 obat** (Cohen's d = 0,15). | `outputs/value/charts/value_icd9_diagnosis_readmission.png` | Data bersumber dari bootstrap UCI Diabetes; hubungan durasi rawat dan polifarmasi adalah **asosiasi keparahan klinis, bukan sebab-akibat**. |
| **Computational** | `outputs/computational/tables/benchmark_op_a_vs_b.csv` & `appendix_for_segment4/` | **Op A (30 Kunci GroupBy): 1,009 s**; **Op A2 (9,65M Kunci GroupBy): ~27,5 s**; **Op B (Window PartitionBy + Sort): 43,719 s** (**43,33x lebih lambat vs Op A**). | `outputs/computational/charts/computational_benchmark_op_a_vs_b.png` | Perbedaan waktu berasal dari desain query (kardinalitas hash map + sort-based windowing), bukan ukuran data (keduanya 13,73M baris). |

---

## 2. Tabel Perubahan dari Versi Sebelumnya (Changelog Lama -> Baru)

| Parameter / Metrik | Nilai Versi Lama (v1) | Nilai Versi Baru (v2) | Alasan & Justifikasi Metodologis |
|:---|:---:|:---:|:---|
| **Definisi "Before" Veracity** | Mengutip laporan sekunder `reports/*.csv` | Dihitung ulang langsung dari file mentah `data/raw/diabetic_data.csv` (101.766 baris) | Memenuhi kriteria audit ketat: membandingkan persentase missing dataset asli UCI terhadap Parquet terkurasi. |
| **Makna Pembersihan Missing** | Disebut "pembersihan missing data" | Diperjelas: "Menstandarkan representasi ketiadaan nilai (noise format '?' $\rightarrow$ NULL sejati), BUKAN mengisi/imputasi nilai" | Menghindari miskonsepsi bahwa data kosong telah dimanipulasi atau diisi secara artifisial. |
| **Penanganan Pasien Meninggal / Hospice** | Tidak diperhitungkan secara khusus | Dihitung eksplisit (326.515 baris / 2,38%) dan diuji via Analisis Sensitivitas | Pasien meninggal/hospice secara klinis tidak mungkin readmisi; jika tidak dikeluarkan akan membiaskan readmission rate. |
| **Uji Statistik Value** | Hanya mengklaim "Chi-Square $p < 0,0001$ signifikan" | Mengganti fokus ke **Ukuran Efek (Effect Size)**: Risk Ratio (1,164x), selisih poin persen (+1,83 pp), Wilson 95% CI, Cramér's V, dan Cohen's d | Pada $N = 13,7$ juta sampel bootstrap, p-value selalu kecil secara artifisial; effect size adalah tolok ukur relevansi klinis riil. |
| **Urutan Kelompok Diagnosis** | Narasi menulis "Sirkulasi disusul Diabetes" | Diperbaiki sesuai ranking volume riil: Rank 1 Sirkulasi (29,91%), Rank 2 Other (16,24%), Rank 3 Pernapasan (14,17%), Rank 4 Pencernaan (9,32%), Rank 5 Diabetes (8,61%) | Mencegah kesalahan pelaporan peringkat: Diabetes adalah peringkat 5 volume, tetapi peringkat 1 risiko readmisi dini di antara penyakit kronis spesifik. |
| **Hubungan Lama Rawat & Obat** | Bahasa implikatif sebab-akibat | Bahasa asosiatif eksplisit ("berasosiasi dengan keparahan klinis") dengan Cohen's d (0,18 dan 0,15) | Hubungan observasional tidak dapat diklaim sebagai kausalitas. |
| **Analisis Komputasi** | Hanya membandingkan Op A vs Op B | Menambahkan kontrol **Op A2** (GroupBy 9,65 juta kunci tanpa window/sort) | Mendekomposisi penyebab lonjakan waktu: kardinalitas kunci (~27,5s) vs sort-based windowing (~43,7s). |
| **Terminologi & Path** | Terdapat kata "deterministik" dan path Windows absolut | Kata "deterministik" dihapus 100%; seluruh path diubah menjadi path relatif | Menjaga netralitas akademis dan portabilitas repositori di lingkungan Linux/HDFS. |

---

## 3. Naskah Video Segmen 5 (Voiceover Script ~154 Kata, Durasi Terukur: 1:15 Menit)

*Distribusi Durasi: Veracity 25 detik (Slide 1), Value 40 detik (Slide 2), Limitasi & Penutup 10 detik (Slide 3).*
*Total Kata: **154 kata** (Estimasi durasi pada kecepatan artikulatif 125–130 kata/menit = **72–75 detik**).*

> **[00:00 - 00:25 | Slide 1: Veracity & Kualitas Data]**  
> *"Memasuki Segmen 5, kami mengevaluasi keandalan data pada 13,73 juta rekam medis. Sebelum kurasi, dataset dipenuhi karakter tanda tanya hingga 96,8 persen pada berat badan dan 94,7 persen pada glukosa. Pipeline sukses mengeliminasi seluruh noise tersebut menjadi NULL sejati, menstandarkan missing tanpa mengisinya. Integritas primary key terjamin sempurna dengan nol duplikat encounter ID dan seluruh rentang klinis valid."*

> **[00:25 - 00:55 | Slide 2: Healthcare Value & Analisis Readmisi]**  
> *"Dari sisi Value, analisis ICD-9 membuktikan penyakit Sirkulasi mendominasi populasi rawat inap sebesar 29,91 persen, diikuti kategori Other dan Pernapasan. Sementara itu, Diabetes mencatat risiko readmisi dini tertinggi di antara diagnosis kronis spesifik, yakni 13,00 persen, melampaui baseline populasi 11,17 persen. Pasien dengan readmisi dini berasosiasi dengan durasi rawat lebih lama—rata-rata 4,78 hari—dan polifarmasi 16,92 jenis obat. Temuan ini terbukti konsisten pada analisis sensitivitas data asli UCI maupun eksklusi pasien hospice."*

> **[00:55 - 01:15 | Slide 3: Limitasi & Kesimpulan]**  
> *"Sebagai limitasi: dataset ini merupakan bootstrap sintetis tanpa timestamp longitudinal, sehingga temuan ini mencerminkan asosiasi klinis, bukan kausalitas riil. Sekian presentasi Segmen 5, terima kasih."*

---

## 4. Panduan Menjalankan Seluruh Modul Kembali (Reproducibility)

Untuk menjalankan ulang seluruh pengujian end-to-end:

```powershell
# Di root repository
cd "d:\Kuliah\Semester 7\BIG_DATA\UTS\Chronic-Disease-Population-Analytics"

# 1. Jalankan semua modul analitis
analytical\.venv\Scripts\python analytical/run_all.py

# 2. Hasilkan 3 slide presentasi beresolusi 1080p
analytical\.venv\Scripts\python analytical/common/slide_generator.py

# 3. Jalankan verifikasi otomatis ledger klaim
analytical\.venv\Scripts\python analytical/verify_claims.py

# 4. Jalankan demo prototipe (opsional)
analytical\.venv\Scripts\python analytical/demo/segment5_demo.py
```
