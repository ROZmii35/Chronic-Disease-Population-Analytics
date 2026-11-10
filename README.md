# Chronic Disease Population Analytics

## Gambaran Umum Proyek

**Chronic Disease Population Analytics** merupakan proyek Big Data yang menerapkan alur kerja rekayasa data (*Data Engineering*) menggunakan Apache Hadoop HDFS dan Apache Spark.

Pada proyek ini, bagian Data Engineering bertanggung jawab atas proses penyimpanan, pemeriksaan kualitas, transformasi, penggabungan, dan pengujian performa data sebelum diserahkan kepada bagian Data Analytics untuk dianalisis lebih lanjut.

## Progres Pengerjaan Data Engineering

Berikut adalah tahapan yang telah diselesaikan:

| Script | Proses | Status |
|---|---|---|
| 10 | Pengujian Integrasi Spark dengan HDFS | Selesai |
| 11 | Validasi Data Mentah | Berhasil |
| 12 | Transformasi Data Raw ke Curated | Berhasil |
| 13 | Penggabungan Data | Berhasil |
| 14 | Validasi Hasil Penggabungan Data | Berhasil |
| 15 | Pengujian Performa | Berhasil |

## Informasi Dataset

Proyek ini menggunakan tiga jenis dataset sintetis (data buatan atau simulasi), yaitu:

1. **Encounters:** berisi informasi kunjungan atau pemeriksaan pasien.
2. **Patient Events:** berisi informasi aktivitas atau kejadian yang disimulasikan selama proses pelayanan pasien.
3. **Clinical Activity:** berisi informasi aktivitas klinis yang dibentuk dari atribut data kunjungan.

Data sintetis tersebut dibuat berdasarkan struktur dan atribut dari dataset UCI Diabetes 130-US Hospitals. Dataset yang digunakan dalam proyek ini bukan merupakan data pasien asli.

**Catatan:** File dataset sintetis tidak disertakan dalam repository GitHub karena ukuran penyimpanannya cukup besar.

## Tahapan Pengolahan Data

Proses pengolahan data dalam proyek ini terdiri dari beberapa tahapan berikut:

1. **Penyimpanan Data Mentah (Raw Data Storage):** menyimpan ketiga dataset sintetis ke dalam Hadoop Distributed File System (HDFS).
2. **Validasi Data (Data Validation):** memeriksa jumlah baris data, ID duplikat, ID kosong, serta beberapa aturan konsistensi data.
3. **Transformasi Data (Data Transformation):** mengubah data berformat CSV dan JSONL menjadi format Parquet dengan kompresi Snappy.
4. **Penggabungan Data (Data Integration):** menggabungkan data Encounters dengan hasil agregasi Patient Events dan Clinical Activity.
5. **Validasi Hasil Penggabungan (Integration Validation):** memeriksa jumlah data, ID duplikat, nilai kosong, dan kesesuaian hasil agregasi.
6. **Pengujian Performa (Performance Benchmark):** mengukur pengurangan ukuran penyimpanan, performa pembacaan data Analytics, dan perbandingan konfigurasi partisi Spark.

## Struktur Penyimpanan Data di HDFS

Berikut adalah struktur direktori penyimpanan data yang digunakan dalam proyek:

```text
/chronic-disease-analytics/
├── raw/
│   ├── encounters/
│   ├── patient_events/
│   └── clinical_activity/
│
├── curated/
│   ├── encounters/
│   ├── patient_events/
│   └── clinical_activity/
│
├── analytics/
│   └── encounter_analytics/
│
└── reports/
```

Keterangan:

- **raw:** menyimpan dataset mentah sebelum proses transformasi.
- **curated:** menyimpan dataset yang telah melalui proses validasi dan transformasi ke format Parquet.
- **analytics:** menyimpan hasil penggabungan data yang siap digunakan untuk analisis lebih lanjut.
- **reports:** menyimpan laporan hasil validasi, integrasi, dan pengujian performa.

## Hasil Pengerjaan Data Engineering

Berdasarkan proses pengolahan dan pengujian yang telah dilakukan, diperoleh hasil sebagai berikut:

| Indikator | Hasil |
|---|---:|
| Total Ukuran Data Mentah (Raw) | 4,66 GB |
| Total Ukuran Data Hasil Transformasi (Curated) | 478,47 MB |
| Pengurangan Ukuran Penyimpanan | 89,98% |
| Jumlah Data Hasil Integrasi | 13.734.415 baris |
| Jumlah Pasien Unik pada Encounters | 9.650.000 |
| Konfigurasi Partisi Tercepat dari Pengujian | 8 partisi |
| Median Waktu Agregasi dengan 8 Partisi | 2,444 detik |

## Dataset Utama untuk Data Analytics

Hasil utama dari proses Data Engineering adalah dataset bernama **encounter_analytics**.

Lokasi penyimpanannya di HDFS:

```text
/chronic-disease-analytics/analytics/encounter_analytics
```

Informasi dataset:

- Format penyimpanan: Parquet.
- Metode kompresi: Snappy.
- Jumlah data: 13.734.415 baris.

Dataset ini berisi informasi kunjungan pasien yang telah dilengkapi dengan hasil penghitungan agregasi aktivitas dari Patient Events dan Clinical Activity.

Dataset tersebut menjadi sumber data utama yang dapat digunakan oleh bagian Data Analytics untuk melakukan proses analisis lebih lanjut.

## Laporan Hasil Pengolahan

Folder `reports/` berisi beberapa laporan yang dihasilkan selama proses Data Engineering, yaitu:

- `data_validation_report.json`: laporan hasil pemeriksaan kualitas data mentah.
- `data_integration_report.json`: laporan hasil proses penggabungan data.
- `integration_validation_report.json`: laporan hasil validasi dataset yang telah digabungkan.
- `performance_benchmark_report.json`: laporan hasil pengujian performa pengolahan data.
- `partition_benchmark.csv`: hasil perbandingan waktu pengolahan menggunakan konfigurasi partisi Spark yang berbeda.

Laporan tersebut dapat digunakan untuk mengetahui hasil pemeriksaan, proses pengolahan, dan performa sistem.

## Penyerahan Data kepada Bagian Data Analytics

Setelah seluruh tahapan Data Engineering selesai, dataset utama yang akan digunakan oleh bagian Data Analytics adalah:

```text
/chronic-disease-analytics/analytics/encounter_analytics
```

Bagian Data Analytics dapat menggunakan dataset tersebut untuk melanjutkan proses analisis, seperti:

1. Analisis distribusi diagnosis atau penyakit.
2. Analisis jumlah pasien berdasarkan kelompok usia.
3. Analisis kunjungan kembali pasien (*readmission*).
4. Analisis aktivitas laboratorium dan pengobatan.
5. Analisis jumlah kunjungan berdasarkan waktu.
6. Pembuatan hasil analisis, grafik, dan visualisasi data.

Dengan demikian, bagian Data Analytics tidak perlu mengulang proses validasi, transformasi, dan penggabungan data yang telah diselesaikan oleh bagian Data Engineering.

**Catatan akses:** Dataset saat ini tersimpan di HDFS pada komputer yang digunakan untuk proses Data Engineering. Anggota tim yang menggunakan komputer berbeda perlu memperoleh file Parquet hasil pengolahan atau mendapatkan akses ke penyimpanan HDFS tersebut sebelum menjalankan analisis.

## Catatan Penting

- Dataset sintetis tidak disertakan dalam repository GitHub karena ukuran filenya besar.
- Script Python dan laporan hasil pengolahan disediakan sebagai dokumentasi proses Data Engineering.
- Untuk menjalankan ulang seluruh proses, dataset sumber sintetis harus diperoleh atau dibuat kembali terlebih dahulu.
- Proyek ini dijalankan menggunakan lingkungan Hadoop dan Spark dengan satu komputer (*single-node*).
- Hasil pengujian performa tidak dapat dianggap sebagai hasil pengujian skalabilitas cluster dengan banyak komputer.
- Seluruh hasil analisis berdasarkan dataset sintetis sehingga tidak boleh dianggap sebagai temuan medis dari data pasien sebenarnya.