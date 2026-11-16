# Hasil Eksekusi & Analisis: Computational Complexity Benchmark

Eksperimen ini membuktikan bahwa perbedaan performa komputasi Big Data **berasal dari desain query (query execution design)**, bukan ukuran dataset, karena ketiganya dieksekusi pada dataset yang persis sama (**13.734.415 baris**).

---

## 1. Ringkasan Hasil Benchmark (1 Warm-up + Pengujian Berulang)

| Parameter | Operation A (Simple GroupBy) | Operation A2 (Control High-Card) | Operation B (Partitioned Window & Sort) |
|:---|:---:|:---:|:---:|
| **Deskripsi Query** | `groupBy("age", "readmitted").count()` | `groupBy("patient_nbr").count()` | `Window.partitionBy("patient_nbr") + Sort` |
| **Kardinalitas Kunci** | **30 kelompok** | **9.650.229 kelompok** | **9.650.229 kelompok** |
| **Kebutuhan Sorting** | **TIDAK** | **TIDAK** | **YA (Sort patient_nbr ASC)** |
| **Waktu Minimum** | **0.875 s** | **8.242 s** | **39.202 s** |
| **Waktu Median** | **1.009 s** | **27.515 s** | **43.719 s** |
| **Waktu Maksimum** | **1.325 s** | **29.734 s** | **73.979 s** |
| **Rata-rata $\pm$ Std** | 1.031 $\pm$ 0.158 s | 21.830 $\pm$ 9.651 s | 52.300 $\pm$ 15.440 s |
| **Rasio Relatif vs Op A** | **1,00x** (Baseline) | **27.27x** | **43.33x** |

---

## 2. Penjelasan Kausalitas Berbasis Bukti Physical Execution Plan

Berdasarkan `explain("formatted")` resmi Apache Spark:

1. **Operation A (HashAggregate Ringan):**
   - Rencana fisik menggunakan operator `HashAggregate(keys=[age, readmitted], functions=[count(1)])`.
   - Karena kardinalitas kunci sangat kecil (30 kombinasi unik), seluruh agregasi selesai di dalam cache L1/L2 memori per partisi (*in-memory aggregation*). Shuffle exchange pada 8 partisi hanya mentransfer 30 baris agregat parsial. Runtime: **1.009 detik**.
2. **Operation A2 (HashAggregate Skala Masif - Kontrol):**
   - Rencana fisik tetap menggunakan `HashAggregate`, namun harus mengalokasikan memori untuk **9.650.229 kelompok pasien unik**.
   - Ini membuktikan dampak murni dari **kardinalitas kunci**: runtime naik menjadi **27.515 detik (27.27x vs Op A)** akibat overhead hashing, manajemen memori hash map, dan transfer shuffle partition pada 9,65 juta kunci unik, *meskipun tanpa operasi sorting*.
3. **Operation B (Exchange Hash Partitioning + Sort-based Window):**
   - Rencana fisik memicu operator `WindowExec`, yang mewajibkan Spark menyisipkan `Exchange hashpartitioning(patient_nbr, 8)` dan **`Sort [patient_nbr ASC NULLS FIRST]`**.
   - Mengurutkan 13,7 juta record untuk 9,65 juta kunci unik memicu CPU cycle masif untuk multi-way merge sort.
   - **Verifikasi Spill:** Pada konfigurasi alokasi driver 4 GB dan 8 shuffle partitions, eksekusi selesai tanpa *spill to disk*; lonjakan waktu murni berasal dari overhead network exchange dan perankingan sort pada memori. Runtime mencapai **43.719 detik (43.33x vs Op A)**.

---

## 3. Kesimpulan untuk Tim Big Data Developer (Segmen 4)
Hasil ini membuktikan prinsip optimasi Spark:
- Query analitis yang dapat diselesaikan dengan `HashAggregate` berpartisi kecil harus selalu diprioritaskan daripada operasi analitis berbasis window sorting.
- Kardinalitas kunci partisi (`patient_nbr`) adalah determinan utama kebutuhan memori shuffle.
