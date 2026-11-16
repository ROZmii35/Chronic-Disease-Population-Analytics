# Hasil Eksekusi & Interpretasi: Value (Wawasan Kesehatan & Efek Klinis)

Modul ini menyajikan wawasan klinis (*Value*) dari dataset `encounter_analytics` (13.734.415 rekam medis). Karena ukuran sampel sangat besar ($N = 13,7$ juta) menyebabkan $p$-value uji signifikansi hampir selalu $< 0,0001$, analisis difokuskan pada **ukuran efek (*Effect Size*)**: Risk Ratio (RR), selisih poin persen (pp), Wilson 95% Confidence Interval, Cramér's V, dan Cohen's d.

Baseline readmisi <30 hari keseluruhan populasi adalah **11.17%** (1,533,838 dari 13,734,415 encounter).

---

## 1. Urutan Lengkap Kelompok Diagnosis ICD-9 Primer vs Readmisi

| Rank Volume | Kategori Diagnosis ICD-9 | Total Encounter | % Populasi | Readmit <30d (%) | Selisih pp vs Baseline | Risk Ratio (RR) | 95% Wilson CI | Total Readmit (%) |
|:---:|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | **Circulatory** | 4,108,120 | 29.91% | **11.46%** | +0.29 pp | 1.026x | [11.43%, 11.49%] | 47.09% |
| 2 | **Other** | 2,232,958 | 16.26% | **11.02%** | -0.15 pp | 0.987x | [10.98%, 11.06%] | 45.58% |
| 3 | **Respiratory** | 1,946,180 | 14.17% | **9.72%** | -1.45 pp | 0.870x | [9.68%, 9.76%] | 48.10% |
| 4 | **Digestive** | 1,276,378 | 9.29% | **10.78%** | -0.39 pp | 0.965x | [10.72%, 10.83%] | 46.25% |
| 5 | **Diabetes** | 1,182,906 | 8.61% | **13.00%** | +1.83 pp | 1.164x | [12.94%, 13.06%] | 50.92% |
| 6 | **Injury** | 941,254 | 6.85% | **12.25%** | +1.08 pp | 1.097x | [12.18%, 12.31%] | 44.34% |
| 7 | **Genitourinary** | 689,572 | 5.02% | **10.83%** | -0.34 pp | 0.970x | [10.75%, 10.90%] | 44.39% |
| 8 | **Musculoskeletal** | 667,838 | 4.86% | **9.47%** | -1.70 pp | 0.848x | [9.40%, 9.54%] | 39.20% |
| 9 | **Neoplasms** | 463,057 | 3.37% | **10.06%** | -1.11 pp | 0.901x | [9.97%, 10.15%] | 33.35% |
| 10 | **Other (V/E code)** | 223,364 | 1.63% | **16.20%** | +5.03 pp | 1.451x | [16.05%, 16.36%] | 50.20% |
| 11 | **Unknown** | 2,788 | 0.02% | **23.28%** | +12.11 pp | 2.085x | [21.75%, 24.88%] | 42.58% |

> **Catatan Uji Statistik:** Uji Chi-Square kontingensi (11 diagnosis $\times$ 3 status readmisi: `<30`, `>30`, `NO`) menghasilkan $\chi^2 = 81703.32$ ($p < 0,0001$), dengan ukuran efek Cramér's V = **0.0545**. Pada $N = 13,7$ juta observasi bootstrap, p-value sangat kecil tidak menjamin signifikansi klinis substantif; interpretasi klinis bertumpu pada selisih proporsi riil.

### Temuan Kunci Diagnosis:
1. **Penyakit Sirkulasi / Kardiovaskular (Rank 1 Volume):** Mendominasi populasi dengan **4.108.120 encounter (29,91%)**, dengan tingkat readmisi <30 hari **11,46%** (sedikit di atas baseline: +0,29 pp, RR 1,026x). Kardiovaskular adalah beban operasional terbesar rumah sakit, tetapi bukan persentase readmisi tertinggi.
2. **Kategori 'Other' (Rank 2 Volume):** Mencakup **2.229.864 encounter (16,24%)**, readmisi <30 hari 11,01%.
3. **Penyakit Pernapasan (Rank 3 Volume):** Mencakup **1.946.180 encounter (14,17%)**, readmisi <30 hari 9,72% (-1,45 pp di bawah baseline).
4. **Penyakit Pencernaan (Rank 4 Volume):** Mencakup **1.279.796 encounter (9,32%)**, readmisi <30 hari 10,71%.
5. **Diabetes (Kode 250.xx, Rank 5 Volume):** Mencakup **1.182.906 encounter (8,61%)**, mencatat tingkat readmisi <30 hari tertinggi di antara diagnosis kronis spesifik yaitu **13,00%** (**+1,83 pp di atas baseline, Risk Ratio 1,164x**).

---

## 2. Stratifikasi Risiko Berdasarkan Kelompok Usia

- **Berdasarkan Volume:** Lansia mendominasi rawat inap:
  - Rank 1: `[70-80)` tahun = **3.516.733 encounter (25,61%)**.
  - Rank 2: `[60-70)` tahun = **3.037.743 encounter (22,12%)**.
  - Rank 3: `[50-60)` tahun = **2.330.158 encounter (16,97%)**.
  - Rank 4: `[80-90)` tahun = **2.320.655 encounter (16,90%)**.
- **Berdasarkan Readmisi <30 Hari:**
  - **Tertinggi:** Kelompok usia muda `[20-30)` (14,23%, +3,06 pp) dan lansia sangat tua `[80-90)` (12,08%, +0,91 pp) serta `[70-80)` (11,76%, +0,59 pp).
  - **Terendah:** Anak-anak `[0-10)` (1,89%, -9,28 pp) dan remaja `[10-20)` (5,80%, -5,37 pp).

---

## 3. Analisis Sensitivitas (Ketahanan Kesimpulan)

Tiga skenario pengujian sensitivitas dilakukan untuk memastikan temuan tidak terdistorsi oleh pengulangan pasien atau pasien meninggal:

| Skenario Pengujian | Jumlah Baris ($N$) | Baseline Readmit <30d | Circulatory (Vol % / Readmit %) | Diabetes (Vol % / Readmit %) | Status Ketahanan |
|:---|:---:|:---:|:---:|:---:|:---:|
| **1. Baseline Curated Sintetis** | 13.734.415 | 11,17% | 29,91% / 11,46% | 8,61% / 13,00% | Acuan Analisis |
| **2. Dataset UCI Asli** | 101.766 | 11,16% | 29,91% / 11,45% | 8,61% / 12,98% | **KONSISTEN SEMPURNA** (Arah dan ranking identik) |
| **3. Encounter Pertama per Pasien** | 9.650.229 | 8,98% | 29,74% / 9,14% | 8,64% / 10,72% | **KONSISTEN** (Ranking identik; readmisi lebih rendah pada kunjungan perdana) |
| **4. Eksklusi Pasien Meninggal/Hospice** | 13.407.900 | 11,40% | 29,87% / 11,69% | 8,61% / 13,27% | **KONSISTEN** (Readmisi naik +0,23 pp akibat eliminasi bias pasien tidak berisiko) |

Kesimpulan: **Arah hubungan asosiasi dan urutan ranking diagnosis terbukti stabil dan robust di semua skenario sensitivitas.**

---

## 4. Intensitas Klinis: Lama Rawat (LOS) & Jumlah Obat (Cohen's d)

Perbandingan langsung kelompok pasien readmisi dini (`<30d`, $n=1.533.838$) terhadap kelompok non-readmisi (`NO`, $n=7.400.305$):
1. **Lama Rawat Inap (LOS):** Rata-rata 4,78 hari ($\pm 3,03$) pada kelompok <30d vs 4,25 hari ($\pm 2,97$) pada kelompok NO. Selisih: **+0,52 hari**, dengan ukuran efek **Cohen's d = 0.18** (*small effect*).
2. **Polifarmasi (Jumlah Obat):** Rata-rata 16,92 jenis obat ($\pm 8,11$) pada kelompok <30d vs 15,67 jenis obat ($\pm 8,43$) pada kelompok NO. Selisih: **+1,25 jenis obat**, dengan ukuran efek **Cohen's d = 0.15** (*small effect*).

> **Catatan Metodologis:** Hubungan ini adalah **asosiasi observasional**, bukan hubungan sebab-akibat (*causality*). Lama rawat dan polifarmasi mencerminkan tingkat keparahan penyakit (*disease acuity*) dan komorbiditas pasien.
