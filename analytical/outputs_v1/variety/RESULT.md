# Hasil Eksekusi & Analisis: Variety (Heterogenitas Sumber Data)

## 1. Integrasi Format Sumber Beragam (*Variety*)
Pipeline berhasil menggabungkan 3 format data berbeda:
1. **Encounters:** CSV tabular standar (13.734.415 baris).
2. **Patient Events:** JSON Lines / NDJSON semi-terstruktur (4.406.252 baris event log).
3. **Clinical Activity:** CSV format vertikal / long unpivoted (8.723.515 baris).

Semua format tersebut disatukan secara terkurasi ke dalam satu tabel relasional tunggal **Parquet Snappy Columnar (`encounter_analytics`)** dengan total 66 atribut terstandardisasi.

---

## 2. Pelacakan Provenance & Silsilah Data
- **`data_origin`:** `synthetic_bootstrap`.
- **`synthetic_flag`:** Seluruh baris bernilai `true` (menandai integritas data audit bahwa seluruh baris adalah data hasil simulasi terkontrol).
