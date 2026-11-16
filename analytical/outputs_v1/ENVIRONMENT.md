# Spesifikasi Lingkungan Eksekusi (ENVIRONMENT.md)

Dokumen ini mencatat konfigurasi perangkat keras, sistem operasi, dan runtime Apache Spark yang digunakan untuk menghasilkan seluruh metrik analitis di modul `analytical/`.

---

## 1. Konfigurasi Sistem Operasi & Perangkat Keras
- **Sistem Operasi:** Microsoft Windows 11 / Windows NT 10.0 x86_64
- **Processor:** 8 Logical CPU Cores
- **Memori RAM Fisik:** 16 GB DDR4
- **Storage Subsystem:** Local NVMe SSD (NTFS Filesystem)
- **Hadoop Binaries:** Apache Hadoop 3.3.5 Native Windows Binaries (`winutils.exe` & `hadoop.dll` pada `analytical/hadoop/bin`)

---

## 2. Runtime & Pustaka Perangkat Lunak
- **Python:** 3.9.13 (Virtual Environment: `analytical/.venv`)
- **Java Virtual Machine:** Java SE Runtime Environment (build 1.8.0_202-b08, 64-Bit Server VM)
- **Apache Spark:** 3.5.1 (PySpark)
- **Komponen Pendukung:**
  - `pandas`: 2.2.2
  - `pyarrow`: 16.1.0
  - `numpy`: 1.26.4
  - `scipy`: 1.13.1
  - `matplotlib`: 3.8.4
  - `plotly`: 5.22.0
  - `pytest`: 8.2.2

---

## 3. Parameter Konfigurasi SparkSession
- **Master URL:** `local[*]` (Memanfaatkan 8 thread core paralel)
- **Driver Memory (`spark.driver.memory`):** `4g` (4 Gigabytes)
- **Shuffle Partitions (`spark.sql.shuffle.partitions`):** `8` (Sesuai hasil benchmark optimal Data Engineer)
- **Kryo Serializer:** Default Java Serialization
- **Parquet Compression Codec:** `snappy`
