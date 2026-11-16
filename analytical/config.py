import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
ANALYTICAL_DIR = Path(__file__).resolve().parent

DATA_PATH = BASE_DIR / "data" / "curated" / "parquet" / "encounter_analytics"
REPORTS_PATH = BASE_DIR / "reports"
OUTPUTS_DIR = ANALYTICAL_DIR / "outputs"

# Verifikasi keberadaan dataset utama
if not DATA_PATH.exists():
    raise FileNotFoundError(
        f"[FATAL] Dataset utama tidak ditemukan di {DATA_PATH}! "
        "Pastikan data/curated/parquet/encounter_analytics tersedia sebelum melanjutkan."
    )

# Spark Configuration (Single-Node Optimal based on 8 Cores, 8 GB RAM)
SPARK_MASTER = "local[*]"
SPARK_APP_NAME = "ChronicDisease-Population-Analytics-Segment5"
SPARK_DRIVER_MEMORY = "4g"
SHUFFLE_PARTITIONS = 8  # Terbukti tercepat di partition_benchmark.csv (median 2.444s)
PARQUET_COMPRESSION = "snappy"

# Output subdirectories
SUBDIRS = ["volume", "velocity", "variety", "veracity", "value", "computational"]
for sub in SUBDIRS:
    (OUTPUTS_DIR / sub / "tables").mkdir(parents=True, exist_ok=True)
    (OUTPUTS_DIR / sub / "charts").mkdir(parents=True, exist_ok=True)
    (OUTPUTS_DIR / sub / "logs").mkdir(parents=True, exist_ok=True)
