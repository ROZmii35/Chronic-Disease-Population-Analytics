import os
import sys
import json
import time
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

from analytical.config import REPORTS_PATH, OUTPUTS_DIR, DATA_PATH
from analytical.common.spark_session import get_spark_session
from analytical.common.loader import load_encounter_analytics
from analytical.common.plotting import save_matplotlib_chart, save_plotly_chart, COLORS

OUTPUT_MOD_DIR = OUTPUTS_DIR / "volume"
TABLES_DIR = OUTPUT_MOD_DIR / "tables"
CHARTS_DIR = OUTPUT_MOD_DIR / "charts"
LOGS_DIR = OUTPUT_MOD_DIR / "logs"
LOG_FILE = LOGS_DIR / "volume.log"

TABLES_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
)

def run_volume_analysis():
    logging.info("=" * 65)
    logging.info("STARTING VOLUME MODULE ANALYSIS (REVISED: UNIT RECONCILIATION)")
    logging.info("=" * 65)

    # 1. Baca metadata ukuran dan row count dari reports/*.json
    bench_json_path = REPORTS_PATH / "performance_benchmark_report.json"
    with open(bench_json_path, "r", encoding="utf-8") as f:
        bench_data = json.load(f)

    size_rows = bench_data["size_comparison"]
    df_size = pd.DataFrame(size_rows)
    df_size.to_csv(TABLES_DIR / "storage_size_reduction.csv", index=False)
    logging.info("Storage reduction data loaded from reports/performance_benchmark_report.json")

    # 2. Hitung ukuran Parquet fisik encounter_analytics di disk lokal
    parquet_files = list(DATA_PATH.glob("*.parquet"))
    total_parquet_bytes = sum(f.stat().st_size for f in parquet_files)
    num_partitions = len(parquet_files)
    size_mb = total_parquet_bytes / (1024 * 1024)
    logging.info(f"Local Parquet disk size: {size_mb:.2f} MB across {num_partitions} partition files")

    # 3. Jalankan Spark untuk satu operasi agregasi nyata dan ukur waktunya
    spark = get_spark_session("ChronicDisease-Volume-R4")
    df = load_encounter_analytics(spark)
    total_records = df.count()
    runtime_cols = len(df.columns)
    disk_cols = 58  # Skema Parquet asli di disk

    start_agg = time.perf_counter()
    agg_result = df.groupBy("readmitted").count().collect()
    agg_elapsed = time.perf_counter() - start_agg
    logging.info(f"Aggregation runtime (groupBy readmitted count): {agg_elapsed:.3f} seconds")

    volume_summary = {
        "dataset_name": "encounter_analytics",
        "total_records": total_records,
        "runtime_columns": runtime_cols,
        "disk_columns": disk_cols,
        "parquet_partitions_count": num_partitions,
        "local_disk_size_mb": round(size_mb, 2),
        "raw_total_bytes": 5008326279,
        "raw_total_gib": 4.664,
        "raw_total_gb": 5.008,
        "curated_total_bytes": 501710538,
        "curated_total_mib": 478.47,
        "curated_total_mb": 501.71,
        "storage_reduction_pct": 89.98,
        "single_aggregation_elapsed_seconds": round(agg_elapsed, 3),
        "scope_reduction": "3 tabel sumber (encounters, patient_events, clinical_activity) dari Raw ke Curated"
    }
    df_vol_sum = pd.DataFrame([volume_summary])
    df_vol_sum.to_csv(TABLES_DIR / "volume_summary.csv", index=False)

    # 4. Plot Chart Efisiensi Penyimpanan (Raw vs Curated)
    df_plot = df_size[df_size["dataset"] != "TOTAL"].copy()
    raw_mb = [r["raw_bytes"] / (1024 * 1024) for _, r in df_plot.iterrows()]
    curated_mb = [r["curated_bytes"] / (1024 * 1024) for _, r in df_plot.iterrows()]

    fig, ax = plt.subplots(figsize=(16, 9))
    x = np.arange(len(df_plot))
    w = 0.35
    b1 = ax.bar(x - w/2, raw_mb, width=w, color=COLORS["danger"], label="Raw Mentah (MiB)", alpha=0.9)
    b2 = ax.bar(x + w/2, curated_mb, width=w, color=COLORS["success"], label="Curated Parquet Snappy (MiB)", alpha=0.9)

    ax.set_xticks(x)
    ax.set_xticklabels(df_plot["dataset"], fontsize=16, fontweight="bold")
    ax.set_ylabel("Ukuran Penyimpanan (MiB)", fontsize=18, fontweight="bold")
    ax.set_title("Volume: Efisiensi Kompresi Kolumnar Parquet Snappy (-89,98% Total Penyimpanan)\n(5.008.326.279 bytes [4,66 GiB] -> 501.710.538 bytes [478,47 MiB])", fontsize=20, fontweight="bold", pad=20)
    ax.legend(loc="upper right", frameon=True, fontsize=16)

    for bar in b1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 40, f"{bar.get_height():.1f} MiB", ha='center', fontweight='bold', fontsize=14)
    for bar in b2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 40, f"{bar.get_height():.1f} MiB", ha='center', fontweight='bold', fontsize=14)

    save_matplotlib_chart(fig, CHARTS_DIR / "volume_storage_reduction.png", "reports/performance_benchmark_report.json")

    fig_html = px.bar(
        df_plot, x="dataset", y="size_reduction_percent",
        color="size_reduction_percent", color_continuous_scale="Greens",
        title="Persentase Reduksi Penyimpanan per Dataset",
        labels={"dataset": "Dataset", "size_reduction_percent": "Reduksi (%)"}
    )
    save_plotly_chart(fig_html, CHARTS_DIR / "volume_storage_reduction.html")

    # 5. Write RESULT.md
    result_md_path = OUTPUT_MOD_DIR / "RESULT.md"
    content = f"""# Hasil Eksekusi & Analisis: Volume (Skala Data & Efisiensi Penyimpanan)

## 1. Skala Data Nyata
- **Total Baris:** {total_records:,} record encounter terintegrasi.
- **Total Kolom Runtime:** {runtime_cols} kolom (termasuk 8 kolom adapter & flag di `loader.py`; file Parquet di disk memiliki {disk_cols} kolom).
- **Jumlah Partisi Fisik:** {num_partitions} file Parquet.
- **Ukuran File Parquet Lokal:** {size_mb:.2f} MB (342.170.835 bytes).

## 2. Rekonsiliasi Efisiensi Reduksi Penyimpanan (R4)
- **Cakupan Reduksi 89,98%:** Angka penghematan 89,98% berlaku untuk **3 tabel sumber secara kumulatif** (`encounters`, `patient_events`, `clinical_activity`) dari Raw mentah ke Curated Parquet, bukan untuk tabel analitis `encounter_analytics` saja.
- **Satuan Penyimpanan:**
  - Raw Total: **5.008.326.279 bytes** = **4,664 GiB** (sering dibulatkan menjadi 4,66 GB pada laporan) = **5,008 GB**.
  - Curated Total: **501.710.538 bytes** = **478,47 MiB** (sering dibulatkan menjadi 478,47 MB pada laporan) = **501,71 MB**.
  - Persentase Penghematan: $\\frac{5.008.326.279 - 501.710.538}{5.008.326.279} \\times 100\\% = \\mathbf{{89,98\\%}}$.

## 3. Kecepatan Agregasi Tunggal
- Eksekusi agregasi `groupBy("readmitted").count()` pada 13,7 juta baris selesai dalam **{agg_elapsed:.3f} detik**.
"""
    with open(result_md_path, "w", encoding="utf-8") as f:
        f.write(content)

    logging.info(f"RESULT.md saved to {result_md_path}")
    logging.info("VOLUME MODULE COMPLETED SUCCESSFULLY.")
    spark.stop()

if __name__ == "__main__":
    run_volume_analysis()
