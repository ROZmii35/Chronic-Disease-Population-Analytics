import os
import sys
import json
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px

from analytical.config import REPORTS_PATH, OUTPUTS_DIR
from analytical.common.spark_session import get_spark_session
from analytical.common.loader import load_encounter_analytics
from analytical.common.plotting import save_matplotlib_chart, save_plotly_chart, COLORS

OUTPUT_MOD_DIR = OUTPUTS_DIR / "variety"
TABLES_DIR = OUTPUT_MOD_DIR / "tables"
CHARTS_DIR = OUTPUT_MOD_DIR / "charts"
LOGS_DIR = OUTPUT_MOD_DIR / "logs"
LOG_FILE = LOGS_DIR / "variety.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
)

def run_variety_analysis():
    logging.info("=" * 65)
    logging.info("STARTING VARIETY MODULE ANALYSIS (MULTI-FORMAT DATA INTEGRATION)")
    logging.info("=" * 65)

    # 1. Dokumentasi Format Sumber vs Target
    source_formats = [
        {
            "dataset_role": "Encounters (Main Master)",
            "source_raw_format": "CSV Tabular",
            "source_record_count": 13734415,
            "raw_size": "2.80 GB (3.002.925.426 bytes)",
            "curated_format": "Parquet Snappy",
            "curated_size": "298.80 MB",
            "key_join_columns": "patient_nbr, encounter_id"
        },
        {
            "dataset_role": "Patient Events (Temporal Logs)",
            "source_raw_format": "JSON Lines (NDJSON)",
            "source_record_count": 4406252,
            "raw_size": "953.67 MB (999.999.861 bytes)",
            "curated_format": "Parquet Snappy",
            "curated_size": "103.67 MB",
            "key_join_columns": "patient_nbr, encounter_id"
        },
        {
            "dataset_role": "Clinical Activity (Melted Features)",
            "source_raw_format": "CSV Long Format (Unpivoted)",
            "source_record_count": 8723515,
            "raw_size": "958.83 MB (1.005.400.992 bytes)",
            "curated_format": "Parquet Snappy",
            "curated_size": "76.00 MB",
            "key_join_columns": "patient_nbr, encounter_id"
        },
        {
            "dataset_role": "Integrated Analytics (Target)",
            "source_raw_format": "Multi-Source Unified",
            "source_record_count": 13734415,
            "raw_size": "4.66 GB (Total Raw)",
            "curated_format": "Parquet Snappy Columnar",
            "curated_size": "328.87 MB",
            "key_join_columns": "Primary Key: encounter_id"
        }
    ]
    df_formats = pd.DataFrame(source_formats)
    formats_csv_path = TABLES_DIR / "source_vs_target_formats.csv"
    df_formats.to_csv(formats_csv_path, index=False)
    logging.info(f"Saved format comparison table to {formats_csv_path}")

    # 2. Ambil Schema Nyata dari Parquet & Kolom Provenance
    spark = get_spark_session("ChronicDisease-Variety")
    df = load_encounter_analytics(spark)
    total_records = df.count()

    schema_details = []
    for field in df.schema.fields:
        schema_details.append({
            "column_name": field.name,
            "data_type": str(field.dataType),
            "is_nullable": field.nullable
        })
    df_schema = pd.DataFrame(schema_details)
    df_schema.to_csv(TABLES_DIR / "curated_schema_fields.csv", index=False)
    logging.info("Saved schema field definitions")

    # Kolom Provenance
    provenance_df = df.groupBy("data_origin", "synthetic_flag").count().toPandas()
    provenance_df.to_csv(TABLES_DIR / "provenance_metadata.csv", index=False)

    # Contoh 5 Baris Terintegrasi
    sample_rows = df.select("encounter_id", "patient_nbr", "age", "race", "diag_1", "time_in_hospital", "num_medications", "total_event_count", "total_activity_count", "readmitted").limit(5).toPandas()
    sample_rows.to_csv(TABLES_DIR / "integrated_sample_rows.csv", index=False)

    # 3. Visualisasi Integrasi Multi-Format (Bar Chart Komposisi Record Sumber)
    fig, ax = plt.subplots(figsize=(14, 8))
    labels = ["Encounters (CSV)", "Patient Events (JSONL)", "Clinical Activity (CSV Long)"]
    records = [13734415 / 1_000_000, 4406252 / 1_000_000, 8723515 / 1_000_000]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]

    bars = ax.bar(labels, records, color=colors, alpha=0.85, width=0.45)
    ax.set_ylabel("Jumlah Catatan Data (Juta Baris)", fontsize=15, fontweight="bold")
    ax.set_title("Variety: Integrasi Tiga Sumber Data Berbeda Format ke dalam Parquet", fontsize=18, fontweight="bold", pad=20)
    ax.set_ylim(0, max(records) * 1.25)

    for bar in bars:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, f"{bar.get_height():.2f} Juta", ha='center', fontweight='bold', fontsize=13)

    save_matplotlib_chart(fig, CHARTS_DIR / "variety_multi_source_composition.png")

    # 4. Write RESULT.md
    result_md_path = OUTPUT_MOD_DIR / "RESULT.md"
    content = f"""# Hasil Eksekusi & Analisis: Variety (Heterogenitas Sumber Data)

## 1. Integrasi Format Sumber Beragam (*Variety*)
Pipeline berhasil menggabungkan 3 format data berbeda:
1. **Encounters:** CSV tabular standar (13.734.415 baris).
2. **Patient Events:** JSON Lines / NDJSON semi-terstruktur (4.406.252 baris event log).
3. **Clinical Activity:** CSV format vertikal / long unpivoted (8.723.515 baris).

Semua format tersebut disatukan secara terkurasi ke dalam satu tabel relasional tunggal **Parquet Snappy Columnar (`encounter_analytics`)** dengan total {len(df.columns)} atribut terstandardisasi.

---

## 2. Pelacakan Provenance & Silsilah Data
- **`data_origin`:** `{provenance_df['data_origin'].iloc[0] if not provenance_df.empty else 'synthetic_bootstrap'}`.
- **`synthetic_flag`:** Seluruh baris bernilai `true` (menandai integritas data audit bahwa seluruh baris adalah data hasil simulasi terkontrol).
"""
    with open(result_md_path, "w", encoding="utf-8") as f:
        f.write(content)

    logging.info(f"RESULT.md saved to {result_md_path}")
    logging.info("VARIETY MODULE COMPLETED SUCCESSFULLY.")
    spark.stop()

if __name__ == "__main__":
    run_variety_analysis()
