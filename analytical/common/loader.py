import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, coalesce, lit
from analytical.config import DATA_PATH

def load_encounter_analytics(spark, verbose=True) -> DataFrame:
    """
    Membaca dataset encounter_analytics Parquet, melakukan validasi skema nyata,
    casting tipe data numerik (dari string ke int), dan menerapkan Adapter Skema
    agar kompatibel dengan skema lokal (Script 09) maupun skema HDFS (Script 13).
    """
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Path dataset tidak ditemukan: {DATA_PATH}")

    df = spark.read.parquet(str(DATA_PATH))
    existing_cols = df.columns

    if verbose:
        print("\n" + "=" * 65)
        print("LOADING & ADAPTING ENCOUNTER ANALYTICS DATASET")
        print("=" * 65)
        print(f"Path: {DATA_PATH}")
        print(f"Total Kolom Ditemukan: {len(existing_cols)}")

    # 1. Casting Eksplisit untuk Kolom Numerik Asli (yang bertipe String di Parquet)
    numeric_columns = [
        "time_in_hospital",
        "num_lab_procedures",
        "num_procedures",
        "num_medications",
        "number_outpatient",
        "number_emergency",
        "number_inpatient",
        "number_diagnoses"
    ]
    for c in numeric_columns:
        if c in existing_cols:
            df = df.withColumn(c, col(c).cast("int"))

    # 2. Adapter Skema: Normalisasi Nama Kolom Agregat Lokal vs HDFS
    # Kolom Total Event
    if "event_count" in existing_cols:
        df = df.withColumn("total_event_count", col("event_count").cast("long"))
    else:
        df = df.withColumn("total_event_count", lit(0).cast("long"))

    # Kolom Total Aktivitas Klinis
    if "activity_record_count" in existing_cols:
        df = df.withColumn("total_activity_count", col("activity_record_count").cast("long"))
    elif "activity_count" in existing_cols:
        df = df.withColumn("total_activity_count", col("activity_count").cast("long"))
    else:
        df = df.withColumn("total_activity_count", lit(0).cast("long"))

    # Kolom Rincian Diagnosis Activity
    if "diagnosis_activity_count" in existing_cols:
        df = df.withColumn("diagnosis_activity_count", col("diagnosis_activity_count").cast("long"))
    elif "diagnosis_count" in existing_cols:
        df = df.withColumn("diagnosis_activity_count", col("diagnosis_count").cast("long"))
    else:
        df = df.withColumn("diagnosis_activity_count", lit(0).cast("long"))

    # Kolom Rincian Medication Activity
    if "medication_activity_count" in existing_cols:
        df = df.withColumn("medication_activity_count", col("medication_activity_count").cast("long"))
    elif "medication_count" in existing_cols:
        df = df.withColumn("medication_activity_count", col("medication_count").cast("long"))
    else:
        df = df.withColumn("medication_activity_count", lit(0).cast("long"))

    # Kolom Rincian Laboratory Activity
    if "laboratory_activity_count" in existing_cols:
        df = df.withColumn("laboratory_activity_count", col("laboratory_activity_count").cast("long"))
    elif "laboratory_count" in existing_cols:
        df = df.withColumn("laboratory_activity_count", col("laboratory_count").cast("long"))
    else:
        df = df.withColumn("laboratory_activity_count", lit(0).cast("long"))

    # Kolom Rincian Metric Activity
    if "encounter_metric_count" in existing_cols:
        df = df.withColumn("metric_activity_count", col("encounter_metric_count").cast("long"))
    elif "metric_count" in existing_cols:
        df = df.withColumn("metric_activity_count", col("metric_count").cast("long"))
    else:
        df = df.withColumn("metric_activity_count", lit(0).cast("long"))

    # Kolom Coverage Indicator (Aturan Keras 7)
    df = df.withColumn("has_clinical_activity", when(col("total_activity_count") > 0, 1).otherwise(0))
    df = df.withColumn("has_patient_event", when(col("total_event_count") > 0, 1).otherwise(0))

    return df
