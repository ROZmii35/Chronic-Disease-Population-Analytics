
import os
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType
)
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum
)

# ==========================================
# 1. CONFIGURATION
# ==========================================

ENCOUNTER_FILE = (
    "data/synthetic/encounters/"
    "encounters_synthetic.csv"
)

EVENT_FILE = (
    "data/synthetic/patient_events/"
    "patient_events.jsonl"
)

ACTIVITY_FILE = (
    "data/synthetic/clinical_activity/"
    "clinical_activity_records.csv"
)

OUTPUT_BASE = "data/curated/parquet"
REPORT_DIR = "reports"

OUTPUT_ENCOUNTERS = f"{OUTPUT_BASE}/encounters"
OUTPUT_EVENTS = f"{OUTPUT_BASE}/patient_events"
OUTPUT_ACTIVITIES = f"{OUTPUT_BASE}/clinical_activity"
OUTPUT_ANALYTICS = f"{OUTPUT_BASE}/encounter_analytics"

os.makedirs(OUTPUT_BASE, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ==========================================
# 2. SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("ChronicDiseaseDataIntegration")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start_time = time.time()

print("=" * 70)
print("CHRONIC DISEASE POPULATION ANALYTICS")
print("DATA INTEGRATION - APACHE SPARK 3.5.9")
print("=" * 70)

# ==========================================
# 3. DEFINE JSON SCHEMA
# ==========================================

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("patient_nbr", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("synthetic_flag", BooleanType(), True)
])

# ==========================================
# 4. DEFINE ACTIVITY SCHEMA
# ==========================================

activity_schema = StructType([
    StructField("activity_id", StringType(), True),
    StructField("patient_nbr", StringType(), True),
    StructField("encounter_id", StringType(), True),
    StructField("source_field", StringType(), True),
    StructField("activity_value", StringType(), True),
    StructField("activity_type", StringType(), True),
    StructField("data_origin", StringType(), True),
    StructField("synthetic_flag", StringType(), True)
])

# ==========================================
# 5. READ SOURCE DATA
# ==========================================

print("\n[1] Reading source datasets...")

encounters = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(ENCOUNTER_FILE)
)

events = (
    spark.read
    .schema(event_schema)
    .json(EVENT_FILE)
)

activities = (
    spark.read
    .option("header", True)
    .schema(activity_schema)
    .csv(ACTIVITY_FILE)
)

print("All three sources loaded successfully.")

# ==========================================
# 6. BASIC RECORD COUNTS
# ==========================================

print("\n[2] Counting source records...")

encounter_count = encounters.count()
event_count = events.count()
activity_count = activities.count()

print("Encounters:", f"{encounter_count:,}")
print("Patient events:", f"{event_count:,}")
print("Clinical activities:", f"{activity_count:,}")

# ==========================================
# 7. WRITE SOURCE TABLES TO PARQUET
# ==========================================

print("\n[3] Writing Parquet datasets...")

(
    encounters.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(OUTPUT_ENCOUNTERS)
)

print("Encounters Parquet completed.")

(
    events.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(OUTPUT_EVENTS)
)

print("Patient Events Parquet completed.")

(
    activities.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(OUTPUT_ACTIVITIES)
)

print("Clinical Activity Parquet completed.")

# ==========================================
# 8. AGGREGATE PATIENT EVENTS
# ==========================================

print("\n[4] Aggregating patient events...")

event_summary = (
    events
    .groupBy("patient_nbr", "encounter_id")
    .agg(
        count("event_id").alias("event_count")
    )
)

# ==========================================
# 9. AGGREGATE CLINICAL ACTIVITIES
# ==========================================

print("\n[5] Aggregating clinical activities...")

activity_summary = (
    activities
    .groupBy("patient_nbr", "encounter_id")
    .agg(
        count("activity_id").alias("activity_count"),
        spark_sum(
            (col("activity_type") == "Diagnosis")
            .cast("long")
        ).alias("diagnosis_count"),
        spark_sum(
            (col("activity_type") == "Medication")
            .cast("long")
        ).alias("medication_count"),
        spark_sum(
            (col("activity_type") == "Laboratory")
            .cast("long")
        ).alias("laboratory_count"),
        spark_sum(
            (col("activity_type") == "Encounter_Metric")
            .cast("long")
        ).alias("metric_count")
    )
)

# ==========================================
# 10. INTEGRATE DATASETS
# ==========================================

print("\n[6] Integrating datasets...")

integrated = (
    encounters
    .join(
        event_summary,
        on=["patient_nbr", "encounter_id"],
        how="left"
    )
    .join(
        activity_summary,
        on=["patient_nbr", "encounter_id"],
        how="left"
    )
)

# Encounters without events or activities get zero
count_columns = [
    "event_count",
    "activity_count",
    "diagnosis_count",
    "medication_count",
    "laboratory_count",
    "metric_count"
]

for field in count_columns:
    integrated = integrated.fillna(
        {field: 0}
    )

# ==========================================
# 11. WRITE ANALYTICAL DATASET
# ==========================================

print("\n[7] Writing integrated analytics...")

(
    integrated.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(OUTPUT_ANALYTICS)
)

# ==========================================
# 12. VALIDATE INTEGRATED RESULT
# ==========================================

print("\n[8] Validating integrated dataset...")

integrated_count = integrated.count()

unique_integrated = (
    integrated
    .select("encounter_id")
    .distinct()
    .count()
)

duplicate_integrated = (
    integrated_count - unique_integrated
)

# ==========================================
# 13. GENERATE REPORT
# ==========================================

elapsed = time.time() - start_time

report = {
    "project": "Chronic Disease Population Analytics",
    "processing_engine": "Apache Spark 3.5.9",
    "storage_format": "Parquet",
    "compression": "Snappy",
    "source_records": {
        "encounters": int(encounter_count),
        "patient_events": int(event_count),
        "clinical_activities": int(activity_count)
    },
    "integrated_records": int(integrated_count),
    "unique_integrated_encounters": int(
        unique_integrated
    ),
    "duplicate_integrated_encounters": int(
        duplicate_integrated
    ),
    "integration_status": (
        "completed"
        if duplicate_integrated == 0
        and integrated_count == encounter_count
        else "requires_review"
    ),
    "output_paths": {
        "encounters": OUTPUT_ENCOUNTERS,
        "patient_events": OUTPUT_EVENTS,
        "clinical_activities": OUTPUT_ACTIVITIES,
        "encounter_analytics": OUTPUT_ANALYTICS
    },
    "processing_time_seconds": round(elapsed, 2)
}

report_file = (
    f"{REPORT_DIR}/data_integration_report.json"
)

with open(report_file, "w") as f:
    json.dump(report, f, indent=4)

# ==========================================
# 14. DISPLAY RESULTS
# ==========================================

print("\n" + "=" * 70)
print("DATA INTEGRATION COMPLETED")
print("=" * 70)

print("Encounter records:", f"{encounter_count:,}")
print("Event records:", f"{event_count:,}")
print("Activity records:", f"{activity_count:,}")

print("Integrated records:", f"{integrated_count:,}")
print("Unique integrated encounters:", f"{unique_integrated:,}")
print("Duplicate integrated encounters:", duplicate_integrated)

print("Integration status:", report["integration_status"])
print("Processing time:", round(elapsed, 2), "seconds")

print("\nReport:", report_file)

spark.stop()
