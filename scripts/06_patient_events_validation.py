
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType
)
from pyspark.sql.functions import (
    col,
    to_timestamp
)
import json
import os
import time

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

REPORT_DIR = "reports"

os.makedirs(REPORT_DIR, exist_ok=True)

# ==========================================
# 2. SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("PatientEventsValidation")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start_time = time.time()

print("=" * 60)
print("PATIENT EVENTS VALIDATION")
print("APACHE SPARK 3.5.9")
print("=" * 60)

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
# 4. READ DATA
# ==========================================

print("\n[1] Reading CSV and JSON...")

encounters = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(ENCOUNTER_FILE)
    .select("patient_nbr", "encounter_id")
)

events = (
    spark.read
    .schema(event_schema)
    .json(EVENT_FILE)
)

# ==========================================
# 5. BASIC COUNTS
# ==========================================

print("\n[2] Counting records...")

encounter_count = encounters.count()
event_count = events.count()

unique_encounters = (
    encounters.select("encounter_id")
    .distinct()
    .count()
)

unique_events = (
    events.select("event_id")
    .distinct()
    .count()
)

unique_patients = (
    encounters.select("patient_nbr")
    .distinct()
    .count()
)

print("Encounter records:", encounter_count)
print("Unique encounters:", unique_encounters)
print("Unique patients:", unique_patients)
print("Event records:", event_count)
print("Unique events:", unique_events)

# ==========================================
# 6. DUPLICATE VALIDATION
# ==========================================

print("\n[3] Checking duplicate IDs...")

duplicate_event_ids = (
    event_count - unique_events
)

duplicate_encounter_ids = (
    encounter_count - unique_encounters
)

# ==========================================
# 7. FOREIGN KEY VALIDATION
# ==========================================

print("\n[4] Checking patient-encounter relationships...")

valid_encounters = (
    encounters
    .select("patient_nbr", "encounter_id")
    .dropDuplicates()
)

unmatched_events = (
    events.join(
        valid_encounters,
        on=["patient_nbr", "encounter_id"],
        how="left_anti"
    )
)

unmatched_event_count = unmatched_events.count()

# ==========================================
# 8. EVENT TYPE VALIDATION
# ==========================================

print("\n[5] Checking event types...")

allowed_types = [
    "admission_registered",
    "laboratory_activity",
    "medication_review",
    "discharge_registered",
    "followup_scheduled"
]

invalid_event_types = (
    events.filter(
        ~col("event_type").isin(allowed_types)
        | col("event_type").isNull()
    ).count()
)

# ==========================================
# 9. TIMESTAMP VALIDATION
# ==========================================

print("\n[6] Checking timestamps...")

invalid_timestamps = (
    events.withColumn(
        "parsed_timestamp",
        to_timestamp(
            col("event_timestamp"),
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
        )
    )
    .filter(
        col("parsed_timestamp").isNull()
    )
    .count()
)

# ==========================================
# 10. SYNTHETIC FLAG VALIDATION
# ==========================================

print("\n[7] Checking synthetic flags...")

invalid_flags = (
    events.filter(
        (col("synthetic_flag") != True)
        | col("synthetic_flag").isNull()
    ).count()
)

invalid_sources = (
    events.filter(
        col("source") != "synthetic_simulation"
    ).count()
)

# ==========================================
# 11. EVENT DISTRIBUTION
# ==========================================

print("\n[8] Event distribution...")

distribution = (
    events.groupBy("event_type")
    .count()
    .collect()
)

event_distribution = {
    row["event_type"]: int(row["count"])
    for row in distribution
}

# ==========================================
# 12. VALIDATION REPORT
# ==========================================

elapsed = time.time() - start_time

report = {
    "validation_engine": "Apache Spark 3.5.9",
    "encounter_records": int(encounter_count),
    "unique_encounters": int(unique_encounters),
    "unique_patients": int(unique_patients),
    "event_records": int(event_count),
    "unique_events": int(unique_events),
    "duplicate_event_ids": int(duplicate_event_ids),
    "duplicate_encounter_ids": int(
        duplicate_encounter_ids
    ),
    "unmatched_events": int(unmatched_event_count),
    "invalid_event_types": int(invalid_event_types),
    "invalid_timestamps": int(invalid_timestamps),
    "invalid_synthetic_flags": int(invalid_flags),
    "invalid_sources": int(invalid_sources),
    "event_distribution": event_distribution,
    "validation_time_seconds": round(elapsed, 2)
}

report_file = (
    f"{REPORT_DIR}/patient_events_validation_report.json"
)

with open(report_file, "w") as f:
    json.dump(report, f, indent=4)

# ==========================================
# 13. DISPLAY RESULTS
# ==========================================

print("\n" + "=" * 60)
print("VALIDATION RESULTS")
print("=" * 60)

for key, value in report.items():
    print(f"{key}: {value}")

print("\nReport saved:", report_file)

spark.stop()
