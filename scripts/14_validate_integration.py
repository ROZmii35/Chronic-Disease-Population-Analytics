
import json
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum
)

# ============================================================
# 1. SPARK CONFIGURATION
# ============================================================

spark = (
    SparkSession.builder
    .appName("ChronicDisease-Integration-Validation")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://localhost:9000/chronic-disease-analytics"

integrated_path = f"{BASE}/analytics/encounter_analytics"
events_path = f"{BASE}/curated/patient_events"
activities_path = f"{BASE}/curated/clinical_activity"

os.makedirs("reports", exist_ok=True)

start = time.perf_counter()

print("\n" + "=" * 60)
print("INTEGRATED DATA VALIDATION")
print("=" * 60)

# ============================================================
# 2. READ DATA
# ============================================================

integrated = spark.read.parquet(integrated_path)
events = spark.read.parquet(events_path)
activities = spark.read.parquet(activities_path)

print("\n[1/5] DATA READ COMPLETED")

# ============================================================
# 3. RECORD COUNT AND UNIQUE ENCOUNTER CHECK
# ============================================================

print("\n[2/5] CHECKING RECORDS AND UNIQUE IDS")

integrated_count = integrated.count()

duplicate_encounters = (
    integrated
    .groupBy("encounter_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_encounter_ids = (
    integrated
    .filter(
        col("encounter_id").isNull() |
        (col("encounter_id") == "")
    )
    .count()
)

null_patient_ids = (
    integrated
    .filter(
        col("patient_nbr").isNull() |
        (col("patient_nbr") == "")
    )
    .count()
)

print(f"Integrated records       : {integrated_count:,}")
print(f"Duplicate encounter IDs  : {duplicate_encounters:,}")
print(f"Missing encounter IDs    : {null_encounter_ids:,}")
print(f"Missing patient IDs      : {null_patient_ids:,}")

# ============================================================
# 4. CHECK AGGREGATED TOTALS
# ============================================================

print("\n[3/5] CHECKING AGGREGATED TOTALS")

integrated_totals = integrated.agg(
    spark_sum("event_count").alias("total_events"),
    spark_sum("activity_record_count").alias("total_activities")
).first()

source_event_count = events.count()
source_activity_count = activities.count()

total_integrated_events = integrated_totals["total_events"] or 0
total_integrated_activities = (
    integrated_totals["total_activities"] or 0
)

event_total_match = (
    total_integrated_events == source_event_count
)

activity_total_match = (
    total_integrated_activities == source_activity_count
)

print(f"Source events             : {source_event_count:,}")
print(f"Integrated event total    : {total_integrated_events:,}")
print(f"Event totals match        : {event_total_match}")

print(f"\nSource activities         : {source_activity_count:,}")
print(f"Integrated activity total : {total_integrated_activities:,}")
print(f"Activity totals match     : {activity_total_match}")

# ============================================================
# 5. CHECK NEGATIVE AGGREGATES
# ============================================================

print("\n[4/5] CHECKING AGGREGATE VALUES")

aggregate_columns = [
    "event_count",
    "admission_event_count",
    "laboratory_event_count",
    "medication_review_count",
    "discharge_event_count",
    "followup_event_count",
    "activity_record_count",
    "diagnosis_activity_count",
    "laboratory_activity_count",
    "medication_activity_count",
    "encounter_metric_count"
]

negative_condition = None

for column_name in aggregate_columns:
    condition = col(column_name) < 0

    if negative_condition is None:
        negative_condition = condition
    else:
        negative_condition = negative_condition | condition

negative_aggregate_rows = (
    integrated.filter(negative_condition).count()
)

print(f"Rows with negative aggregates: {negative_aggregate_rows:,}")

# ============================================================
# 6. FINAL STATUS
# ============================================================

print("\n[5/5] GENERATING VALIDATION REPORT")

checks = {
    "record_count_matches_expected": (
        integrated_count == 13734415
    ),
    "duplicate_encounter_ids": duplicate_encounters == 0,
    "missing_encounter_ids": null_encounter_ids == 0,
    "missing_patient_ids": null_patient_ids == 0,
    "event_totals_match": event_total_match,
    "activity_totals_match": activity_total_match,
    "negative_aggregate_rows": negative_aggregate_rows == 0
}

all_passed = all(checks.values())

elapsed = time.perf_counter() - start

report = {
    "application": "Chronic Disease Population Analytics",
    "process": "Integrated Data Validation",
    "timestamp": datetime.now().isoformat(),
    "status": "PASSED" if all_passed else "REVIEW_REQUIRED",
    "integrated_records": integrated_count,
    "duplicate_encounter_ids": duplicate_encounters,
    "missing_encounter_ids": null_encounter_ids,
    "missing_patient_ids": null_patient_ids,
    "source_event_records": source_event_count,
    "integrated_event_records": total_integrated_events,
    "source_activity_records": source_activity_count,
    "integrated_activity_records": total_integrated_activities,
    "negative_aggregate_rows": negative_aggregate_rows,
    "checks": checks,
    "validation_seconds": round(elapsed, 2)
}

with open(
    "reports/integration_validation_report.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(report, file, indent=4)

print("\n" + "=" * 60)
print("INTEGRATION VALIDATION SUMMARY")
print("=" * 60)

for name, passed in checks.items():
    print(f"{name:38}: {'PASSED' if passed else 'FAILED'}")

print("-" * 60)
print(f"Integrated records : {integrated_count:,}")
print(f"Validation time    : {elapsed:.2f} seconds")
print(f"Overall status     : {report['status']}")
print("=" * 60)

print("\nReport saved: reports/integration_validation_report.json")

spark.stop()
