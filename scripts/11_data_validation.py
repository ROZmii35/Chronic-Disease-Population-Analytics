
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel


# ============================================================
# 1. SPARK CONFIGURATION
# ============================================================

HDFS = "hdfs://localhost:9000/chronic-disease-analytics/raw"

PROJECT_DIR = os.getcwd()
REPORT_DIR = os.path.join(PROJECT_DIR, "reports")
REPORT_FILE = os.path.join(REPORT_DIR, "data_validation_report.json")

os.makedirs(REPORT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("ChronicDisease-Data-Validation")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

report = {
    "project": "Chronic Disease Population Analytics",
    "stage": "Data Validation",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "source": "HDFS",
    "datasets": {},
    "overall_status": "RUNNING"
}


def count_rows(df):
    return df.count()


def count_duplicates(df, column):
    return (
        df.groupBy(column)
        .count()
        .filter(F.col(column).isNotNull() & (F.col("count") > 1))
        .count()
    )


def count_nulls(df, column):
    return df.filter(
        F.col(column).isNull() |
        (F.trim(F.col(column).cast("string")) == "")
    ).count()


def count_invalid_flag(df):
    return df.filter(
        F.lower(F.col("synthetic_flag").cast("string"))
        .isin("true", "false") == False
    ).count()


def count_missing_origin(df):
    return count_nulls(df, "data_origin")


def count_unmatched(df, encounters):
    """
    Memeriksa apakah encounter_id dan patient_nbr
    pada dataset anak cocok dengan dataset encounters.
    """

    reference = (
        encounters
        .select("encounter_id", "patient_nbr")
        .dropDuplicates(["encounter_id"])
    )

    child = (
        df.select("encounter_id", "patient_nbr")
        .dropDuplicates()
    )

    unmatched = (
        child.alias("c")
        .join(
            reference.alias("r"),
            F.col("c.encounter_id") == F.col("r.encounter_id"),
            "left"
        )
        .filter(
            F.col("r.encounter_id").isNull() |
            (F.col("c.patient_nbr") != F.col("r.patient_nbr")) |
            F.col("c.patient_nbr").isNull()
        )
    )

    return unmatched.count()


# ============================================================
# 3. READ DATA FROM HDFS
# ============================================================

print("\n" + "=" * 65)
print("CHRONIC DISEASE POPULATION ANALYTICS")
print("DATA VALIDATION STARTED")
print("=" * 65)

encounters = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{HDFS}/encounters/encounters_synthetic.csv")
#    .persist(StorageLevel.MEMORY_AND_DISK)
)

events = (
    spark.read
    .json(f"{HDFS}/patient_events/patient_events.jsonl")
#    .persist(StorageLevel.MEMORY_AND_DISK)
)

activities = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{HDFS}/clinical_activity/clinical_activity_records.csv")
#    .persist(StorageLevel.MEMORY_AND_DISK)
)


# ============================================================
# 4. ENCOUNTERS VALIDATION
# ============================================================

print("\n[1/3] VALIDATING ENCOUNTERS...")

encounter_count = count_rows(encounters)

encounter_duplicates = count_duplicates(
    encounters, "encounter_id"
)

encounter_null_id = count_nulls(
    encounters, "encounter_id"
)

encounter_null_patient = count_nulls(
    encounters, "patient_nbr"
)

encounter_invalid_flag = count_invalid_flag(encounters)

encounter_missing_origin = count_missing_origin(encounters)

encounter_unique_patients = (
    encounters.select("patient_nbr")
    .distinct()
    .count()
)

report["datasets"]["encounters"] = {
    "total_records": encounter_count,
    "unique_patients": encounter_unique_patients,
    "duplicate_encounter_ids": encounter_duplicates,
    "missing_encounter_ids": encounter_null_id,
    "missing_patient_ids": encounter_null_patient,
    "invalid_synthetic_flags": encounter_invalid_flag,
    "missing_data_origin": encounter_missing_origin
}

print(f"Total records             : {encounter_count:,}")
print(f"Unique patients            : {encounter_unique_patients:,}")
print(f"Duplicate encounter IDs   : {encounter_duplicates:,}")
print(f"Missing encounter IDs      : {encounter_null_id:,}")
print(f"Missing patient IDs        : {encounter_null_patient:,}")
print(f"Invalid synthetic flags    : {encounter_invalid_flag:,}")
print(f"Missing data origin        : {encounter_missing_origin:,}")


# ============================================================
# 5. PATIENT EVENTS VALIDATION
# ============================================================

print("\n[2/3] VALIDATING PATIENT EVENTS...")

event_count = count_rows(events)

event_duplicates = count_duplicates(events, "event_id")

event_null_id = count_nulls(events, "event_id")

event_null_patient = count_nulls(events, "patient_nbr")

event_null_encounter = count_nulls(events, "encounter_id")

event_invalid_flag = count_invalid_flag(events)

allowed_event_types = [
    "admission_registered",
    "laboratory_activity",
    "medication_review",
    "discharge_registered",
    "followup_scheduled"
]

event_invalid_type = events.filter(
    ~F.col("event_type").isin(allowed_event_types) |
    F.col("event_type").isNull()
).count()

event_invalid_timestamp = events.filter(
    F.to_timestamp(
        F.col("event_timestamp"),
        "yyyy-MM-dd'T'HH:mm:ssX"
    ).isNull()
).count()

event_unmatched = count_unmatched(events, encounters)

event_missing_source = count_nulls(events, "source")

report["datasets"]["patient_events"] = {
    "total_records": event_count,
    "duplicate_event_ids": event_duplicates,
    "missing_event_ids": event_null_id,
    "missing_patient_ids": event_null_patient,
    "missing_encounter_ids": event_null_encounter,
    "invalid_event_types": event_invalid_type,
    "invalid_timestamps": event_invalid_timestamp,
    "unmatched_patient_encounter_pairs": event_unmatched,
    "invalid_synthetic_flags": event_invalid_flag,
    "missing_source": event_missing_source
}

print(f"Total records             : {event_count:,}")
print(f"Duplicate event IDs       : {event_duplicates:,}")
print(f"Invalid event types       : {event_invalid_type:,}")
print(f"Invalid timestamps        : {event_invalid_timestamp:,}")
print(f"Unmatched patient pairs   : {event_unmatched:,}")
print(f"Invalid synthetic flags   : {event_invalid_flag:,}")


# ============================================================
# 6. CLINICAL ACTIVITY VALIDATION
# ============================================================

print("\n[3/3] VALIDATING CLINICAL ACTIVITY...")

activity_count = count_rows(activities)

activity_duplicates = count_duplicates(
    activities, "activity_id"
)

activity_null_id = count_nulls(
    activities, "activity_id"
)

activity_null_patient = count_nulls(
    activities, "patient_nbr"
)

activity_null_encounter = count_nulls(
    activities, "encounter_id"
)

activity_invalid_flag = count_invalid_flag(activities)

allowed_activity_types = [
    "Diagnosis",
    "Laboratory",
    "Medication",
    "Encounter_Metric"
]

activity_invalid_type = activities.filter(
    ~F.col("activity_type").isin(allowed_activity_types) |
    F.col("activity_type").isNull()
).count()

activity_unmatched = count_unmatched(
    activities, encounters
)

activity_missing_origin = count_missing_origin(
    activities
)

report["datasets"]["clinical_activity"] = {
    "total_records": activity_count,
    "duplicate_activity_ids": activity_duplicates,
    "missing_activity_ids": activity_null_id,
    "missing_patient_ids": activity_null_patient,
    "missing_encounter_ids": activity_null_encounter,
    "invalid_activity_types": activity_invalid_type,
    "unmatched_patient_encounter_pairs": activity_unmatched,
    "invalid_synthetic_flags": activity_invalid_flag,
    "missing_data_origin": activity_missing_origin
}

print(f"Total records             : {activity_count:,}")
print(f"Duplicate activity IDs    : {activity_duplicates:,}")
print(f"Invalid activity types    : {activity_invalid_type:,}")
print(f"Unmatched patient pairs   : {activity_unmatched:,}")
print(f"Invalid synthetic flags   : {activity_invalid_flag:,}")


# ============================================================
# 7. OVERALL VALIDATION RESULT
# ============================================================

checks = [
    encounter_duplicates,
    encounter_null_id,
    encounter_null_patient,
    encounter_invalid_flag,
    encounter_missing_origin,
    event_duplicates,
    event_null_id,
    event_null_patient,
    event_null_encounter,
    event_invalid_flag,
    event_invalid_type,
    event_invalid_timestamp,
    event_unmatched,
    event_missing_source,
    activity_duplicates,
    activity_null_id,
    activity_null_patient,
    activity_null_encounter,
    activity_invalid_flag,
    activity_invalid_type,
    activity_unmatched,
    activity_missing_origin
]

total_issues = sum(checks)

report["total_records"] = (
    encounter_count + event_count + activity_count
)

report["total_validation_issues"] = total_issues

report["overall_status"] = (
    "PASSED" if total_issues == 0 else "REVIEW_REQUIRED"
)


# ============================================================
# 8. SAVE VALIDATION REPORT
# ============================================================

with open(REPORT_FILE, "w", encoding="utf-8") as file:
    json.dump(report, file, indent=4, ensure_ascii=False)

print("\n" + "=" * 65)
print("DATA VALIDATION SUMMARY")
print("=" * 65)

print(f"Total records       : {report['total_records']:,}")
print(f"Total issues        : {total_issues:,}")
print(f"Overall status      : {report['overall_status']}")

print(f"\nReport saved to:")
print(REPORT_FILE)

print("\nDATA VALIDATION COMPLETED")

encounters.unpersist()
events.unpersist()
activities.unpersist()

spark.stop()
