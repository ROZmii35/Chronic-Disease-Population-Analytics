from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    create_map,
    lit
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

ACTIVITY_FILE = (
    "data/synthetic/clinical_activity/"
    "clinical_activity_records.csv"
)

REPORT_DIR = "reports"

os.makedirs(REPORT_DIR, exist_ok=True)

# ==========================================
# 2. SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("ClinicalActivityValidation")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

start_time = time.time()

print("=" * 65)
print("CLINICAL ACTIVITY VALIDATION")
print("APACHE SPARK 3.5.9")
print("=" * 65)

# ==========================================
# 3. DEFINE SCHEMAS
# ==========================================

encounter_schema = StructType([
    StructField("patient_nbr", StringType(), True),
    StructField("encounter_id", StringType(), True)
])

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
# 4. READ DATA
# ==========================================

print("\n[1] Reading datasets...")

encounters = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(ENCOUNTER_FILE)
    .select(
        "patient_nbr",
        "encounter_id"
    )
)


activities = (
    spark.read
    .option("header", True)
    .schema(activity_schema)
    .csv(ACTIVITY_FILE)
)

# ==========================================
# 5. BASIC COUNTS
# ==========================================

print("\n[2] Counting records...")

encounter_count = encounters.count()
activity_count = activities.count()

unique_activity_ids = (
    activities
    .select("activity_id")
    .distinct()
    .count()
)

unique_patients = (
    activities
    .select("patient_nbr")
    .distinct()
    .count()
)

unique_activity_encounters = (
    activities
    .select("encounter_id")
    .distinct()
    .count()
)

# ==========================================
# 6. DUPLICATE VALIDATION
# ==========================================

print("\n[3] Checking duplicate IDs...")

duplicate_activity_ids = (
    activity_count - unique_activity_ids
)

# ==========================================
# 7. FOREIGN KEY VALIDATION
# ==========================================

print("\n[4] Checking encounter relationships...")

valid_encounters = (
    encounters
    .select("patient_nbr", "encounter_id")
    .dropDuplicates()
)

unmatched_activities = (
    activities.join(
        valid_encounters,
        on=["patient_nbr", "encounter_id"],
        how="left_anti"
    )
)

unmatched_activity_count = (
    unmatched_activities.count()
)

# ==========================================
# 8. ACTIVITY TYPE VALIDATION
# ==========================================

print("\n[5] Checking activity categories...")

allowed_types = [
    "Diagnosis",
    "Laboratory",
    "Medication",
    "Encounter_Metric"
]

invalid_activity_types = (
    activities.filter(
        ~col("activity_type").isin(allowed_types)
        | col("activity_type").isNull()
    ).count()
)

# ==========================================
# 9. SOURCE FIELD VALIDATION
# ==========================================

print("\n[6] Checking source field mapping...")

diagnosis_columns = [
    "diag_1", "diag_2", "diag_3"
]

laboratory_columns = [
    "A1Cresult", "max_glu_serum"
]

medication_columns = [
    "metformin",
    "repaglinide",
    "nateglinide",
    "chlorpropamide",
    "glimepiride",
    "acetohexamide",
    "glipizide",
    "glyburide",
    "tolbutamide",
    "pioglitazone",
    "rosiglitazone",
    "acarbose",
    "miglitol",
    "troglitazone",
    "tolazamide",
    "examide",
    "citoglipton",
    "insulin",
    "glyburide-metformin",
    "glipizide-metformin",
    "glimepiride-pioglitazone",
    "metformin-rosiglitazone",
    "metformin-pioglitazone"
]

metric_columns = [
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_outpatient",
    "number_emergency",
    "number_inpatient",
    "number_diagnoses"
]

field_mapping = {}

for field in diagnosis_columns:
    field_mapping[field] = "Diagnosis"

for field in laboratory_columns:
    field_mapping[field] = "Laboratory"

for field in medication_columns:
    field_mapping[field] = "Medication"

for field in metric_columns:
    field_mapping[field] = "Encounter_Metric"

mapping_expr = create_map(
    *[
        item
        for pair in field_mapping.items()
        for item in (lit(pair[0]), lit(pair[1]))
    ]
)

invalid_source_fields = (
    activities.filter(
        mapping_expr[col("source_field")].isNull()
    ).count()
)

incorrect_field_mapping = (
    activities.filter(
        mapping_expr[col("source_field")].isNotNull()
        & (
            col("activity_type")
            != mapping_expr[col("source_field")]
        )
    ).count()
)

# ==========================================
# 10. MISSING VALUE VALIDATION
# ==========================================

print("\n[7] Checking missing values...")

required_fields = [
    "activity_id",
    "patient_nbr",
    "encounter_id",
    "source_field",
    "activity_value",
    "activity_type",
    "data_origin",
    "synthetic_flag"
]

missing_condition = None

for field in required_fields:

    condition = (
        col(field).isNull()
        | (trim(col(field)) == "")
    )

    if missing_condition is None:
        missing_condition = condition
    else:
        missing_condition = (
            missing_condition | condition
        )

missing_activity_records = (
    activities.filter(missing_condition).count()
)

# ==========================================
# 11. SYNTHETIC FLAG VALIDATION
# ==========================================

print("\n[8] Checking synthetic provenance...")

invalid_flags = (
    activities.filter(
        (lower(trim(col("synthetic_flag")) ) != "true")
        | col("synthetic_flag").isNull()
    ).count()
)

invalid_origins = (
    activities.filter(
        col("data_origin")
        != "synthetic_attribute_projection"
    ).count()
)

# ==========================================
# 12. ACTIVITY DISTRIBUTION
# ==========================================

print("\n[9] Activity distribution...")

distribution = (
    activities
    .groupBy("activity_type")
    .count()
    .collect()
)

activity_distribution = {
    row["activity_type"]: int(row["count"])
    for row in distribution
}

# ==========================================
# 13. VALIDATION REPORT
# ==========================================

elapsed = time.time() - start_time

report = {
    "validation_engine": "Apache Spark 3.5.9",
    "encounter_records": int(encounter_count),
    "activity_records": int(activity_count),
    "unique_activity_ids": int(unique_activity_ids),
    "unique_patients": int(unique_patients),
    "unique_activity_encounters": int(
        unique_activity_encounters
    ),
    "duplicate_activity_ids": int(
        duplicate_activity_ids
    ),
    "unmatched_activities": int(
        unmatched_activity_count
    ),
    "invalid_activity_types": int(
        invalid_activity_types
    ),
    "invalid_source_fields": int(
        invalid_source_fields
    ),
    "incorrect_field_mapping": int(
        incorrect_field_mapping
    ),
    "missing_activity_records": int(
        missing_activity_records
    ),
    "invalid_synthetic_flags": int(invalid_flags),
    "invalid_origins": int(invalid_origins),
    "activity_distribution": activity_distribution,
    "validation_time_seconds": round(elapsed, 2)
}

report_file = (
    f"{REPORT_DIR}/clinical_activity_validation_report.json"
)

with open(report_file, "w") as f:
    json.dump(report, f, indent=4)

# ==========================================
# 14. DISPLAY RESULTS
# ==========================================

print("\n" + "=" * 65)
print("CLINICAL ACTIVITY VALIDATION RESULTS")
print("=" * 65)

for key, value in report.items():
    print(f"{key}: {value}")

print("\nReport saved:", report_file)

spark.stop()
