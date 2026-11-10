
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType
)
from pyspark.sql.functions import col, lower, trim

# ============================================================
# 1. SPARK CONFIGURATION
# ============================================================

spark = (
    SparkSession.builder
    .appName("ChronicDisease-Raw-To-Curated")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://localhost:9000/chronic-disease-analytics"

# ============================================================
# 2. SOURCE AND DESTINATION PATHS
# ============================================================

encounters_input = (
    f"{BASE}/raw/encounters/encounters_synthetic.csv"
)
events_input = (
    f"{BASE}/raw/patient_events/patient_events.jsonl"
)
activities_input = (
    f"{BASE}/raw/clinical_activity/clinical_activity_records.csv"
)

encounters_output = f"{BASE}/curated/encounters"
events_output = f"{BASE}/curated/patient_events"
activities_output = f"{BASE}/curated/clinical_activity"

# ============================================================
# 3. READ ENCOUNTERS
# ============================================================

print("\n[1/3] PROCESSING ENCOUNTERS")

encounters = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(encounters_input)
)

# Normalize synthetic_flag if the column exists.
if "synthetic_flag" in encounters.columns:
    encounters = encounters.withColumn(
        "synthetic_flag",
        lower(trim(col("synthetic_flag"))).cast(BooleanType())
    )

encounters_count = encounters.count()

(
    encounters.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(encounters_output)
)

print(f"Encounters records : {encounters_count:,}")
print(f"Saved to           : {encounters_output}")

# ============================================================
# 4. READ PATIENT EVENTS
# ============================================================

print("\n[2/3] PROCESSING PATIENT EVENTS")

events_schema = StructType([
    StructField("encounter_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("patient_nbr", StringType(), True),
    StructField("source", StringType(), True),
    StructField("synthetic_flag", BooleanType(), True)
])

events = (
    spark.read
    .schema(events_schema)
    .json(events_input)
)

events_count = events.count()

(
    events.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(events_output)
)

print(f"Patient events records : {events_count:,}")
print(f"Saved to               : {events_output}")

# ============================================================
# 5. READ CLINICAL ACTIVITY
# ============================================================

print("\n[3/3] PROCESSING CLINICAL ACTIVITY")

activities = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(activities_input)
)

if "synthetic_flag" in activities.columns:
    activities = activities.withColumn(
        "synthetic_flag",
        lower(trim(col("synthetic_flag"))).cast(BooleanType())
    )

activities_count = activities.count()

(
    activities.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(activities_output)
)

print(f"Clinical activity records : {activities_count:,}")
print(f"Saved to                   : {activities_output}")

# ============================================================
# 6. SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("RAW TO CURATED TRANSFORMATION COMPLETED")
print("=" * 60)

print(f"Encounters        : {encounters_count:,}")
print(f"Patient Events    : {events_count:,}")
print(f"Clinical Activity : {activities_count:,}")

print(
    f"Total records     : "
    f"{encounters_count + events_count + activities_count:,}"
)

print("\nOutput format : Parquet")
print("Compression   : Snappy")
print("Storage       : HDFS /curated")

spark.stop()
