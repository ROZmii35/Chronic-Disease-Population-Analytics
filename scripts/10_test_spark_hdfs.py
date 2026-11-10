
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ChronicDisease-HDFS-Test")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

HDFS = "hdfs://localhost:9000"

files = {
    "encounters": f"{HDFS}/chronic-disease-analytics/raw/encounters/encounters_synthetic.csv",
    "patient_events": f"{HDFS}/chronic-disease-analytics/raw/patient_events/patient_events.jsonl",
    "clinical_activity": f"{HDFS}/chronic-disease-analytics/raw/clinical_activity/clinical_activity_records.csv"
}

print("\n=== TEST READ HDFS ===")

encounters = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(files["encounters"])
)

events = spark.read.json(files["patient_events"])

activities = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(files["clinical_activity"])
)

print("\nEncounters schema:")
encounters.printSchema()

print("\nPatient Events schema:")
events.printSchema()

print("\nClinical Activity schema:")
activities.printSchema()

print("\n=== SAMPLE DATA ===")

encounters.show(5, truncate=True)
events.show(5, truncate=True)
activities.show(5, truncate=True)

print("\nHDFS READ TEST COMPLETED")

spark.stop()
