
import pandas as pd
import numpy as np
import os
import json
import time
from collections import Counter

# ==========================================
# 1. CONFIGURATION
# ==========================================

INPUT_FILE = (
    "data/synthetic/encounters/"
    "encounters_synthetic.csv"
)

OUTPUT_DIR = "data/synthetic/clinical_activity"

OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "clinical_activity_records.csv"
)

REPORT_DIR = "reports"

TARGET_GB = 1.0
TARGET_BYTES = int(TARGET_GB * 1_000_000_000)

CHUNK_SIZE = 100_000
SAMPLE_RATE = 0.05

RANDOM_SEED = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ==========================================
# 2. SOURCE ATTRIBUTES
# ==========================================

diagnosis_columns = [
    "diag_1",
    "diag_2",
    "diag_3"
]

laboratory_columns = [
    "A1Cresult",
    "max_glu_serum"
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

value_columns = (
    diagnosis_columns
    + laboratory_columns
    + medication_columns
    + metric_columns
)

id_columns = [
    "patient_nbr",
    "encounter_id"
]

required_columns = id_columns + value_columns

activity_type_mapping = {}

for column in diagnosis_columns:
    activity_type_mapping[column] = "Diagnosis"

for column in laboratory_columns:
    activity_type_mapping[column] = "Laboratory"

for column in medication_columns:
    activity_type_mapping[column] = "Medication"

for column in metric_columns:
    activity_type_mapping[column] = "Encounter_Metric"

# ==========================================
# 3. INITIALIZATION
# ==========================================

print("=" * 65)
print("CLINICAL ACTIVITY RECORDS GENERATOR")
print("CHRONIC DISEASE POPULATION ANALYTICS")
print("=" * 65)

print("\nTarget:", TARGET_GB, "GB")
print("Chunk size:", CHUNK_SIZE)
print("Encounter sampling rate:", SAMPLE_RATE)

if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(INPUT_FILE)

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

rng = np.random.default_rng(RANDOM_SEED)

start_time = time.time()

total_scanned = 0
selected_encounters = 0
total_activities = 0
total_bytes = 0
batch_number = 0

activity_distribution = Counter()

# ==========================================
# 4. BATCH PROCESSING
# ==========================================

reader = pd.read_csv(
    INPUT_FILE,
    usecols=required_columns,
    dtype="string",
    chunksize=CHUNK_SIZE,
    low_memory=False
)

for chunk in reader:

    # Stop when target is reached
    if total_bytes >= TARGET_BYTES:
        break

    batch_number += 1
    total_scanned += len(chunk)

    # Select 5% of encounters
    mask = (
        rng.random(len(chunk))
        < SAMPLE_RATE
    )

    selected = chunk.loc[mask].copy()

    if selected.empty:
        continue

    selected_encounters += len(selected)

    # ======================================
    # 5. WIDE TO LONG TRANSFORMATION
    # ======================================

    activity_df = selected.melt(
        id_vars=id_columns,
        value_vars=value_columns,
        var_name="source_field",
        value_name="activity_value"
    )

    # Remove missing values.
    # Keep meaningful values such as medication "No"
    activity_df = activity_df[
        activity_df["activity_value"].notna()
    ].copy()

    activity_df["activity_value"] = (
        activity_df["activity_value"].str.strip()
    )

    activity_df = activity_df[
        (activity_df["activity_value"] != "")
        & (activity_df["activity_value"] != "?")
    ].copy()

    if activity_df.empty:
        continue

    # ======================================
    # 6. GENERATE ACTIVITY ATTRIBUTES
    # ======================================

    activity_df["activity_type"] = (
        activity_df["source_field"]
        .map(activity_type_mapping)
    )

    activity_df["data_origin"] = (
        "synthetic_attribute_projection"
    )

    activity_df["synthetic_flag"] = True

    # ======================================
    # 7. GENERATE UNIQUE ACTIVITY IDs
    # ======================================

    number_of_activities = len(activity_df)

    start_id = total_activities + 1
    end_id = total_activities + number_of_activities

    activity_df.insert(
        0,
        "activity_id",
        [
            f"ACT{x:012d}"
            for x in range(start_id, end_id + 1)
        ]
    )

    total_activities += number_of_activities

    # ======================================
    # 8. UPDATE DISTRIBUTION
    # ======================================

    activity_distribution.update(
        activity_df["activity_type"].value_counts().to_dict()
    )

    # ======================================
    # 9. WRITE CSV BATCH
    # ======================================

    write_header = (
        batch_number == 1
        or total_bytes == 0
    )

    if write_header:
        # The first batch with activities writes header
        write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0

    activity_df.to_csv(
        OUTPUT_FILE,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8"
    )

    # Measure actual output size
    total_bytes = os.path.getsize(OUTPUT_FILE)

    print(
        f"Batch {batch_number} | "
        f"Scanned: {total_scanned:,} | "
        f"Selected: {selected_encounters:,} | "
        f"Activities: {total_activities:,} | "
        f"Size: {total_bytes / 1_000_000:.2f} MB"
    )

    del selected
    del activity_df

# ==========================================
# 10. FINAL REPORT
# ==========================================

elapsed_time = time.time() - start_time

actual_size_gb = (
    total_bytes / 1_000_000_000
)

report = {
    "dataset": "Chronic Disease Population Analytics",
    "source": "Synthetic Encounters CSV",
    "output_format": "CSV Long Format",
    "generation_method": "Batch Wide-to-Long Transformation",
    "random_seed": RANDOM_SEED,
    "encounter_sample_rate": SAMPLE_RATE,
    "source_encounters_scanned": int(total_scanned),
    "selected_encounters": int(selected_encounters),
    "synthetic_records": int(total_activities),
    "target_size_gb": TARGET_GB,
    "actual_size_gb": round(actual_size_gb, 6),
    "file_size_bytes": int(total_bytes),
    "generation_time_seconds": round(
        elapsed_time, 2
    ),
    "activity_distribution": dict(activity_distribution),
    "data_origin": "synthetic_attribute_projection",
    "synthetic_flag": True
}

report_file = (
    f"{REPORT_DIR}/clinical_activity_generation_report.json"
)

with open(report_file, "w") as f:
    json.dump(report, f, indent=4)

# ==========================================
# 11. DISPLAY RESULTS
# ==========================================

print("\n" + "=" * 65)
print("CLINICAL ACTIVITY GENERATION COMPLETED")
print("=" * 65)

print("Source encounters:", f"{total_scanned:,}")
print("Selected encounters:", f"{selected_encounters:,}")
print("Activity records:", f"{total_activities:,}")
print("File size:", round(actual_size_gb, 4), "GB")
print("Generation time:", round(elapsed_time, 2), "seconds")

print("\nActivity distribution:")

for category, count in activity_distribution.items():
    print(f"{category}: {count:,}")

print("\nOutput:", OUTPUT_FILE)
print("Report:", report_file)

if total_bytes < TARGET_BYTES:
    print(
        "\nNOTICE: Source was exhausted before "
        "the 1 GB target was reached."
    )
