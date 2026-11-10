
import pandas as pd
import numpy as np
import os
import json
import time

# ==========================================
# CONFIGURATION
# ==========================================

INPUT_FILE = "data/processed/diabetic_cleaned.csv"

OUTPUT_DIR = "data/synthetic/encounters"

OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "encounters_synthetic.csv"
)

REPORT_DIR = "reports"

# TEST TARGET: 500 MB
# Setelah berhasil, ubah menjadi 3 GB.
TARGET_GB = 3.0

TARGET_BYTES = int(TARGET_GB * 1_000_000_000)

BATCH_PATIENTS = 10000
RANDOM_SEED = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ==========================================
# LOAD CLEANED DATA
# ==========================================

print("=" * 60)
print("SYNTHETIC DATA GENERATION")
print("CHRONIC DISEASE POPULATION ANALYTICS")
print("=" * 60)

start_time = time.time()

df = pd.read_csv(
    INPUT_FILE,
    dtype={
        "patient_nbr": "string",
        "encounter_id": "string"
    },
    low_memory=False
)

print("\nOriginal records:", len(df))

# ==========================================
# GROUP ENCOUNTERS BY PATIENT
# ==========================================

groups = df.groupby(
    "patient_nbr",
    sort=False
).indices

patient_groups = list(groups.values())

print("Unique patient groups:", len(patient_groups))

# ==========================================
# RANDOM GENERATOR
# ==========================================

rng = np.random.default_rng(RANDOM_SEED)

# ==========================================
# PREPARE OUTPUT
# ==========================================

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

patient_counter = 0
encounter_counter = 0

total_rows = 0
batch_number = 0

write_header = True

# ==========================================
# SYNTHETIC GENERATION
# ==========================================

print("\nStarting generation...")

while True:

    if os.path.exists(OUTPUT_FILE):
        current_size = os.path.getsize(OUTPUT_FILE)
    else:
        current_size = 0

    if current_size >= TARGET_BYTES:
        break

    batch_number += 1

    # Randomly sample patient groups WITH replacement
    selected_groups = rng.integers(
        0,
        len(patient_groups),
        size=BATCH_PATIENTS
    )

    selected_indices = [
        patient_groups[i]
        for i in selected_groups
    ]

    # Combine selected encounter indices
    all_indices = np.concatenate(selected_indices)

    batch_df = df.iloc[all_indices].copy()

    # Generate a new synthetic patient ID
    synthetic_patient_ids = []

    for group_indices in selected_indices:

        patient_counter += 1

        synthetic_patient_ids.extend(
            [f"SYN_P{patient_counter:09d}"]
            * len(group_indices)
        )

    # Generate unique synthetic encounter IDs
    new_encounter_ids = np.arange(
        encounter_counter + 1,
        encounter_counter + len(batch_df) + 1
    )

    batch_df["patient_nbr"] = synthetic_patient_ids

    batch_df["encounter_id"] = [
        f"SYN_E{x:012d}"
        for x in new_encounter_ids
    ]

    encounter_counter += len(batch_df)

    # Add synthetic provenance labels
    batch_df["data_origin"] = "synthetic_bootstrap"
    batch_df["synthetic_flag"] = True

    # Save batch
    batch_df.to_csv(
        OUTPUT_FILE,
        mode="w" if write_header else "a",
        header=write_header,
        index=False
    )

    write_header = False

    total_rows += len(batch_df)

    # Release batch memory
    del batch_df

    current_size = os.path.getsize(OUTPUT_FILE)

    print(
        f"Batch {batch_number} | "
        f"Rows: {total_rows:,} | "
        f"Size: {current_size / 1_000_000:.2f} MB"
    )

# ==========================================
# FINAL REPORT
# ==========================================

final_size = os.path.getsize(OUTPUT_FILE)

elapsed = time.time() - start_time

report = {
    "source_dataset": "UCI Diabetes 130-US Hospitals",
    "generation_method": "Patient-Level Bootstrap Sampling",
    "random_seed": RANDOM_SEED,
    "synthetic_records": int(total_rows),
    "synthetic_patients": int(patient_counter),
    "synthetic_encounters": int(encounter_counter),
    "target_size_gb": TARGET_GB,
    "actual_size_gb": round(
        final_size / 1_000_000_000, 4
    ),
    "file_size_bytes": int(final_size),
    "generation_time_seconds": round(elapsed, 2),
    "synthetic_flag": True
}

with open(
    f"{REPORT_DIR}/synthetic_generation_report.json",
    "w"
) as f:
    json.dump(report, f, indent=4)

print("\n" + "=" * 60)
print("SYNTHETIC GENERATION COMPLETED")
print("=" * 60)

print("Synthetic records:", f"{total_rows:,}")
print("Synthetic patients:", f"{patient_counter:,}")
print("File size:", round(final_size / 1_000_000_000, 3), "GB")
print("Elapsed time:", round(elapsed, 2), "seconds")
print("Output:", OUTPUT_FILE)
