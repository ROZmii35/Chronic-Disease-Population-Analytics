
import pandas as pd
import numpy as np
import json
import os
from collections import Counter

ORIGINAL = "data/processed/diabetic_cleaned.csv"
SYNTHETIC = "data/synthetic/encounters/encounters_synthetic.csv"
REPORT_DIR = "reports"

os.makedirs(REPORT_DIR, exist_ok=True)

CHUNK_SIZE = 100_000

print("=" * 60)
print("SYNTHETIC DATA VALIDATION")
print("=" * 60)

# Load original dataset
original = pd.read_csv(
    ORIGINAL,
    dtype=str,
    keep_default_na=False
)

original = original.replace("?", np.nan)
original = original.replace("", np.nan)

original_readmission = (
    original["readmitted"]
    .value_counts(normalize=True)
)

original_age = (
    original["age"]
    .value_counts(normalize=True)
)

# Counters
synthetic_readmission = Counter()
synthetic_age = Counter()

total_rows = 0
missing_counts = Counter()

encounter_ids = set()
patient_ids = set()

duplicate_encounters = 0
invalid_origin = 0

print("\nReading synthetic data in chunks...")

for chunk in pd.read_csv(
    SYNTHETIC,
    dtype=str,
    chunksize=CHUNK_SIZE,
    keep_default_na=False
):

    total_rows += len(chunk)

    # Duplicate encounter IDs
    ids = chunk["encounter_id"].tolist()

    for eid in ids:
        if eid in encounter_ids:
            duplicate_encounters += 1
        else:
            encounter_ids.add(eid)

    # Count unique patients
    patient_ids.update(
        chunk["patient_nbr"].unique()
    )

    # Distribution
    synthetic_readmission.update(
        chunk["readmitted"].value_counts().to_dict()
    )

    synthetic_age.update(
        chunk["age"].value_counts().to_dict()
    )

    # Missing values
    for col in chunk.columns:
        missing_counts[col] += int(
            chunk[col].isin(["", "?"]).sum()
        )

    # Synthetic origin validation
    invalid_origin += int(
        (chunk["data_origin"] != "synthetic_bootstrap").sum()
    )

    print(
        f"Validated {total_rows:,} records",
        end="\r"
    )

# Calculate distribution differences
readmission_comparison = {}

for category in original_readmission.index:

    original_pct = float(
        original_readmission.get(category, 0) * 100
    )

    synthetic_pct = (
        synthetic_readmission.get(category, 0)
        / total_rows * 100
    )

    readmission_comparison[category] = {
        "original_pct": round(original_pct, 3),
        "synthetic_pct": round(synthetic_pct, 3),
        "difference_percentage_points": round(
            abs(original_pct - synthetic_pct), 3
        )
    }

age_comparison = {}

for category in original_age.index:

    original_pct = float(
        original_age.get(category, 0) * 100
    )

    synthetic_pct = (
        synthetic_age.get(category, 0)
        / total_rows * 100
    )

    age_comparison[category] = {
        "original_pct": round(original_pct, 3),
        "synthetic_pct": round(synthetic_pct, 3),
        "difference_percentage_points": round(
            abs(original_pct - synthetic_pct), 3
        )
    }

report = {
    "synthetic_records": total_rows,
    "unique_patients": len(patient_ids),
    "unique_encounters": len(encounter_ids),
    "duplicate_encounters": duplicate_encounters,
    "invalid_origin": invalid_origin,
    "readmission_comparison": readmission_comparison,
    "age_comparison": age_comparison,
    "missing_counts": dict(missing_counts)
}

output_file = (
    f"{REPORT_DIR}/synthetic_validation_report.json"
)

with open(output_file, "w") as f:
    json.dump(report, f, indent=4)

print("\n\nVALIDATION COMPLETED")
print("Records:", total_rows)
print("Unique patients:", len(patient_ids))
print("Unique encounters:", len(encounter_ids))
print("Duplicate encounters:", duplicate_encounters)
print("Invalid origin:", invalid_origin)
print("Report:", output_file)
