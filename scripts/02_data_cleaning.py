
import pandas as pd
import numpy as np
import os
import json

# ==========================================
# 1. CONFIGURATION
# ==========================================

INPUT_FILE = "data/raw/diabetic_data.csv"

OUTPUT_DIR = "data/processed"
REPORT_DIR = "reports"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

print("=" * 60)
print("CHRONIC DISEASE POPULATION ANALYTICS")
print("DATA CLEANING AND QUALITY MANAGEMENT")
print("=" * 60)

# ==========================================
# 2. LOAD ORIGINAL DATA
# ==========================================

df = pd.read_csv(
    INPUT_FILE,
    dtype={
        "patient_nbr": "string",
        "encounter_id": "string"
    },
    low_memory=False
)

original_records = len(df)
original_columns = len(df.columns)

print("\n[1] ORIGINAL DATA")
print("Records:", original_records)
print("Columns:", original_columns)

# ==========================================
# 3. REMOVE UNWANTED SPACES
# ==========================================

for col in df.select_dtypes(include=["object", "string"]).columns:
    df[col] = df[col].str.strip()

print("\n[2] WHITESPACE CLEANING COMPLETED")

# ==========================================
# 4. HANDLE MISSING VALUES
# ==========================================

# Hanya tanda '?' yang diubah menjadi NULL.
# Kategori 'None' tetap dipertahankan.

df = df.replace(r"^\?$", pd.NA, regex=True)

print("[3] MISSING VALUE STANDARDIZATION COMPLETED")

# ==========================================
# 5. DATA TYPE CONVERSION
# ==========================================

numeric_columns = [
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_outpatient",
    "number_emergency",
    "number_inpatient",
    "number_diagnoses"
]

for col in numeric_columns:
    df[col] = pd.to_numeric(
        df[col],
        errors="coerce"
    ).astype("Int64")

print("[4] NUMERIC TYPE CONVERSION COMPLETED")

# ==========================================
# 6. ID VALIDATION
# ==========================================

missing_patient_id = int(
    df["patient_nbr"].isna().sum()
)

missing_encounter_id = int(
    df["encounter_id"].isna().sum()
)

duplicate_encounters = int(
    df["encounter_id"].duplicated().sum()
)

print("\n[5] ID VALIDATION")
print("Missing patient IDs:", missing_patient_id)
print("Missing encounter IDs:", missing_encounter_id)
print("Duplicate encounters:", duplicate_encounters)

# ==========================================
# 7. CATEGORY VALIDATION
# ==========================================

valid_readmission = [
    "NO",
    ">30",
    "<30"
]

invalid_readmission = df[
    ~df["readmitted"].isin(valid_readmission)
    & df["readmitted"].notna()
]

print("\n[6] CATEGORY VALIDATION")
print(
    "Invalid readmission values:",
    len(invalid_readmission)
)

# ==========================================
# 8. NUMERIC RANGE VALIDATION
# ==========================================

range_rules = {
    "time_in_hospital": (1, 14),
    "num_lab_procedures": (0, 150),
    "num_procedures": (0, 100),
    "num_medications": (0, 100),
    "number_outpatient": (0, 100),
    "number_emergency": (0, 100),
    "number_inpatient": (0, 100),
    "number_diagnoses": (1, 20)
}

range_report = []

for col, (minimum, maximum) in range_rules.items():

    invalid_count = int(
        (
            (df[col] < minimum) |
            (df[col] > maximum)
        ).sum()
    )

    range_report.append({
        "column": col,
        "minimum_allowed": minimum,
        "maximum_allowed": maximum,
        "invalid_count": invalid_count
    })

range_df = pd.DataFrame(range_report)

print("\n[7] NUMERIC RANGE VALIDATION")
print(range_df.to_string(index=False))

range_df.to_csv(
    f"{REPORT_DIR}/numeric_validation_report.csv",
    index=False
)

# ==========================================
# 9. MISSING VALUE REPORT
# ==========================================

missing_report = pd.DataFrame({
    "column": df.columns,
    "missing_count": df.isna().sum().values,
    "missing_percentage": (
        df.isna().mean().values * 100
    ).round(2)
})

missing_report.to_csv(
    f"{REPORT_DIR}/cleaned_missing_report.csv",
    index=False
)

# ==========================================
# 10. SAVE CLEANED DATA
# ==========================================

csv_output = (
    f"{OUTPUT_DIR}/diabetic_cleaned.csv"
)

parquet_output = (
    f"{OUTPUT_DIR}/diabetic_cleaned.parquet"
)

df.to_csv(
    csv_output,
    index=False
)

df.to_parquet(
    parquet_output,
    index=False,
    engine="pyarrow"
)

# ==========================================
# 11. QUALITY SUMMARY
# ==========================================

quality_summary = {
    "original_records": int(original_records),
    "cleaned_records": int(len(df)),
    "original_columns": int(original_columns),
    "cleaned_columns": int(len(df.columns)),
    "missing_patient_id": missing_patient_id,
    "missing_encounter_id": missing_encounter_id,
    "duplicate_encounters": duplicate_encounters,
    "invalid_readmission": int(len(invalid_readmission)),
    "total_missing_values": int(df.isna().sum().sum()),
    "cleaning_status": "completed"
}

with open(
    f"{REPORT_DIR}/data_quality_summary.json",
    "w"
) as file:
    json.dump(
        quality_summary,
        file,
        indent=4
    )

print("\n" + "=" * 60)
print("DATA CLEANING COMPLETED")
print("=" * 60)

print("Original records:", original_records)
print("Cleaned records:", len(df))
print("Columns:", len(df.columns))

print("\nOUTPUT FILES:")
print(csv_output)
print(parquet_output)
print(f"{REPORT_DIR}/data_quality_summary.json")
