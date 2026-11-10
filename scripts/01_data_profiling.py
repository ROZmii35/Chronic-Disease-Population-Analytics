import pandas as pd
import numpy as np
import os 
import json

# Konfiurasi data
input_file = 'data/raw/diabetic_data.csv'
output_dir = 'reports'
os.makedirs(output_dir, exist_ok=True)
print("="*60)
print("Chronic Disease Population Analytics")
print("Data Profiling")
print("="*60)

# load data
df = pd.read_csv(input_file,low_memory=False)
print("\n[1] Informasi Dataset:")
print("Total Records: ", len(df))
print("Total Columns: ", len(df.columns))
print("memory usage: ", round(df.memory_usage(deep=True).sum() / 1024**2, 2), "MB")

#  tipe data
print("\n[2] Tipe Data:")
df_missing = df.replace("?", np.nan)
missing_report = pd.DataFrame(
    {'column': df_missing.columns,
     'missing_count': df_missing.isnull().sum().values,
     'missing_percentage': (df_missing.isnull().mean().values * 100).round(2)})
missing_report = missing_report.sort_values(by='missing_percentage', ascending=False)
print("[3] Missing Values Report:")
print(missing_report.to_string(index=False))
missing_report.to_csv(f"{output_dir}/missing_values_report.csv", index=False)

# cek duplikasi
print("\n[4] Cek Duplikasi:")
print("Duplicate rows: ", df.duplicated().sum())
# ==========================================
# 6. UNIQUE PATIENTS
# ==========================================

print("\n[5] PATIENT INFORMATION")

print(
    "Unique Patients:",
    df["patient_nbr"].nunique()
)

print(
    "Total Encounters:",
    df["encounter_id"].nunique()
)

print(
    "Average Encounters per Patient:",
    round(
        len(df) / df["patient_nbr"].nunique(), 2
    )
)

# ==========================================
# 7. READMISSION DISTRIBUTION
# ==========================================

print("\n[6] READMISSION DISTRIBUTION")

print(
    df["readmitted"].value_counts()
)

# ==========================================
# 8. NUMERICAL STATISTICS
# ==========================================

print("\n[7] NUMERICAL STATISTICS")

print(df.describe().to_string())

# ==========================================
# 9. SAVE PROFILE REPORT
# ==========================================

profile = {
    "total_records": int(len(df)),
    "total_columns": int(len(df.columns)),
    "unique_patients": int(
        df["patient_nbr"].nunique()
    ),
    "unique_encounters": int(
        df["encounter_id"].nunique()
    ),
    "duplicate_rows": int(
        df.duplicated().sum()
    ),
    "duplicate_encounter_ids": int(
        df["encounter_id"].duplicated().sum()
    )
}

with open(
    f"{output_dir}/dataset_profile.json",
    "w"
) as f:
    json.dump(profile, f, indent=4)

print("\nProfiling completed!")
print("Reports saved in:", output_dir)
