import sys
import math
from pathlib import Path
import pandas as pd

# Root repo setup
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from analytical.config import OUTPUTS_DIR

def verify_all_claims():
    print("=" * 75)
    print("VERIFY CLAIMS & AUTOMATED LEDGER AUDIT (UTS BIG DATA SEGMEN 5)")
    print("=" * 75)

    ledger_path = OUTPUTS_DIR / "CLAIMS_LEDGER.csv"
    claims = []
    has_mismatch = False

    def add_claim(claim_id, klaim, expected_val, tolerance, actual_val, satuan, file_sumber, lokasi):
        nonlocal has_mismatch
        # Check matching
        is_match = False
        if isinstance(expected_val, (int, float)) and isinstance(actual_val, (int, float)):
            if math.isclose(expected_val, actual_val, abs_tol=tolerance):
                is_match = True
        else:
            if str(expected_val).strip().lower() == str(actual_val).strip().lower():
                is_match = True

        status = "MATCH" if is_match else "MISMATCH"
        if not is_match:
            has_mismatch = True

        claims.append({
            "claim_id": claim_id,
            "klaim": klaim,
            "nilai_diharapkan": expected_val,
            "nilai_aktual": actual_val,
            "satuan": satuan,
            "toleransi": tolerance,
            "file_sumber": str(Path(file_sumber).as_posix()),
            "lokasi": lokasi,
            "status": status
        })

    # ---------------------------------------------------------
    # 1. Claims Veracity
    # ---------------------------------------------------------
    missing_csv = OUTPUTS_DIR / "veracity" / "tables" / "missing_value_comparison.csv"
    if missing_csv.exists():
        df_m = pd.read_csv(missing_csv)
        row_weight = df_m[df_m["column_name"] == "weight"].iloc[0]
        row_glu = df_m[df_m["column_name"] == "max_glu_serum"].iloc[0]

        add_claim("C01", "Persentase missing weight pada UCI asli", 96.86, 0.05, float(row_weight["uci_raw_missing_pct"]), "%", missing_csv, "column weight, uci_raw_missing_pct")
        add_claim("C02", "Persentase missing glukosa serum pada UCI asli", 94.75, 0.05, float(row_glu["uci_raw_missing_pct"]), "%", missing_csv, "column max_glu_serum, uci_raw_missing_pct")
        add_claim("C03", "Sisa karakter kotor '?' pada Parquet weight", 0, 0, int(row_weight["curated_remaining_question_marks"]), "baris", missing_csv, "column weight, curated_remaining_question_marks")
        add_claim("C04", "Sisa karakter kotor '?' pada Parquet glukosa", 0, 0, int(row_glu["curated_remaining_question_marks"]), "baris", missing_csv, "column max_glu_serum, curated_remaining_question_marks")

    integ_csv = OUTPUTS_DIR / "veracity" / "tables" / "data_integrity_validation.csv"
    if integ_csv.exists():
        df_int = pd.read_csv(integ_csv)
        pk_dup = df_int[df_int["aturan_validasi"].str.contains("Primary Key Keunikan")].iloc[0]
        los_val = df_int[df_int["aturan_validasi"].str.contains("Lama Rawat")].iloc[0]
        add_claim("C05", "Jumlah duplikat primary key encounter_id", 0, 0, int(pk_dup["pelanggaran"]), "baris", integ_csv, "aturan Primary Key Keunikan")
        add_claim("C06", "Pelanggaran batas rentang lama rawat 1-14 hari", 0, 0, int(los_val["pelanggaran"]), "baris", integ_csv, "aturan Rentang Lama Rawat")

    issues_csv = OUTPUTS_DIR / "veracity" / "tables" / "three_quality_issues_before_after.csv"
    if issues_csv.exists():
        df_iss = pd.read_csv(issues_csv)
        hospice_row = df_iss[df_iss["isu_kualitas"].str.contains("Pasien Meninggal / Hospice")].iloc[0]
        add_claim("C07", "Keberadaan isu pasien meninggal / hospice", "Pasien Meninggal / Hospice (Discharge 11, 13, 14, 19, 20, 21)", 0, hospice_row["isu_kualitas"], "kategori", issues_csv, "isu_kualitas row 3")

    # ---------------------------------------------------------
    # 2. Claims Value
    # ---------------------------------------------------------
    diag_csv = OUTPUTS_DIR / "value" / "tables" / "icd9_diagnosis_vs_readmission.csv"
    if diag_csv.exists():
        df_d = pd.read_csv(diag_csv)
        row_circ = df_d[df_d["diagnosis_group"] == "Circulatory"].iloc[0]
        row_diab = df_d[df_d["diagnosis_group"] == "Diabetes"].iloc[0]
        row_resp = df_d[df_d["diagnosis_group"] == "Respiratory"].iloc[0]

        add_claim("C08", "Peringkat volume diagnosis Sirkulasi / Kardiovaskular", 1, 0, int(row_circ["rank_volume"]), "rank", diag_csv, "Circulatory, rank_volume")
        add_claim("C09", "Persentase populasi diagnosis Sirkulasi", 29.91, 0.05, float(row_circ["pct_population"]), "%", diag_csv, "Circulatory, pct_population")
        add_claim("C10", "Peringkat volume diagnosis Pernapasan", 3, 0, int(row_resp["rank_volume"]), "rank", diag_csv, "Respiratory, rank_volume")
        add_claim("C11", "Peringkat volume diagnosis Diabetes", 5, 0, int(row_diab["rank_volume"]), "rank", diag_csv, "Diabetes, rank_volume")
        add_claim("C12", "Tingkat readmisi <30d diagnosis Diabetes", 13.00, 0.05, float(row_diab["pct_readmit_lt30"]), "%", diag_csv, "Diabetes, pct_readmit_lt30")
        add_claim("C13", "Baseline readmisi <30d populasi keseluruhan", 11.17, 0.05, float(row_diab["baseline_lt30_pct"]), "%", diag_csv, "baseline_lt30_pct")
        add_claim("C14", "Risk ratio diagnosis Diabetes vs baseline", 1.164, 0.01, float(row_diab["risk_ratio"]), "ratio", diag_csv, "Diabetes, risk_ratio")

    intensity_csv = OUTPUTS_DIR / "value" / "tables" / "clinical_intensity_vs_readmission.csv"
    if intensity_csv.exists():
        df_i = pd.read_csv(intensity_csv)
        row_los = df_i[df_i["metrik_klinis"].str.contains("Lama Rawat Inap")].iloc[0]
        row_med = df_i[df_i["metrik_klinis"].str.contains("Jumlah Obat")].iloc[0]

        add_claim("C15", "Rata-rata lama rawat pasien readmisi <30d", 4.78, 0.05, float(row_los["mean_lt30"]), "hari", intensity_csv, "mean_lt30 LOS")
        add_claim("C16", "Rata-rata lama rawat pasien non-readmisi NO", 4.25, 0.05, float(row_los["mean_no"]), "hari", intensity_csv, "mean_no LOS")
        add_claim("C17", "Cohen's d lama rawat inap", 0.18, 0.02, float(row_los["cohens_d"]), "d", intensity_csv, "cohens_d LOS")
        add_claim("C18", "Rata-rata jumlah obat pasien readmisi <30d", 16.92, 0.05, float(row_med["mean_lt30"]), "obat", intensity_csv, "mean_lt30 num_medications")
        add_claim("C19", "Rata-rata jumlah obat pasien non-readmisi NO", 15.67, 0.05, float(row_med["mean_no"]), "obat", intensity_csv, "mean_no num_medications")
        add_claim("C20", "Cohen's d jumlah obat", 0.15, 0.02, float(row_med["cohens_d"]), "d", intensity_csv, "cohens_d num_medications")

    # ---------------------------------------------------------
    # 3. Claims Volume & Rekonsiliasi Storage
    # ---------------------------------------------------------
    vol_csv = OUTPUTS_DIR / "volume" / "tables" / "volume_summary.csv"
    if vol_csv.exists():
        df_vol = pd.read_csv(vol_csv).iloc[0]
        add_claim("C21", "Total baris encounter analitis terintegrasi", 13734415, 0, int(df_vol["total_records"]), "baris", vol_csv, "total_records")
        add_claim("C22", "Persentase reduksi penyimpanan storage", 89.98, 0.02, float(df_vol["storage_reduction_pct"]), "%", vol_csv, "storage_reduction_pct")

    # ---------------------------------------------------------
    # 4. Claims Computational
    # ---------------------------------------------------------
    bench_csv = OUTPUTS_DIR / "computational" / "tables" / "benchmark_op_a_vs_b.csv"
    if bench_csv.exists():
        df_b = pd.read_csv(bench_csv)
        row_op_a = df_b[df_b["operation"].str.contains("Operation A ")].iloc[0]
        row_op_b = df_b[df_b["operation"].str.contains("Operation B")].iloc[0]
        add_claim("C23", "Waktu eksekusi median Operation A", 1.009, 0.15, float(row_op_a["median_detik"]), "detik", bench_csv, "Operation A median_detik")
        add_claim("C24", "Rasio waktu Operation B vs Operation A", 43.33, 5.0, float(row_op_b["rasio_vs_op_a"]), "x", bench_csv, "Operation B rasio_vs_op_a")

    # Simpan ledger ke CSV
    df_ledger = pd.DataFrame(claims)
    df_ledger.to_csv(ledger_path, index=False)

    print(f"\nTotal Klaim Diuji: {len(df_ledger)}")
    matches = (df_ledger['status'] == 'MATCH').sum()
    mismatches = (df_ledger['status'] == 'MISMATCH').sum()
    print(f"Hasil: {matches} MATCH | {mismatches} MISMATCH")
    print(f"Ledger disimpan di: {ledger_path}")

    if has_mismatch:
        print("\n[ERROR] Ditemukan klaim MISMATCH! Periksa detail di atas.")
        sys.exit(1)
    else:
        print("\n[SUCCESS] Seluruh klaim 100% MATCH dengan file bukti!")
        sys.exit(0)

if __name__ == "__main__":
    verify_all_claims()
