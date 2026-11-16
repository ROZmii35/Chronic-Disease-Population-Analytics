import sys
import time
import argparse
import importlib
from pathlib import Path

# Tambahkan direktori root repo ke sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

MODULE_SPECS = {
    "veracity": ("analytical.veracity.04_veracity", "run_veracity_analysis", "Veracity (Data Quality & Integrity)"),
    "value": ("analytical.value.05_value", "run_value_analysis", "Value (Healthcare Interpretation)"),
    "computational": ("analytical.computational.06_benchmark_op_a_vs_b", "run_computational_benchmark", "Computational Complexity (Op A vs B)"),
    "volume": ("analytical.volume.01_volume", "run_volume_analysis", "Volume (Scale & Storage Reduction)"),
    "velocity": ("analytical.velocity.02_velocity", "run_velocity_analysis", "Velocity (Micro-Batch & Partitions)"),
    "variety": ("analytical.variety.03_variety", "run_variety_analysis", "Variety (Multi-format Integration)")
}

def main():
    parser = argparse.ArgumentParser(description="Menjalankan Pipeline Analisis Big Data Segmen 5.")
    parser.add_argument(
        "--only",
        choices=list(MODULE_SPECS.keys()),
        help="Jalankan hanya satu modul spesifik (misal: --only veracity)"
    )
    args = parser.parse_args()

    print("=" * 70)
    print("CHRONIC DISEASE POPULATION ANALYTICS - SEGMEN 5 (UTS BIG DATA)")
    print("MODUL EKSEKUSI ANALISIS LENGKAP (VERACITY, VALUE, COMPUTATIONAL, 5Vs)")
    print("=" * 70)

    start_total = time.perf_counter()
    executed_keys = [args.only] if args.only else list(MODULE_SPECS.keys())
    results = {}

    for mod_key in executed_keys:
        mod_path, func_name, desc = MODULE_SPECS[mod_key]
        print(f"\n>>> [RUNNING] Modul: {desc} ...")
        t0 = time.perf_counter()
        try:
            mod = importlib.import_module(mod_path)
            func = getattr(mod, func_name)
            func()
            t_elapsed = time.perf_counter() - t0
            results[mod_key] = ("SUCCESS", t_elapsed)
            print(f">>> [PASSED] Modul {mod_key} selesai dalam {t_elapsed:.2f} detik.")
        except Exception as e:
            t_elapsed = time.perf_counter() - t0
            results[mod_key] = (f"FAILED: {e}", t_elapsed)
            print(f">>> [FAILED] Modul {mod_key} gagal ({e}) setelah {t_elapsed:.2f} detik.")
            raise e

    total_elapsed = time.perf_counter() - start_total
    print("\n" + "=" * 70)
    print("RINGKASAN EKSEKUSI PIPELINE ANALISIS")
    print("=" * 70)
    for mod_key, (status, elap) in results.items():
        desc = MODULE_SPECS[mod_key][2]
        print(f" - {desc:45}: {status} ({elap:.2f} s)")
    print("-" * 70)
    print(f"Total Waktu Eksekusi: {total_elapsed:.2f} detik ({total_elapsed/60:.2f} menit)")
    print(f"Seluruh tabel, grafik, dan laporan tersimpan di: analytical/outputs/")
    print("=" * 70)

if __name__ == "__main__":
    main()
