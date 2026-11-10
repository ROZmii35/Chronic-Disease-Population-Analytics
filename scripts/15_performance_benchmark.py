
import csv
import json
import statistics
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession


BASE = "hdfs://localhost:9000/chronic-disease-analytics"

RAW = {
    "encounters": f"{BASE}/raw/encounters",
    "patient_events": f"{BASE}/raw/patient_events",
    "clinical_activity": f"{BASE}/raw/clinical_activity",
}

CURATED = {
    "encounters": f"{BASE}/curated/encounters",
    "patient_events": f"{BASE}/curated/patient_events",
    "clinical_activity": f"{BASE}/curated/clinical_activity",
}

ANALYTICS = f"{BASE}/analytics/encounter_analytics"

PARTITIONS = [2, 4, 8]
REPEATS = 3


def format_bytes(n):
    value = float(n)

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{value:.2f} {unit}"
        value /= 1024


def get_hdfs_size(spark, uri):
    """Mengambil ukuran logis file di HDFS."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(conf)

    if not fs.exists(path):
        raise FileNotFoundError(f"Path tidak ditemukan: {uri}")

    return int(fs.getContentSummary(path).getLength())


def main():

    report_dir = Path("reports")
    report_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("ChronicDisease_Performance_Benchmark")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    report = {
        "project": "Chronic Disease Population Analytics",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "environment": {
            "benchmark_type": "single-node",
            "spark_version": spark.version,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "adaptive_query_execution": False,
            "partition_settings": PARTITIONS,
            "repeats": REPEATS
        },
        "size_comparison": [],
        "analytics_read_benchmark": {},
        "partition_benchmark": [],
        "methodology_notes": [
            "HDFS size menggunakan ukuran logis, bukan ukuran setelah dikalikan replication factor.",
            "Partition benchmark menggunakan groupBy(patient_nbr).count().",
            "Median dihitung dari tiga kali pengujian.",
            "Benchmark dijalankan pada single-node environment.",
            "Waktu dapat dipengaruhi cache filesystem, JVM warm-up, dan beban komputer."
        ]
    }

    try:

        # ==========================================
        # 1. STORAGE EFFICIENCY
        # ==========================================

        print("\n[1/3] STORAGE EFFICIENCY")
        print("=" * 60)

        raw_total = 0
        curated_total = 0

        for name in RAW:

            raw_size = get_hdfs_size(spark, RAW[name])
            curated_size = get_hdfs_size(spark, CURATED[name])

            reduction = (
                (raw_size - curated_size) / raw_size * 100
                if raw_size else 0
            )

            raw_total += raw_size
            curated_total += curated_size

            row = {
                "dataset": name,
                "raw_bytes": raw_size,
                "raw_size": format_bytes(raw_size),
                "curated_bytes": curated_size,
                "curated_size": format_bytes(curated_size),
                "size_reduction_percent": round(reduction, 2)
            }

            report["size_comparison"].append(row)

            print(f"\nDataset: {name}")
            print(f"Raw     : {format_bytes(raw_size)}")
            print(f"Curated : {format_bytes(curated_size)}")
            print(f"Reduction: {reduction:.2f}%")

        total_reduction = (
            (raw_total - curated_total) / raw_total * 100
            if raw_total else 0
        )

        report["size_comparison"].append({
            "dataset": "TOTAL",
            "raw_bytes": raw_total,
            "raw_size": format_bytes(raw_total),
            "curated_bytes": curated_total,
            "curated_size": format_bytes(curated_total),
            "size_reduction_percent": round(total_reduction, 2)
        })

        print("\nTOTAL STORAGE")
        print(f"Raw total     : {format_bytes(raw_total)}")
        print(f"Curated total : {format_bytes(curated_total)}")
        print(f"Reduction     : {total_reduction:.2f}%")

        # ==========================================
        # 2. ANALYTICS READ PERFORMANCE
        # ==========================================

        print("\n[2/3] ANALYTICS READ PERFORMANCE")
        print("=" * 60)

        analytics_df = spark.read.parquet(ANALYTICS)

        start = time.perf_counter()

        analytics_count = analytics_df.count()

        elapsed = time.perf_counter() - start

        throughput = (
            analytics_count / elapsed
            if elapsed else 0
        )

        report["analytics_read_benchmark"] = {
            "record_count": int(analytics_count),
            "elapsed_seconds": round(elapsed, 3),
            "throughput_records_per_second": round(throughput, 2),
            "throughput_definition":
                "Jumlah record dibagi waktu eksekusi dalam detik."
        }

        print(f"Records    : {analytics_count:,}")
        print(f"Elapsed    : {elapsed:.3f} seconds")
        print(f"Throughput : {throughput:,.2f} records/second")

        # ==========================================
        # 3. PARTITION PERFORMANCE
        # ==========================================

        print("\n[3/3] PARTITION PERFORMANCE")
        print("=" * 60)

        for partition_count in PARTITIONS:

            spark.conf.set(
                "spark.sql.shuffle.partitions",
                str(partition_count)
            )

            trial_times = []
            patient_groups = None
            input_records = None

            for trial in range(1, REPEATS + 1):

                df = spark.read.parquet(CURATED["encounters"])

                start = time.perf_counter()

                # Agregasi berdasarkan patient_nbr.
                grouped = (
                    df.groupBy("patient_nbr")
                    .count()
                )

                # Action untuk menjalankan proses Spark.
                patient_groups = grouped.count()

                elapsed = time.perf_counter() - start

                trial_times.append(elapsed)

                print(
                    f"Partitions={partition_count}, "
                    f"Trial={trial}/{REPEATS}, "
                    f"Time={elapsed:.3f}s, "
                    f"Groups={patient_groups:,}"
                )

                # Catat jumlah input pada pengujian pertama.
                if input_records is None:
                    input_records = df.count()

            median_time = statistics.median(trial_times)

            throughput = (
                input_records / median_time
                if median_time else 0
            )

            report["partition_benchmark"].append({
                "shuffle_partitions": partition_count,
                "trial_times_seconds": [
                    round(x, 3) for x in trial_times
                ],
                "median_seconds": round(median_time, 3),
                "patient_groups": int(patient_groups),
                "input_encounter_records": int(input_records),
                "throughput_records_per_second": round(
                    throughput, 2
                )
            })

        # ==========================================
        # 4. SAVE REPORT
        # ==========================================

        json_path = report_dir / "performance_benchmark_report.json"

        with json_path.open("w", encoding="utf-8") as f:
            json.dump(
                report,
                f,
                indent=4,
                ensure_ascii=False
            )

        csv_path = report_dir / "partition_benchmark.csv"

        with csv_path.open(
            "w",
            newline="",
            encoding="utf-8-sig"
        ) as f:

            fields = [
                "shuffle_partitions",
                "trial_times_seconds",
                "median_seconds",
                "patient_groups",
                "input_encounter_records",
                "throughput_records_per_second"
            ]

            writer = csv.DictWriter(
                f,
                fieldnames=fields
            )

            writer.writeheader()

            for row in report["partition_benchmark"]:

                csv_row = row.copy()

                csv_row["trial_times_seconds"] = ";".join(
                    map(str, row["trial_times_seconds"])
                )

                writer.writerow(csv_row)

        # ==========================================
        # 5. FINAL SUMMARY
        # ==========================================

        print("\n" + "=" * 60)
        print("PERFORMANCE BENCHMARK COMPLETED")
        print("=" * 60)

        print(f"Raw total      : {format_bytes(raw_total)}")
        print(f"Curated total  : {format_bytes(curated_total)}")
        print(f"Reduction      : {total_reduction:.2f}%")
        print(f"Analytics rows : {analytics_count:,}")

        print("\nPARTITION RESULTS")

        for row in report["partition_benchmark"]:

            print(
                f"Partitions {row['shuffle_partitions']}: "
                f"Median {row['median_seconds']:.3f} seconds"
            )

        print(f"\nJSON Report : {json_path}")
        print(f"CSV Report  : {csv_path}")

        print("\nSTATUS: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
