import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from analytical.config import SPARK_MASTER, SPARK_APP_NAME, SPARK_DRIVER_MEMORY, SHUFFLE_PARTITIONS

# 1. Konfigurasi JAVA_HOME jika belum ada di environment
if "JAVA_HOME" not in os.environ:
    default_java_paths = [
        r"C:\Program Files\Java\jre-1.8",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Java\jdk-11",
    ]
    for p in default_java_paths:
        if os.path.exists(p):
            os.environ["JAVA_HOME"] = p
            break

# 2. Konfigurasi HADOOP_HOME & winutils lokal untuk environment Windows
hadoop_dir = Path(__file__).resolve().parent.parent / "hadoop"
if (hadoop_dir / "bin" / "winutils.exe").exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    os.environ["hadoop.home.dir"] = str(hadoop_dir)
    bin_path = str(hadoop_dir / "bin")
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")

def get_spark_session(app_name=SPARK_APP_NAME):
    """
    Menginisialisasi atau mengambil SparkSession terstandarisasi untuk analisis Segmen 5.
    Menggunakan shuffle.partitions=8 (konfigurasi tercepat dari benchmark DE).
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "false")  # Nonaktifkan AQE agar hasil benchmark deterministik
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")            # Matikan Spark UI untuk menghemat socket & memory
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
