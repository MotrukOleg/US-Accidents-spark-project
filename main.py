import os
import sys
from pyspark.sql import SparkSession

from schemas.raw_schema import raw_schema
from config import DATA_PATH

from modules.extraction import load_data, verify_data


def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = (
        SparkSession.builder
        .appName("US-Accidents-Project")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )

    print("\n--- Spark Session успішно ініціалізована ---\n")

    print("--- Етап видобування (Extraction) ---")

    raw_df = load_data(spark, DATA_PATH, raw_schema)

    verify_data(raw_df)

    print("\n--- Етап видобування успішно завершено ---")

    spark.stop()


if __name__ == "__main__":
    main()