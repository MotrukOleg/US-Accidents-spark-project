import os
import sys
from pyspark.sql import SparkSession

from schemas.raw_schema import raw_schema
from config import DATA_PATH


def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = (
        SparkSession.builder
        .appName("US-Accidents-Project")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    print("\n--- Spark Session успішно ініціалізована ---\n")

    df = (
        spark.read
        .schema(raw_schema)
        .option("header", True)
        .csv(DATA_PATH)
    )

    print("=== SCHEMA ===")
    df.printSchema()

    print("\n=== FIRST 5 ROWS ===")
    df.show(5, truncate=False)

    print("\n=== ROW COUNT ===")
    print(df.count())

    spark.stop()


if __name__ == "__main__":
    main()