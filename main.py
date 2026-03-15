import os
import sys
from pyspark.sql import SparkSession
from schemas.raw_schema import raw_schema
from config import DATA_PATH, CATEGORICAL_COLUMNS, NUMERICAL_COLUMNS
from modules.parsing import parse_and_transform_features
from modules.extraction import load_data, verify_data
from modules.eda_stats import (
    get_metadata,
    run_categorical_eda,
    run_numerical_eda,
    run_numerical_plots
)
from modules.feature_selection import select_features


def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = (
        SparkSession.builder
        .appName("US-Accidents-Project")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.maxResultSize", "4g")
        .getOrCreate()
    )

    print("\n--- Spark Session успішно ініціалізована ---\n")

    print("--- Етап видобування (Extraction) ---")
    raw_df = load_data(spark, DATA_PATH, raw_schema)
    # verify_data(raw_df)
    #
    # print("\n--- Етап аналізу (EDA) ---")
    #
    # get_metadata(raw_df)

    # print("\n--- Аналіз числових ознак ---")
    # run_numerical_eda(raw_df, NUMERICAL_COLUMNS)
    #
    # print("\n--- Побудова графіків числових ознак ---")
    # run_numerical_plots(raw_df, NUMERICAL_COLUMNS)
    #
    # print("\n--- Аналіз категоріальних ознак ---")
    # run_categorical_eda(raw_df, CATEGORICAL_COLUMNS)

    print("\n--- Приведення типів та парсинг даних ---")
    processed_df = parse_and_transform_features(raw_df)

    print("\n--- Вибір ознак ---")
    processed_df = select_features(processed_df)

    print("\n--- Етап видобування та аналізу успішно завершено ---")
    spark.stop()

if __name__ == "__main__":
    main()