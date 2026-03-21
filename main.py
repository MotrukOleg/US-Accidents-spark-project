import os
import sys

from analytics.om_requests import *

from config import DATA_PATH, CATEGORICAL_COLUMNS, NUMERICAL_COLUMNS

from pyspark.sql import SparkSession

from modules.feature_selection import select_features
from modules.extraction import load_data, verify_data
from modules.parsing import parse_and_transform_features
from modules.olap import create_olap, check_olap_dimensions
from modules.data_quality import check_data_quality, remove_duplicates, handle_missing_values
from modules.eda_stats import get_metadata,run_categorical_eda,run_numerical_eda,run_numerical_plots

from schemas.raw_schema import raw_schema

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = (
    SparkSession.builder
    .appName("US-Accidents-Project")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.debug.maxToStringFields", "1000")
    .getOrCreate()
)

print("\n--- Spark Session успішно ініціалізована ---\n")

print("--- Етап видобування (Extraction) ---")
raw_df = load_data(spark, DATA_PATH, raw_schema)

verify_data(raw_df)

print("\n--- Етап аналізу (EDA) ---")
get_metadata(raw_df)

print("\n--- Аналіз числових ознак ---")
run_numerical_eda(raw_df, NUMERICAL_COLUMNS)

print("\n--- Побудова графіків числових ознак ---")
run_numerical_plots(raw_df, NUMERICAL_COLUMNS)

print("\n--- Аналіз категоріальних ознак ---")
run_categorical_eda(raw_df, CATEGORICAL_COLUMNS)

print("\n--- Аналіз якості даних до інженерії ознак ---")
check_data_quality(raw_df)

processed_df = remove_duplicates(raw_df)

print("\n--- Приведення типів та парсинг даних ---")
processed_df = parse_and_transform_features(processed_df)

print("\n--- Вибір ознак ---")
processed_df = select_features(processed_df)

print("\n--- Аналіз якості даних після інженерії ознак ---")
check_data_quality(processed_df)

processed_df = remove_duplicates(processed_df)
processed_df = handle_missing_values(processed_df)

print("\n--- Аналіз якості даних після обробки ---")
check_data_quality(processed_df)

print("\n--- Створення OLAP-куба ---")

olap = create_olap(processed_df)

print("\n--- Етап видобування, аналізу та попередньої обробки успішно завершено ---")

print("\n --- запити до OLAP-куба --- \n")
get_top_dangerous_hours_per_state(olap).show(50, False)

spark.stop()