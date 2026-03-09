from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType, StringType, TimestampType


def get_dataset_metadata(df):
    num_cols = [c for c, t in df.dtypes if "int" in t or "double" in t or "float" in t]
    cat_cols = [c for c, t in df.dtypes if "string" in t]
    date_cols = [c for c, t in df.dtypes if "timestamp" in t]

    return {
        "rows": df.count(),
        "cols": len(df.columns),
        "numerical": num_cols,
        "categorical": cat_cols,
        "datetime": date_cols
    }

def perform_diverse_analysis(file_path):
    spark = SparkSession.builder \
        .appName("US_Accidents_Stage1_Analysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    meta = get_dataset_metadata(df)
    print(f"\n--- ДАНІ ДАТАСЕТУ ---")
    print(f"Загальний обсяг: {meta['rows']} рядків на {meta['cols']} колонок.")
    print(f"Структура: {len(meta['numerical'])} числових, {len(meta['categorical'])} категоріальних.")

    print(f"\n--- ЗАГАЛЬНА СТАТИСТИКА ---")
    df.describe().show()

    print(f"\n--- БАЛАНС КЛЮЧОВИХ КАТЕГОРІЙ ---")
    for col_name in ["Severity", "Sunrise_Sunset"]:
        print(f"\nРозподіл за {col_name}:")
        df.groupBy(col_name).count().orderBy(F.desc("count")).show()

    if meta['datetime']:
        print(f"\n--- [4] ЧАСОВИЙ ДІАПАЗОН ---")
        df.select(F.min(meta['datetime'][0]), F.max(meta['datetime'][0])).show()

    return df


if __name__ == "__main__":
    PATH = "data/US_Accidents_March23.csv"
    perform_diverse_analysis(PATH)