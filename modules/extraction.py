from pyspark.sql import DataFrame, SparkSession

def load_data(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(path)
    )

def verify_data(df: DataFrame):
    print("\n--- Перевірка даних ---")

    df.printSchema()

    count = df.count()
    print(f"Кількість завантажених рядків: {count}")

    df.show(5, truncate=True)

    if count == 0:
        print("[WARNING] DataFrame порожній!")