from pyspark.sql import DataFrame, SparkSession

def load_data(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(path)
    )

