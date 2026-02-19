import os
import sys
from pyspark.sql import SparkSession

def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName("US-Accidents-Project") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

    data = [("Project", "Started"), ("File", "Active")]
    df = spark.createDataFrame(data, ["Entity", "Status"])

    print("--- Spark Session успішно ініціалізована ---")
    df.show() 

if __name__ == "__main__":
    main()