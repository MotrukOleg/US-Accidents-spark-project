from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = SparkSession.builder \
        .appName("US_Accidents_EDA") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    file_path = "data/US_Accidents_March23.csv"

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        print("\n" + "="*50)
        print("1. ОСНОВНА СТАТИСТИКА")
        print("="*50)

        df.select("Severity", "Distance(mi)", "Temperature(F)", "Visibility(mi)").describe().show()

        print("\n" + "="*50)
        print("2. АНАЛІЗ ПРОПУЩЕНИХ ЗНАЧЕНЬ (NULLs)")
        print("="*50)

        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns[:10]]) # перші 10 для прикладу
        null_counts.show()

        print("\n" + "="*50)
        print("3. ЧАСОВІ МЕЖІ ТА РОЗМІР")
        print("="*50)
        print(f"Загальна кількість записів: {df.count()}")
        df.select(F.min("Start_Time").alias("Початок даних"), F.max("Start_Time").alias("Кінець даних")).show()

    except Exception as e:
        print(f"Помилка: {e}. Перевір, чи файл дійсно лежить за шляхом {file_path}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()