from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def get_dataset_metadata(df):
    num_cols = [c for c, t in df.dtypes if "int" in t or "double" in t or "float" in t]
    cat_cols = [c for c, t in df.dtypes if "string" in t or "boolean" in t]
    date_cols = [c for c, t in df.dtypes if "timestamp" in t]

    return {
        "rows": df.count(),
        "cols": len(df.columns),
        "numerical": num_cols,
        "categorical": cat_cols,
        "datetime": date_cols
    }


def run_comprehensive_analysis(file_path):
    spark = SparkSession.builder \
        .appName("US_Accidents_Full_EDA") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        meta = get_dataset_metadata(df)

        print(f"\n--- ОПИС ДАТАСЕТУ ---")
        print(f"Обсяг: {meta['rows']} рядків на {meta['cols']} колонок.")
        print(f"Структура: {len(meta['numerical'])} числових, {len(meta['categorical'])} категоріальних.")

        print(f"\n--- АНАЛІЗ УСІХ КАТЕГОРІАЛЬНИХ ОЗНАК ---")
        for col_name in meta['categorical']:
            print(f"\nТоп-5 значений для '{col_name}':")
            df.groupBy(col_name).count().orderBy(F.desc("count")).show(5)

        print(f"\n--- ГЕНЕРАЦІЯ ГРАФІКІВ ---")
        sns.set_theme(style="whitegrid")

        severity_data = df.groupBy("Severity").count().toPandas()
        plt.figure(figsize=(8, 5))
        sns.barplot(x='Severity', y='count', data=severity_data, palette='viridis')
        plt.title('Розподіл аварій за рівнем важкості (Severity)')
        plt.savefig('viz_severity.png')
        plt.close()

        state_data = df.groupBy("State").count().orderBy(F.desc("count")).limit(10).toPandas()
        plt.figure(figsize=(10, 6))
        sns.barplot(x='count', y='State', data=state_data, palette='magma')
        plt.title('Топ-10 штатів за кількістю ДТП')
        plt.savefig('viz_top_states.png')
        plt.close()

        sunset_data = df.groupBy("Sunrise_Sunset").count().toPandas().dropna()
        plt.figure(figsize=(7, 7))
        plt.pie(sunset_data['count'], labels=sunset_data['Sunrise_Sunset'], autopct='%1.1f%%',
                colors=['orange', 'navy'])
        plt.title('Розподіл ДТП: День vs Ніч')
        plt.savefig('viz_day_night.png')
        plt.close()

        weather_data = df.groupBy("Weather_Condition").count().orderBy(F.desc("count")).limit(5).toPandas()
        plt.figure(figsize=(10, 5))
        sns.barplot(x='count', y='Weather_Condition', data=weather_data, palette='coolwarm')
        plt.title('Топ-5 погодних умов під час ДТП')
        plt.savefig('viz_weather.png')
        plt.close()

        return df

    except Exception as e:
        print(f"Помилка: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    PATH = "data/US_Accidents_March23.csv"
    run_comprehensive_analysis(PATH)