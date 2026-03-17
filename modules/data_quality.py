import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from config import DRY_CONDITIONS, PERFECT_VISIBILITY_CONDITIONS, POOR_VISIBILITY_CONDITIONS


def check_duplicates(df: DataFrame):
    print("\n" + "=" * 40)
    print("АНАЛІЗ ДУБЛІКАТІВ")
    print("=" * 40)

    total_count = df.count()
    print(f"Загальна кількість рядків у датасеті: {total_count}")

    id_distinct_count = df.select("ID").distinct().count()
    id_duplicates = total_count - id_distinct_count
    id_pct = (id_duplicates / total_count) * 100
    print(f"Дублікати за колонкою 'ID': {id_duplicates} ({id_pct:.4f}%)")

    semantic_distinct_count = df.drop("ID").distinct().count()
    semantic_duplicates = total_count - semantic_distinct_count
    semantic_pct = (semantic_duplicates / total_count) * 100
    print(f"Смислові дублікати (без урахування ID): {semantic_duplicates} ({semantic_pct:.4f}%)")

    if "Source" in df.columns:
        deep_dup = total_count - df.drop("ID", "Source").distinct().count()
        print(f"Смислові дублікати (без урахування ID та Source): {deep_dup} ({(deep_dup / total_count) * 100:.4f}%)")

    print("-" * 40)

def check_missing_values(df: DataFrame):
    print("\n" + "=" * 50)
    print("АНАЛІЗ ПРОПУЩЕНИХ ЗНАЧЕНЬ")
    print("=" * 50)

    total_rows = df.count()
    null_logic = []

    for col_name, dtype in df.dtypes:
        condition = F.col(col_name).isNull()

        if dtype in ("double", "float"):
            condition = condition | F.isnan(F.col(col_name))

        if dtype == "string":
            condition = condition | (F.trim(F.col(col_name)) == "")

        null_logic.append(F.count(F.when(condition, col_name)).alias(col_name))

    null_counts = df.select(null_logic).collect()[0].asDict()

    results = []
    for col_name, missing_count in null_counts.items():
        if missing_count > 0:
            percentage = (missing_count / total_rows) * 100
            results.append((col_name, missing_count, percentage))

    results.sort(key=lambda x: x[2], reverse=True)

    if not results:
        print("Пропущених значень не знайдено.")
    else:
        print(f"{'Назва колонки':<25} | {'К-сть пропусків':<15} | {'Відсоток':<10}")
        print("-" * 55)
        for col_name, count, pct in results:
            print(f"{col_name:<25} | {count:<15} | {pct:.2f}%")

    print("=" * 50 + "\n")

def check_data_quality(df: DataFrame):
    check_duplicates(df)
    check_missing_values(df)

def remove_duplicates(df: DataFrame) -> DataFrame:
    print(f"\n--- Очищення дублікатів ---")

    initial_count = df.count()
    df_cleaned = df.dropDuplicates(subset=[c for c in df.columns if c != "ID"])

    final_count = df_cleaned.count()
    removed = initial_count - final_count

    print(f"Видалено записів: {removed} ({(removed / initial_count) * 100:.4f}%)")

    return df_cleaned

def handle_missing_values(df: DataFrame) -> DataFrame:
    print("\n--- Опрацювання пропусків ---")
    initial_count = df.count()

    critical_geo_cols = ['City', 'Zipcode', 'Zipcode_Base', 'Timezone']
    subset_to_drop = [c for c in critical_geo_cols if c in df.columns]

    df = df.dropna(subset=subset_to_drop)

    num_cols = ["Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)"]
    medians = {}

    agg_exprs = [F.percentile_approx(F.col(c), 0.5).alias(c) for c in num_cols]
    medians = df.select(agg_exprs).collect()[0].asDict()

    df = df.withColumn("Precipitation(in)",
                       F.when(F.col("Precipitation(in)").isNotNull(), F.col("Precipitation(in)"))
                       .when(F.col("Weather_Condition").isin(DRY_CONDITIONS) | F.col("Weather_Condition").isNull(), 0.0)
                       .otherwise(medians["Precipitation(in)"])
                       )

    df = df.withColumn("Visibility(mi)",
                       F.when(F.col("Visibility(mi)").isNotNull(), F.col("Visibility(mi)"))
                       .when(F.col("Weather_Condition").isin(PERFECT_VISIBILITY_CONDITIONS), 10.0)
                       .when(F.col("Weather_Condition").isin(POOR_VISIBILITY_CONDITIONS), 1.0)
                       .otherwise(medians["Visibility(mi)"])
                       )

    df = df.fillna({c: medians[c] for c in num_cols if c in medians and c not in ["Precipitation(in)", "Visibility(mi)"]})

    df = df.fillna({"Weather_Condition": "Unknown", "Wind_Direction": "Unknown"})

    final_count = df.count()
    print(f"Очищення завершено. Видалено рядків: {initial_count - final_count}")

    return df