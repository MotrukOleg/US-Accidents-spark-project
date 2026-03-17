import pyspark.sql.functions as F
from pyspark.sql import DataFrame


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