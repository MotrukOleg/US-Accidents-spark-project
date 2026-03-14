import os
import seaborn as sns
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType
from config import OUTPUT_PLOT_DIR


def get_metadata(df):
    print(f"Загальна кількість рядків: {df.count()}")
    print(f"Кількість колонок: {len(df.columns)}")


def get_categorical_stats(df, col_name):
    stats_df = df.groupBy(col_name).count().orderBy(F.desc("count"))
    return stats_df


def plot_categorical_feature(pd_df, col_name):
    os.makedirs(OUTPUT_PLOT_DIR, exist_ok=True)
    plt.figure(figsize=(10, 6))

    if len(pd_df) <= 5:
        plt.pie(pd_df['count'], labels=pd_df[col_name], autopct='%1.1f%%',
                colors=sns.color_palette('viridis', len(pd_df)))
        plt.title(f"Розподіл {col_name}")
    else:
        sns.barplot(data=pd_df, x=col_name, y='count', hue=col_name, palette='viridis', legend=False)
        plt.xticks(rotation=45)
        plt.title(f"Розподіл {col_name}")

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_PLOT_DIR, f"{col_name}_dist.png"))
    plt.close()


def run_categorical_eda(df, categorical_cols):
    for col_name in categorical_cols:
        if col_name in df.columns:
            print(f"\n--- Аналіз: {col_name} ---")

            stats_df = get_categorical_stats(df, col_name)
            stats_df.show(5)

            pd_df = stats_df.limit(10).toPandas()

            if not pd_df.empty:
                plot_categorical_feature(pd_df, col_name)


def get_numerical_stats(df, col_name):
    stats_df = df.select(
        F.count(F.col(col_name)).alias("count"),
        F.count(F.when(F.col(col_name).isNull(), 1)).alias("nulls"),
        F.mean(F.col(col_name)).alias("mean"),
        F.stddev(F.col(col_name)).alias("stddev"),
        F.min(F.col(col_name)).alias("min"),
        F.expr(f"percentile_approx(`{col_name}`, 0.25)").alias("q1"),
        F.expr(f"percentile_approx(`{col_name}`, 0.5)").alias("median"),
        F.expr(f"percentile_approx(`{col_name}`, 0.75)").alias("q3"),
        F.max(F.col(col_name)).alias("max")
    )

    return stats_df


def run_numerical_eda(df, numerical_cols):
    print("\n--- Статистика числових ознак ---")

    for col_name in numerical_cols:
        if col_name in df.columns:
            print(f"\n--- Аналіз: {col_name} ---")

            stats_df = get_numerical_stats(df, col_name)
            stats_df.show(truncate=False)


def plot_numerical_feature(df, col_name):
    os.makedirs(OUTPUT_PLOT_DIR, exist_ok=True)

    pd_df = df.select(col_name).dropna().toPandas()

    if not pd_df.empty:
        lower_bound = pd_df[col_name].quantile(0.01)
        upper_bound = pd_df[col_name].quantile(0.99)

        filtered_df = pd_df[
            (pd_df[col_name] >= lower_bound) &
            (pd_df[col_name] <= upper_bound)
        ]

        plt.figure(figsize=(10, 6))
        sns.histplot(data=filtered_df, x=col_name, kde=True)
        plt.title(f"Розподіл {col_name}")
        plt.xlim(lower_bound, upper_bound)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_PLOT_DIR, f"{col_name}_dist.png"))
        plt.close()


def run_numerical_plots(df, numerical_cols):
    for col_name in numerical_cols:
        if col_name in df.columns:
            print(f"\n--- Графік: {col_name} ---")

            plot_numerical_feature(df, col_name)