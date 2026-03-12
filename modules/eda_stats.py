import os
import seaborn as sns
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
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