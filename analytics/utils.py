import os

def save_results(df, name, user_prefix):
    folder_path = f"analytics_results/{user_prefix}"

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = f"{folder_path}/{name}.csv"
    df.toPandas().to_csv(file_path, index=False)