from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofweek, hour, unix_timestamp, round, substring

def parse_and_transform_features(df: DataFrame) -> DataFrame:

    df_parsed = (df
        .withColumn("Year", year(col("Start_Time")))
        .withColumn("Month", month(col("Start_Time")))
        .withColumn("DayOfWeek", dayofweek(col("Start_Time")))
        .withColumn("Hour", hour(col("Start_Time")))
        )
    
    duration_seconds = unix_timestamp(col("End_Time")) - unix_timestamp(col("Start_Time"))
    df_parsed = df_parsed.withColumn("Duration_Minutes", round(duration_seconds / 60, 2))

    df_parsed = df_parsed.withColumn("Zipcode_Base", substring(col("Zipcode"), 1, 5))
    
    return df_parsed