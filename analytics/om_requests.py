from pyspark.sql import functions as F, Window


def get_severity_by_weather(olap_tables):
    facts = olap_tables["accidents_fact"]
    weather = olap_tables["weather_dim"]

    return (facts.join(weather, "WeatherID")
        .groupBy("Weather_Condition")
        .agg(
            F.count("ID").alias("Total_Accidents"),
            F.avg("Severity").alias("Avg_Severity")
        )
        .orderBy(F.desc("Total_Accidents")))

def get_top_dangerous_hours_per_state(olap):
    state_hour_stats = (olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["time_dim"], "TimeID")
        .groupBy("State", "Hour")
        .agg(F.count("ID").alias("Accident_Volume")))

    window_rank = Window.partitionBy("State").orderBy(F.desc("Accident_Volume"))

    return (state_hour_stats.withColumn("Rank", F.dense_rank().over(window_rank))
        .filter(F.col("Rank") <= 3)
        .orderBy("State", "Rank"))

def get_hourly_trend_by_state(olap):
    hourly_data = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["time_dim"]), "TimeID")
        .groupBy("State", "Year", "Month", "DayOfWeek", "Hour")
        .agg(F.count("ID").alias("count")))

    window_spec = Window.partitionBy("State").orderBy("Year", "Month", "DayOfWeek", "Hour")
    return (hourly_data
        .withColumn("Prev_Hour_Count", F.lag("count", 1).over(window_spec))
        .withColumn("Diff", F.col("count") - F.col("Prev_Hour_Count"))
        .filter(F.col("Prev_Hour_Count").isNotNull())
        .orderBy(F.col("count").desc())
    )

def get_night_accident_percentage(olap):
    total_by_city = (olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .groupBy("State", "City")
        .agg(F.count("ID").alias("Total_Accidents")))

    night_by_city = (olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["light_dim"], "LightID")
        .filter(F.col("Light_Level") == "0")
        .groupBy("State", "City")
        .agg(F.count("ID").alias("Night_Accidents")))

    return (night_by_city.join(total_by_city, ["State", "City"])
        .withColumn("Night_Ratio", F.round((F.col("Night_Accidents") / F.col("Total_Accidents")) * 100, 2))
        .filter(F.col("Total_Accidents") > 50)
        .orderBy(F.desc("Night_Ratio")))