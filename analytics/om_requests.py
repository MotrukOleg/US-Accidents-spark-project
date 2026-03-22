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
        .orderBy(F.desc("Night_Ratio"))
            )

def get_infrastructure_contribution_by_state(olap):
    infra_types = ["Traffic_Signal", "Crossing", "Junction", "Stop"]

    base_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["road_dim"]), "RoadID")
               )

    contribution_df = base_df.groupBy("State").agg(
        F.count("ID").alias("Total_State_Accidents"),
        *[F.sum(F.when(F.col(c) == True, 1).otherwise(0)).alias(c) for c in infra_types]
    )

    for c in infra_types:
        contribution_df = contribution_df.withColumn(
            f"{c}_Percent",
            F.round((F.col(c) / F.col("Total_State_Accidents")) * 100, 2)
        )

    return (contribution_df.select("State", "Total_State_Accidents",
                                  *[f"{c}_Percent" for c in infra_types])
                            .orderBy(F.desc("Total_State_Accidents"))
            )

def get_extreme_delay_anomalies(olap):
    df = olap["accidents_fact"].join(F.broadcast(olap["location_dim"]), "LocationID")

    city_window = Window.partitionBy("State", "City")

    return (df.withColumn("City_Avg_Duration", F.avg("Duration_Minutes").over(city_window))
             .withColumn("Deviation_Factor", F.col("Duration_Minutes") / F.col("City_Avg_Duration"))
             .filter(
                 (F.col("Duration_Minutes") > 60) &
                 (F.col("Deviation_Factor") > 2.5)
             )
             .select("ID", "State", "City", "Duration_Minutes",
                     F.round("City_Avg_Duration", 2).alias("Avg_In_City"),
                     F.round("Deviation_Factor", 2).alias("Anomaly_Index"))
             .orderBy(F.desc("Deviation_Factor"))
            )