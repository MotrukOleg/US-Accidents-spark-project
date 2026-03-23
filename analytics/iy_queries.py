from pyspark.sql import functions as F, Window


def iy_queries(olap):
    get_duration_by_infrastructure(olap).show(10, False)
    get_duration_by_infrastructure(olap).explain()

    get_extreme_temperature_severity(olap).show(10, False)
    get_extreme_temperature_severity(olap).explain()

    get_post_peak_drop_by_city(olap).show(10, False)
    get_post_peak_drop_by_city(olap).explain()

    get_distance_by_precipitation(olap).show(10, False)
    get_distance_by_precipitation(olap).explain()

    get_weekday_instability_by_city(olap).show(10, False)
    get_weekday_instability_by_city(olap).explain()

    get_high_severity_share_by_state(olap).show(10, False)
    get_high_severity_share_by_state(olap).explain()


def get_duration_by_infrastructure(olap):
    infra_types = [
        c for c in olap["road_dim"].columns
        if c != "RoadID"
    ]

    base_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["road_dim"]), "RoadID")
        .filter(F.col("Duration_Minutes").isNotNull())
        .filter(F.col("Duration_Minutes") > 10)
    )

    exploded_df = (base_df
        .select(
            "ID",
            "Duration_Minutes",
            F.explode(F.array(*[
                F.when(F.col(c) == True, F.lit(c)) for c in infra_types
            ])).alias("Infrastructure_Type")
        )
        .filter(F.col("Infrastructure_Type").isNotNull())
    )

    return (exploded_df
        .groupBy("Infrastructure_Type")
        .agg(
            F.count("ID").alias("Total_Accidents"),
            F.round(F.avg("Duration_Minutes"), 2).alias("Avg_Duration_Minutes"),
            F.round(F.max("Duration_Minutes"), 2).alias("Max_Duration_Minutes")
        )
        .filter(F.col("Total_Accidents") >= 100)
        .orderBy(F.desc("Avg_Duration_Minutes"))
    )


def get_extreme_temperature_severity(olap):
    weather_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["weather_dim"]), "WeatherID")
        .filter(F.col("Temperature(F)").isNotNull())
        .withColumn(
            "Temperature_Group",
            F.when(F.col("Temperature(F)") < 32, "Freezing")
             .when(F.col("Temperature(F)") > 90, "Hot")
             .otherwise("Moderate")
        )
    )

    return (weather_df
        .groupBy("Temperature_Group")
        .agg(
            F.count("ID").alias("Total_Accidents"),
            F.round(F.avg("Severity"), 2).alias("Avg_Severity"),
            F.round(F.avg("Duration_Minutes"), 2).alias("Avg_Duration_Minutes")
        )
        .orderBy(F.desc("Avg_Severity"), F.desc("Total_Accidents"))
    )


def get_post_peak_drop_by_city(olap):
    city_hour_stats = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["time_dim"]), "TimeID")
        .filter(F.col("Hour").between(17, 20))
        .groupBy("State", "City", "Hour")
        .agg(F.count("ID").alias("Accident_Count"))
    )

    window_spec = Window.partitionBy("State", "City").orderBy("Hour")

    return (city_hour_stats
        .withColumn("Prev_Hour_Count", F.lag("Accident_Count", 1).over(window_spec))
        .withColumn("Diff", F.col("Accident_Count") - F.col("Prev_Hour_Count"))
        .filter(F.col("Prev_Hour_Count").isNotNull())
        .filter(F.col("Diff") < 0)
        .withColumn("Drop_Percent", F.round((-F.col("Diff") / F.col("Prev_Hour_Count")) * 100, 2))
        .orderBy(F.asc("Diff"))
    )


def get_distance_by_precipitation(olap):
    precipitation_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["weather_dim"]), "WeatherID")
        .filter(F.col("Distance(mi)").isNotNull())
        .filter(F.col("Precipitation(in)").isNotNull())
        .withColumn(
            "Precipitation_Flag",
            F.when(F.col("Precipitation(in)") > 0, "With_Precipitation")
             .otherwise("Without_Precipitation")
        )
    )

    return (precipitation_df
        .groupBy("Precipitation_Flag")
        .agg(
            F.count("ID").alias("Total_Accidents"),
            F.round(F.avg("Distance(mi)"), 2).alias("Avg_Distance"),
            F.round(F.avg("Severity"), 2).alias("Avg_Severity")
        )
        .orderBy(F.desc("Avg_Distance"))
    )


def get_weekday_instability_by_city(olap):
    city_day_stats = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["time_dim"]), "TimeID")
        .groupBy("State", "City", "DayOfWeek")
        .agg(F.count("ID").alias("Accident_Count"))
    )

    instability_df = (city_day_stats
        .groupBy("State", "City")
        .agg(
            F.sum("Accident_Count").alias("Total_Accidents"),
            F.round(F.avg("Accident_Count"), 2).alias("Avg_Daily_Accidents"),
            F.round(F.stddev("Accident_Count"), 2).alias("Stddev_Daily_Accidents")
        )
        .filter(F.col("Total_Accidents") >= 100)
        .fillna(0, subset=["Stddev_Daily_Accidents"])
    )

    rank_window = Window.partitionBy("State").orderBy(F.desc("Stddev_Daily_Accidents"))

    return (instability_df
        .withColumn("Rank", F.dense_rank().over(rank_window))
        .filter(F.col("Rank") <= 3)
        .orderBy("State", "Rank")
    )


def get_high_severity_share_by_state(olap):
    total_by_state = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .groupBy("State")
        .agg(F.count("ID").alias("Total_Accidents"))
    )

    severe_by_state = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .filter(F.col("Severity") >= 3)
        .groupBy("State")
        .agg(F.count("ID").alias("High_Severity_Accidents"))
    )

    return (severe_by_state
        .join(total_by_state, "State")
        .withColumn(
            "High_Severity_Share",
            F.round((F.col("High_Severity_Accidents") / F.col("Total_Accidents")) * 100, 2)
        )
        .orderBy(F.desc("High_Severity_Share"))
    )