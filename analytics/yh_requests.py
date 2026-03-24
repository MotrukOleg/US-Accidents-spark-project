from pyspark.sql import functions as F, Window

def yh_requests(olap):
    get_severe_freezing_no_signal_locations(olap).show(10, False)
    get_severe_freezing_no_signal_locations(olap).explain()

    get_junction_distance_by_light_level(olap).show(10, False)
    get_junction_distance_by_light_level(olap).explain()

    get_high_wind_spike_days(olap).show(10, False)
    get_high_wind_spike_days(olap).explain()

    get_county_severity_rank_rain_snow(olap).show(10, False)
    get_county_severity_rank_rain_snow(olap).explain()

    get_top_infra_long_jams_top_states(olap).show(10, False)
    get_top_infra_long_jams_top_states(olap).explain()

    get_visibility_impact_by_timezone(olap).show(10, False)
    get_visibility_impact_by_timezone(olap).explain()

def get_county_severity_rank_rain_snow(olap):
    base_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["weather_dim"]), "WeatherID")
        .filter(F.col("Weather_Condition").isin("Rain", "Snow"))
    )

    grouped_df = (base_df
        .groupBy("State", "County")
        .agg(
            F.round(F.avg("Severity"), 2).alias("Avg_Severity"),
            F.count("ID").alias("Accident_Count")
        )
        .filter(F.col("Accident_Count") >= 10)
    )

    window_spec = Window.partitionBy("State").orderBy(F.desc("Avg_Severity"))

    return (grouped_df
        .withColumn("Severity_Rank", F.dense_rank().over(window_spec))
        .orderBy("State", "Severity_Rank")
    )


def get_junction_distance_by_light_level(olap):
    return (olap["accidents_fact"]
        .join(F.broadcast(olap["road_dim"]), "RoadID")
        .join(F.broadcast(olap["light_dim"]), "LightID", "left")
        .filter(F.col("Junction") == True)
        .filter(F.col("Light_Level").isNotNull())
        .groupBy("Light_Level")
        .agg(
            F.count("ID").alias("Accident_Count"),
            F.round(F.avg("Distance(mi)"), 4).alias("Avg_Distance_Miles")
        )
        .orderBy(F.desc("Light_Level"))
    )


def get_high_wind_spike_days(olap):
    base_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["weather_dim"]), "WeatherID")
        .withColumn("Accident_Date", F.to_date("Start_Time"))
    )

    daily_state_counts = (base_df
        .groupBy("State", "Accident_Date")
        .agg(
            F.count("ID").alias("Daily_Count"),
            F.max("Wind_Speed(mph)").alias("Max_Wind_Speed") 
        )
    )

    days = lambda i: i * 86400
    window_spec = (Window.partitionBy("State")
                   .orderBy(F.unix_timestamp("Accident_Date"))
                   .rangeBetween(-days(14), -days(1)))

    return (daily_state_counts
        .withColumn("Prev_14_Day_Avg", F.round(F.avg("Daily_Count").over(window_spec), 2))
        .filter(F.col("Prev_14_Day_Avg").isNotNull())
        .filter((F.col("Daily_Count") > F.col("Prev_14_Day_Avg")) & (F.col("Max_Wind_Speed") > 20))
        .orderBy(F.desc("Max_Wind_Speed"))
    )


def get_top_infra_long_jams_top_states(olap):
    top_states_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .groupBy("State")
        .count()
        .orderBy(F.desc("count"))
        .limit(5)
        .select("State")
    )

    infra_types = [c for c in olap["road_dim"].columns if c != "RoadID"]

    long_jams = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(top_states_df), "State")
        .join(F.broadcast(olap["road_dim"]), "RoadID")
        .filter(F.col("Distance(mi)") > 1.0)
    )

    exploded_jams = (long_jams
        .select(
            "ID",
            F.explode(F.array(*[
                F.when(F.col(c) == True, F.lit(c)) for c in infra_types
            ])).alias("Infrastructure_Type")
        )
        .filter(F.col("Infrastructure_Type").isNotNull())
    )

    return (exploded_jams
        .groupBy("Infrastructure_Type")
        .agg(F.count("ID").alias("Jam_Count"))
        .orderBy(F.desc("Jam_Count"))
        .limit(3)
    )


def get_visibility_impact_by_timezone(olap):
    base_df = (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["weather_dim"]), "WeatherID")
        .filter(F.col("Visibility(mi)").isNotNull())
        .withColumn(
            "Visibility_Category",
            F.when(F.col("Visibility(mi)") < 1.0, "Low (<1mi)")
             .when(F.col("Visibility(mi)") > 10.0, "Perfect (>10mi)")
             .otherwise("Normal")
        )
        .filter(F.col("Visibility_Category") != "Normal")
        .withColumn("Accident_Date", F.to_date("Start_Time"))
    )

    daily_stats = (base_df
        .groupBy("Timezone", "Visibility_Category", "Accident_Date")
        .agg(F.count("ID").alias("Daily_Accidents"))
    )

    return (daily_stats
        .groupBy("Timezone", "Visibility_Category")
        .agg(
            F.round(F.avg("Daily_Accidents"), 2).alias("Avg_Daily_Accidents"),
            F.sum("Daily_Accidents").alias("Total_Accidents")
        )
        .orderBy("Timezone", "Visibility_Category")
    )


def get_severe_freezing_no_signal_locations(olap):
    return (olap["accidents_fact"]
        .join(F.broadcast(olap["location_dim"]), "LocationID")
        .join(F.broadcast(olap["weather_dim"]), "WeatherID")
        .join(F.broadcast(olap["road_dim"]), "RoadID")
        .filter(F.col("Severity") >= 3)
        .filter(F.col("Temperature(F)") < 32.0)
        .filter(F.col("Traffic_Signal") == False)
        .groupBy("State", "City", "Zipcode_Base") 
        .agg(F.count("ID").alias("Severe_Accidents_Count"))
        .orderBy(F.desc("Severe_Accidents_Count"))
        .limit(10)
    )