from pyspark.sql import functions as F, Window

def yv_requests(olap):
    analysis_functions = [
        ("1. Середня видимість за тяжкістю", get_visibility_by_severity),
        ("2. ТОП POI для тяжких аварій (Severity 4)", get_top_poi_for_severe_accidents),
        ("3. Ранжування міст у кожному штаті", get_city_ranking_by_state),
        ("4. Максимальна тривалість за освітленістю", get_max_duration_by_light),
        ("5. Сезонність найтяжчих аварій", get_severe_accidents_seasonality),
        ("6. Відхилення кількості аварій міста від сер. по штату", get_city_accident_deviation)
    ]

    for title, func in analysis_functions:
        print(f"{title}")

        df_result = func(olap)
        df_result.show(10, False)

        print(f"--- План виконання трансформацій для '{title}' ---")
        df_result.explain()

        print("-" * 60 + "\n")

def get_visibility_by_severity(olap):
    return (olap["accidents_fact"]
            .join(olap["weather_dim"], "WeatherID")
            .groupBy("Severity")
            .agg(F.round(F.avg("Visibility(mi)"), 2).alias("Avg_Visibility"))
            .orderBy("Severity"))

def get_top_poi_for_severe_accidents(olap):
    poi_cols = ["Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
                "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming", "Traffic_Signal"]

    spark_session = olap["accidents_fact"].sparkSession

    df = (olap["accidents_fact"]
          .filter(F.col("Severity") == 4)
          .join(olap["road_dim"], "RoadID"))

    total_severe = df.count()

    any_poi_cond = F.col(poi_cols[0])
    for col in poi_cols[1:]:
        any_poi_cond = any_poi_cond | F.col(col)

    no_poi_count = df.filter(~any_poi_cond).count()

    agg_exprs = [F.sum(F.col(c).cast("int")).alias(c) for c in poi_cols]
    poi_stats = df.select(agg_exprs).collect()[0].asDict()

    data = [("Regular Road (No POI)", no_poi_count, round((no_poi_count / total_severe) * 100, 2))]
    for name, count in poi_stats.items():
        if count > 0:
            data.append((name, count, round((count / total_severe) * 100, 2)))

    return (spark_session.createDataFrame(data, ["Infrastructure_Type", "Count", "Percentage"])
            .orderBy(F.desc("Count")))

def get_city_ranking_by_state(olap):
    city_counts = (olap["accidents_fact"]
                   .join(olap["location_dim"], "LocationID")
                   .groupBy("State", "City")
                   .agg(F.count("ID").alias("Count")))

    window_spec = Window.partitionBy("State").orderBy(F.desc("Count"))

    return (city_counts
            .withColumn("Rank", F.rank().over(window_spec))
            .filter(F.col("Rank") <= 3)
            .orderBy("State", "Rank"))

def get_max_duration_by_light(olap):
    return (olap["accidents_fact"]
            .join(olap["light_dim"], "LightID")
            .groupBy("Light_Level")
            .agg(F.max("Duration_Minutes").alias("Max_Duration_Min"))
            .orderBy(F.desc("Max_Duration_Min")))

def get_severe_accidents_seasonality(olap):
    season_expr = (F.when(F.col("Month").isin(12, 1, 2), "Winter")
                   .when(F.col("Month").isin(3, 4, 5), "Spring")
                   .when(F.col("Month").isin(6, 7, 8), "Summer")
                   .otherwise("Autumn"))

    df = (olap["accidents_fact"]
          .filter(F.col("Severity") >= 3)
          .join(olap["time_dim"], "TimeID")
          .withColumn("Season", season_expr))

    season_counts = df.groupBy("Season").agg(F.count("ID").alias("Accident_Count"))

    total_window = Window.partitionBy()

    return (season_counts
            .withColumn("Total_Count", F.sum("Accident_Count").over(total_window))
            .withColumn("Percentage", F.round((F.col("Accident_Count") / F.col("Total_Count")) * 100, 2))
            .drop("Total_Count")
            .orderBy(F.desc("Accident_Count")))

def get_city_accident_deviation(olap):
    city_stats = (olap["accidents_fact"]
                  .join(olap["location_dim"], "LocationID")
                  .groupBy("State", "City")
                  .agg(F.count("ID").alias("City_Count")))

    state_window = Window.partitionBy("State")

    return (city_stats
            .withColumn("State_Avg", F.round(F.avg("City_Count").over(state_window), 2))
            .withColumn("Deviation", F.round(F.col("City_Count") - F.col("State_Avg"), 2))
            .orderBy(F.desc("Deviation")))