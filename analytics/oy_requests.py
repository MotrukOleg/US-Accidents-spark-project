from pyspark.sql import functions as F, Window
from analytics.utils import save_results


def oy_requests(olap):
    analysis_functions = [
        ("peak_hour_cities", get_peak_hour_cities_per_state),
        ("severe_weather_highways", get_severe_weather_on_highways),
        ("weekend_congestion_anomaly", get_weekend_vs_weekday_congestion),
        ("mom_accident_trend", get_mom_accident_trend),
        ("precipitation_impact", get_precipitation_impact_by_state),
        ("yoy_growth_surge", get_yoy_surge_cities)
    ]

    for i, (file_name, func) in enumerate(analysis_functions, start=1):
        print(f"{i}.")

        df_result = func(olap)
        df_result.show(10, False)

        print(f"--- План виконання трансформацій для '{i}.' ---")
        df_result.explain()

        save_results(df_result, file_name, "olesia_yankiv")

        print("-" * 60 + "\n")


def get_peak_hour_cities_per_state(olap):
    df = (
        olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["time_dim"], "TimeID")
        .filter(F.col("Hour").isin([7, 8, 9, 17, 18, 19]))
    )

    city_stats = df.groupBy("State", "City").agg(F.count("ID").alias("Accident_Count"))

    window_spec = Window.partitionBy("State").orderBy(F.desc("Accident_Count"))

    return (
        city_stats
        .withColumn("Rank", F.rank().over(window_spec))
        .filter(F.col("Rank") <= 3)
        .orderBy("State", "Rank")
    )


def get_severe_weather_on_highways(olap):
    return (
        olap["accidents_fact"]
        .join(olap["weather_dim"], "WeatherID")
        .join(olap["road_dim"], "RoadID")
        .filter(
            (F.col("Severity") > 3) &
            (F.col("Traffic_Signal") == False) &
            (F.col("Crossing") == False) &
            (F.col("Junction") == False)
        )
        .groupBy("Weather_Condition")
        .agg(F.count("ID").alias("Severe_Accidents"))
        .orderBy(F.desc("Severe_Accidents"))
        .limit(3)
    )


def get_weekend_vs_weekday_congestion(olap):
    base = (
        olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["time_dim"], "TimeID")
    )

    stats = base.groupBy("State").agg(
        (F.sum(F.when(F.col("DayOfWeek").isin([1, 7]), F.col("Distance(mi)")).otherwise(0)) / 2)
        .alias("Avg_Weekend_Dist"),
        (F.sum(F.when(~F.col("DayOfWeek").isin([1, 7]), F.col("Distance(mi)")).otherwise(0)) / 5)
        .alias("Avg_Weekday_Dist")
    )

    return (
        stats
        .withColumn(
            "Intensity_Diff",
            F.round(F.col("Avg_Weekend_Dist") - F.col("Avg_Weekday_Dist"), 2)
        )
        .orderBy(F.desc("Avg_Weekend_Dist"))
        .limit(5)
    )


def get_mom_accident_trend(olap):
    monthly_data = (
        olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["time_dim"], "TimeID")
        .groupBy("State", "Year", "Month")
        .agg(F.count("ID").alias("Monthly_Accidents"))
    )

    window_spec = Window.partitionBy("State").orderBy("Year", "Month")

    return (
        monthly_data
        .withColumn("Prev_Month_Count", F.lag("Monthly_Accidents").over(window_spec))
        .withColumn("MoM_Change", F.col("Monthly_Accidents") - F.col("Prev_Month_Count"))
        .filter(F.col("Prev_Month_Count").isNotNull())
        .orderBy("State", "Year", "Month")
    )


def get_precipitation_impact_by_state(olap):
    base = (
        olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["weather_dim"], "WeatherID")
    )

    state_totals = base.groupBy("State").agg(
        F.count("ID").alias("Total_Accidents"),
        F.count(
            F.when(F.col("Weather_Condition").rlike("(?i)Rain|Snow|Storm"), 1)
        ).alias("Precip_Accidents")
    )

    return (
        state_totals
        .withColumn(
            "Precip_Ratio",
            F.round((F.col("Precip_Accidents") / F.col("Total_Accidents")) * 100, 2)
        )
        .orderBy(F.desc("Precip_Ratio"))
        .limit(5)
    )


def get_yoy_surge_cities(olap):
    city_year_data = (
        olap["accidents_fact"]
        .join(olap["location_dim"], "LocationID")
        .join(olap["time_dim"], "TimeID")
        .groupBy("State", "City", "Year")
        .agg(F.count("ID").alias("Yearly_Count"))
    )

    window_city = Window.partitionBy("State", "City").orderBy("Year")

    return (
        city_year_data
        .withColumn("Prev_Year_Count", F.lag("Yearly_Count").over(window_city))
        .filter(F.col("Prev_Year_Count") > 20)
        .withColumn(
            "Growth_Pct",
            F.round(
                ((F.col("Yearly_Count") - F.col("Prev_Year_Count")) / F.col("Prev_Year_Count")) * 100,
                2
            )
        )
        .filter(F.col("Growth_Pct") > 50)
        .orderBy(F.desc("Growth_Pct"))
    )