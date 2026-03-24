from pyspark.sql import functions as F, Window
from analytics.utils import save_results

def oy_requests(olap):
    print("--- ТОП-3 міста у кожному штаті в години пік ---")
    df1 = get_peak_hour_cities_per_state(olap)
    df1.show(10, False)
    df1.explain()
    save_results(df1, "peak_hour_cities", "olesia_yankiv")

    print("\n--- Погода та тяжкість ДТП на швидкісних ділянках ---")
    df2 = get_severe_weather_on_highways(olap)
    df2.show(10, False)
    df2.explain()
    save_results(df2, "severe_weather_highways", "olesia_yankiv")

    print("\n--- Порівняння заторів: Вихідні vs Будні ---")
    df3 = get_weekend_vs_weekday_congestion(olap)
    df3.show(5, False)
    df3.explain()
    save_results(df3, "weekend_congestion_anomaly", "olesia_yankiv")

    print("\n--- Щомісячна динаміка ДТП по штатах ---")
    df4 = get_mom_accident_trend(olap)
    df4.show(10, False)
    df4.explain()
    save_results(df4, "mom_accident_trend", "olesia_yankiv")

    print("\n--- Частка аварій під час опадів по штатах ---")
    df5 = get_precipitation_impact_by_state(olap)
    df5.show(5, False)
    df5.explain()
    save_results(df5, "precipitation_impact", "olesia_yankiv")

    print("\n--- Міста з різким зростанням ДТП в межах року ---")
    df6 = get_yoy_surge_cities(olap)
    df6.show(10, False)
    df6.explain()
    save_results(df6, "yoy_growth_surge", "olesia_yankiv")


def get_peak_hour_cities_per_state(olap):
    df = (olap["accidents_fact"]
          .join(olap["location_dim"], "LocationID")
          .join(olap["time_dim"], "TimeID")
          .filter(F.col("Hour").isin([7, 8, 9, 17, 18, 19])))
    city_stats = df.groupBy("State", "City").agg(F.count("ID").alias("Accident_Count"))
    window_spec = Window.partitionBy("State").orderBy(F.desc("Accident_Count"))
    return (city_stats.withColumn("Rank", F.rank().over(window_spec))
            .filter(F.col("Rank") <= 3)
            .orderBy("State", "Rank"))


def get_severe_weather_on_highways(olap):
    return (olap["accidents_fact"]
            .join(olap["weather_dim"], "WeatherID")
            .join(olap["road_dim"], "RoadID")
            .filter((F.col("Severity") > 3) &
                    (F.col("Traffic_Signal") == False) &
                    (F.col("Crossing") == False) &
                    (F.col("Junction") == False))
            .groupBy("Weather_Condition")
            .agg(F.count("ID").alias("Severe_Accidents"))
            .orderBy(F.desc("Severe_Accidents"))
            .limit(3))


def get_weekend_vs_weekday_congestion(olap):
    base = (olap["accidents_fact"]
            .join(olap["location_dim"], "LocationID")
            .join(olap["time_dim"], "TimeID"))
    stats = base.groupBy("State").agg(
        (F.sum(F.when(F.col("DayOfWeek").isin([1, 7]), F.col("Distance(mi)")).otherwise(0)) / 2).alias("Avg_Weekend_Dist"),
        (F.sum(F.when(~F.col("DayOfWeek").isin([1, 7]), F.col("Distance(mi)")).otherwise(0)) / 5).alias("Avg_Weekday_Dist")
    )
    return (stats.withColumn("Intensity_Diff", F.round(F.col("Avg_Weekend_Dist") - F.col("Avg_Weekday_Dist"), 2))
            .orderBy(F.desc("Avg_Weekend_Dist"))
            .limit(5))


def get_mom_accident_trend(olap):
    monthly_data = (olap["accidents_fact"]
                    .join(olap["location_dim"], "LocationID")
                    .join(olap["time_dim"], "TimeID")
                    .groupBy("State", "Year", "Month")
                    .agg(F.count("ID").alias("Monthly_Accidents")))
    window_spec = Window.partitionBy("State").orderBy("Year", "Month")
    return (monthly_data
            .withColumn("Prev_Month_Count", F.lag("Monthly_Accidents").over(window_spec))
            .withColumn("MoM_Change", F.col("Monthly_Accidents") - F.col("Prev_Month_Count"))
            .filter(F.col("Prev_Month_Count").isNotNull())
            .orderBy("State", "Year", "Month"))


def get_precipitation_impact_by_state(olap):
    base = (olap["accidents_fact"]
            .join(olap["location_dim"], "LocationID")
            .join(olap["weather_dim"], "WeatherID"))
    state_totals = base.groupBy("State").agg(
        F.count("ID").alias("Total_Accidents"),
        F.count(F.when(F.col("Weather_Condition").rlike("(?i)Rain|Snow|Storm"), 1)).alias("Precip_Accidents")
    )
    return (state_totals.withColumn("Precip_Ratio",
                                    F.round((F.col("Precip_Accidents") / F.col("Total_Accidents")) * 100, 2))
            .orderBy(F.desc("Precip_Ratio"))
            .limit(5))


def get_yoy_surge_cities(olap):
    city_year_data = (olap["accidents_fact"]
                      .join(olap["location_dim"], "LocationID")
                      .join(olap["time_dim"], "TimeID")
                      .groupBy("State", "City", "Year")
                      .agg(F.count("ID").alias("Yearly_Count")))
    window_city = Window.partitionBy("State", "City").orderBy("Year")
    return (city_year_data
            .withColumn("Prev_Year_Count", F.lag("Yearly_Count").over(window_city))
            .filter(F.col("Prev_Year_Count") > 20)
            .withColumn("Growth_Pct",
                        F.round(((F.col("Yearly_Count") - F.col("Prev_Year_Count")) / F.col("Prev_Year_Count")) * 100, 2))
            .filter(F.col("Growth_Pct") > 50)
            .orderBy(F.desc("Growth_Pct")))