from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from schemas.road_dim_schema import road_dim_schema
from schemas.light_dim_schema import light_dim_schema
from schemas.location_dim_schema import location_dim_schema
from schemas.weather_dim_schema import weather_dim_schema
from schemas.time_dim_schema import time_dim_schema
from schemas.fact_schema import fact_schema

def create_olap(df):
    road_cols = get_safe_cols(road_dim_schema, "RoadID", df)

    road_dim = (df.select(*road_cols)
                .distinct()
                .withColumn("RoadID", F.monotonically_increasing_id())
                )

    light_dim = (df.select("Light_Level")
                 .distinct()
                 .withColumn("LightID", F.monotonically_increasing_id())
                 )

    location_cols = get_safe_cols(location_dim_schema, "LocationID", df)

    location_dim = (df.select(*location_cols)
                    .distinct()
                    .withColumn("LocationID", F.monotonically_increasing_id())
                    )

    weather_cols = get_safe_cols(weather_dim_schema, "WeatherID", df)

    weather_dim = (df.select(*weather_cols)
                   .distinct()
                   .withColumn("WeatherID", F.monotonically_increasing_id())
                   )

    time_cols = get_safe_cols(time_dim_schema, "TimeID", df)

    time_dim = (df.select(*time_cols)
                .distinct()
                .withColumn("TimeID", F.monotonically_increasing_id())
                )

    road_keys = [c for c in road_cols if c in df.columns]
    loc_keys = [c for c in location_cols if c in df.columns]
    weath_keys = [c for c in weather_cols if c in df.columns]
    time_keys = [c for c in time_cols if c in df.columns]

    fact_joined = (df
        .join(broadcast(road_dim), road_keys, "left")
        .join(broadcast(location_dim), loc_keys, "left")
        .join(broadcast(weather_dim), weath_keys, "left")
        .join(broadcast(light_dim), "Light_Level", "left")
        .join(broadcast(time_dim), time_keys, "left"))

    accidents_cols = get_safe_cols(fact_schema, None, fact_joined)
    final_selection = [c for c in accidents_cols if c in fact_joined.columns]

    accidents_fact = fact_joined.select(*final_selection)

    return {
        "road_dim": road_dim,
        "light_dim": light_dim,
        "location_dim": location_dim,
        "weather_dim": weather_dim,
        "time_dim": time_dim,
        "accidents_fact": accidents_fact
    }

def get_safe_cols(schema, id_name=None, df_actual=None):
    return [field.name for field in schema.fields
            if field.name != id_name and field.name in df_actual.columns]

def check_olap_dimensions(dimensions):
    for dim_df, dim_name in dimensions.items():
        print(f"\n--- Перевірка виміру: {dim_name} ---")
        dim_name.show(5)
