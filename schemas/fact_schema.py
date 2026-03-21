from pyspark.sql.types import *

fact_schema = StructType([
    StructField("ID", StringType(), False),
    StructField("Severity", ByteType(), False),
    StructField("Start_Time", TimestampNTZType(), False),
    StructField("End_Time", TimestampNTZType(), False),
    StructField("Duration_Minutes", IntegerType(), False),
    StructField("Distance(mi)", DoubleType(), False),
    StructField("LightID", IntegerType(), False),
    StructField("WeatherID", IntegerType(), False),
    StructField("RoadID", IntegerType(), False),
    StructField("LocationID", IntegerType(), False),
    StructField("TimeID", IntegerType(), False),
])