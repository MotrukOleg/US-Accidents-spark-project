from pyspark.sql.types import *

fact_schema = StructType([
    StructField("AccidentID", StringType(), False),
    StructField("Source", StringType(), False),
    StructField("Severity", ByteType(), False),
    StructField("Start_Time", TimestampNTZType(), False),
    StructField("End_Time", TimestampNTZType(), False),
    StructField("Distance(mi)", DoubleType(), False),
    StructField("Description", StringType(), False),
    StructField("LightID", IntegerType(), False),
    StructField("WeatherID", IntegerType(), False),
    StructField("RoadID", IntegerType(), False),
    StructField("LocationID", IntegerType(), False),
])