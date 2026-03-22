from pyspark.sql.types import *

time_dim_schema = StructType([
    StructField("TimeID", IntegerType(), False),
    StructField("Hour", IntegerType(), False),
    StructField("DayOfWeek", IntegerType(), False),
    StructField("Month", IntegerType(), False),
    StructField("Year", IntegerType(), False),
])