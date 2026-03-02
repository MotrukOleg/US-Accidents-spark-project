from pyspark.sql.types import *

location_dim_schema = StructType([
    StructField("LocationID", IntegerType(), False),
    StructField("Start_Lat", DoubleType(), False),
    StructField("Start_Lng", DoubleType(), False),
    StructField("End_Lat", DoubleType(), True),
    StructField("End_Lng", DoubleType(), True),
    StructField("Street", StringType(), False),
    StructField("City", StringType(), False),
    StructField("County", StringType(), False),
    StructField("State", StringType(), False),
    StructField("Zipcode", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("Timezone", StringType(), False),
    StructField("Airport_Code", StringType(), False),
])