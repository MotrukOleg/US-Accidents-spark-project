from pyspark.sql.types import *

location_dim_schema = StructType([
    StructField("LocationID", IntegerType(), False),
    StructField("Start_Lat", DoubleType(), False),
    StructField("Start_Lng", DoubleType(), False),
    StructField("City", StringType(), False),
    StructField("County", StringType(), False),
    StructField("State", StringType(), False),
    StructField("Zipcode_Base", StringType(), False),
    StructField("Timezone", StringType(), False),
])