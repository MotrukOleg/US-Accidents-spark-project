from pyspark.sql.types import *

light_dim_schema = StructType([
    StructField("LightID", IntegerType(), False),
    StructField("Light_level", StringType(), False),
])