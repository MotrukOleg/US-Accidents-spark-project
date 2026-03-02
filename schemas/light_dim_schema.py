from pyspark.sql.types import *

light_dim_schema = StructType([
    StructField("LightID", IntegerType(), False),
    StructField("Sunrise_Sunset", StringType(), False),
    StructField("Civil_Twilight", StringType(), False),
    StructField("Nautical_Twilight", StringType(), False),
    StructField("Astronomical_Twilight", StringType(), False),
])