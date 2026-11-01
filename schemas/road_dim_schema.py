from pyspark.sql.types import *

road_dim_schema = StructType([
    StructField("RoadID", IntegerType(), False),
    StructField("Amenity", BooleanType(), False),
    StructField("Bump", BooleanType(), False),
    StructField("Crossing", BooleanType(), False),
    StructField("Give_Way", BooleanType(), False),
    StructField("Junction", BooleanType(), False),
    StructField("No_Exit", BooleanType(), False),
    StructField("Railway", BooleanType(), False),
    StructField("Roundabout", BooleanType(), False),
    StructField("Station", BooleanType(), False),
    StructField("Stop", BooleanType(), False),
    StructField("Traffic_Calming", BooleanType(), False),
    StructField("Traffic_Signal", BooleanType(), False),
])