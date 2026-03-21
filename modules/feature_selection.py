from pyspark.sql.functions import col, when

def select_features(df):
    return (df
            .transform(merge_features)
            .transform(delete_features)
           )

def delete_features(df):

    no_column_needed = ["Source"]
    lot_of_missing_values = ["End_Lat", "End_Lng"]
    highly_correlated = ["Wind_Chill(F)", "Airport_Code","Weather_Timestamp"]
    twilight_cols = ["Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]
    other = ["Description","Street","Country","Zipcode","Turning_Loop"]

    to_drop = (
            no_column_needed +
            lot_of_missing_values +
            highly_correlated +
            twilight_cols +
            other
    )

    df = df.drop(*to_drop)

    return df

def change_twilights_values(df):

    """
    Sunrise and sunset, civil twilight, nautical twilight, astronomical twilight are categorical features indicating
    the position of the sun at the time of the accident. To facilitate understanding of the level of road illumination
    at the time of the accident, it was decided to introduce the feature of the level of illumination
    which is calculated based on the features above according to the
    principle "Day" = 1, "Night" = 0.
    Accordingly:
    0 - Complete darkness, even the sun is far below the horizon.
    1 - The sky is starting to get a little brighter, but the stars are still clearly visible.
    2 - The horizon line on the sea can already be distinguished.
    3 - The most active twilight; usually this is the time when street lighting is turned on.
    4 - The sun has already risen above the horizon.
    """

    twilight_cols = [
        'Astronomical_Twilight',
        'Nautical_Twilight',
        'Civil_Twilight',
        'Sunrise_Sunset'
    ]

    for c in twilight_cols:
        df = df.withColumn(c, when(col(c) == 'Day', 1).otherwise(0).cast("int"))

    return df

def merge_features(df):

    """
    aggregate the features related to traffic control, road geometry and surroundings into three new features:
    - Traffic_Control_Presence: Indicates whether any traffic control feature is present (1) or not (0).
    - Road_Geometry_Presence: Indicates whether any road geometry feature is present (1) or not (0).
    - Surroundings_Presence: Indicates whether any surroundings feature is present (1) or not (0).
    """

    df = change_twilights_values(df)

    df = df.withColumn("Light_Level",
                       col('Astronomical_Twilight') + col('Nautical_Twilight') +
                       col('Civil_Twilight') + col('Sunrise_Sunset')
                       )

    return df