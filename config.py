DATA_PATH = "data/US_Accidents_March23.csv"
OUTPUT_PLOT_DIR = "visualization/"

CATEGORICAL_COLUMNS = [
    "Source", "Severity", "Street", "City", "County", "State",
    "Zipcode", "Country", "Timezone", "Airport_Code",
    "Wind_Direction", "Weather_Condition", "Amenity", "Bump",
    "Crossing", "Give_Way", "Junction", "No_Exit", "Railway",
    "Roundabout", "Station", "Stop", "Traffic_Calming",
    "Traffic_Signal", "Turning_Loop", "Sunrise_Sunset",
    "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"
]

NUMERICAL_COLUMNS = [
    "Start_Lat", "Start_Lng", "End_Lat", "End_Lng", "Distance(mi)",
    "Temperature(F)", "Wind_Chill(F)", "Humidity(%)",
    "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)"
]