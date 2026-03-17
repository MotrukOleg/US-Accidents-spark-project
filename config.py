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

DRY_CONDITIONS = [
    "Clear", "Fair", "Fair / Windy", "Cloudy", "Cloudy / Windy",
    "Mostly Cloudy", "Mostly Cloudy / Windy", "Partly Cloudy",
    "Partly Cloudy / Windy", "Scattered Clouds", "Overcast", "Haze",
    "Haze / Windy", "Light Haze", "Fog", "Fog / Windy", "Light Fog",
    "Patches of Fog", "Partial Fog", "Shallow Fog", "Light Freezing Fog",
    "Smoke", "Smoke / Windy", "Heavy Smoke", "Dust Whirls", "Blowing Dust",
    "Blowing Dust / Windy", "Widespread Dust", "Widespread Dust / Windy",
    "Blowing Sand", "Sand", "Funnel Cloud", "Volcanic Ash", "N/A Precipitation"
]

PERFECT_VISIBILITY_CONDITIONS = [
    "Clear", "Fair", "Fair / Windy", "Mostly Cloudy", "Mostly Cloudy / Windy",
    "Partly Cloudy", "Partly Cloudy / Windy", "Scattered Clouds"
]

POOR_VISIBILITY_CONDITIONS = [
    "Fog", "Fog / Windy", "Light Fog", "Patches of Fog", "Partial Fog",
    "Shallow Fog", "Light Freezing Fog", "Haze", "Haze / Windy", "Light Haze",
    "Smoke", "Smoke / Windy", "Heavy Smoke", "Dust Whirls", "Blowing Dust",
    "Blowing Dust / Windy", "Widespread Dust", "Widespread Dust / Windy",
    "Blowing Sand", "Sand", "Blowing Snow", "Heavy Blowing Snow", "Mist", "Squalls"
]