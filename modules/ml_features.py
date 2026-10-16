"""Підготовка даних для ML-задач (Severity / Duration_Minutes): інженерія ознак, кодування, оцінка зв'язку."""
import math

import numpy as np
import pandas as pd
from pyspark.sql import Column, DataFrame, Window, functions as F
from scipy.stats.contingency import association
from sklearn.feature_selection import f_classif
from sklearn.preprocessing import TargetEncoder

ROAD_BOOL = ["Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit", "Railway",
             "Roundabout", "Station", "Stop", "Traffic_Calming", "Traffic_Signal"]

WEATHER_NUM = ["Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)"]
TWILIGHT = ["Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]

# Ключові слова з Description. Опис складається разом із записом про ДТП і прямо описує
# вплив на рух (перекриття, блокування смуг), тобто фактично дублює ціль — використовуємо
# лише для діагностики витоку, а не як робочі ознаки.
DESC_PATTERNS = {
    "Desc_Closed": r"clos(ed|ure)",
    "Desc_Blocked": r"block",
    "Desc_Lane": r"\blanes?\b",
    "Desc_Shoulder": r"shoulder",
    "Desc_Ramp_Exit": r"\bramp\b|\bexit",
    "Desc_Slow_Traffic": r"slow traffic|delay|queu|stationary|stop and go",
    "Desc_Serious": r"multi-vehicle|multiple vehicle|overturn|jackknif|serious|fatal|injur",
    "Desc_Hazard": r"fire|spill|hazmat|debris",
}


def normalize_wind_direction(c: Column) -> Column:
    c = F.upper(F.trim(c))
    return (F.when(c.isNull() | (c == "") | (c == "UNKNOWN"), "UNKNOWN")
             .when(c == "CALM", "CALM")
             .when(c.isin("VAR", "VARIABLE"), "VAR")
             .when(c == "NORTH", "N").when(c == "SOUTH", "S")
             .when(c == "EAST", "E").when(c == "WEST", "W")
             .otherwise(c))


def weather_group(c: Column) -> Column:
    w = F.lower(c)
    return (F.when(w.isNull() | (w == "unknown"), "Unknown")
             .when(w.rlike("thunder|t-storm|tornado|funnel|squall"), "Thunderstorm")
             .when(w.rlike("snow|sleet|ice pellets|hail|wintry|freezing rain|freezing drizzle"), "Snow_Ice")
             .when(w.rlike("rain|drizzle|shower"), "Rain")
             .when(w.rlike("fog|mist|haze|smoke|dust|sand|ash"), "Low_Visibility")
             .when(w.rlike("cloud|overcast"), "Cloudy")
             .when(w.rlike("clear|fair"), "Clear")
             .otherwise("Other"))


def street_type(c: Column) -> Column:
    """Тип дороги за назвою вулиці: магістралі суттєво відрізняються від локальних вулиць."""
    s = F.lower(F.trim(c))
    return (F.when(s.isNull() | (s == ""), "Unknown")
             .when(s.rlike(r"^i-|^i \d|interstate|\bi-\d"), "Interstate")
             .when(s.rlike(r"^us-|^us \d|us highway|us hwy|us route|\bus-\d"), "US_Highway")
             .when(s.rlike(r"expy|expressway|fwy|freeway|tpke|turnpike|tollway|toll rd|beltway|pkwy|parkway"),
                   "Expressway")
             .when(s.rlike(r"^[a-z]{2}-\d|^sr[- ]\d|state route|state hwy|state highway|state road|"
                           r"^route \d|^rte? \d|^hwy|highway|^county road|^cr[- ]\d|^fm[- ]\d"), "Route_Hwy")
             .when(s.rlike(r"\b(ave|avenue|blvd|boulevard)\b"), "Avenue_Blvd")
             .when(s.rlike(r"\b(st|street|rd|road|dr|drive|ln|lane|way|ct|court|cir|circle|pl|place|"
                           r"ter|terrace|trl|trail|pike)\b"), "Local_Street")
             .otherwise("Other"))


def raw_extras(raw_df: DataFrame) -> DataFrame:
    """Колонки з сирих даних, яких немає після select_features / handle_missing_values.

    * Source, тип і нормалізована назва вулиці;
    * прапорці ключових слів Description (лише для перевірки витоку);
    * Missing_* — чи було значення невідоме в сирих даних (handle_missing_values заповнює погоду
      медіаною / "Unknown", а select_features перетворює невідомі twilight-значення на «ніч»).
    """
    desc = F.lower(F.coalesce(F.col("Description"), F.lit("")))
    cols = [
        F.col("ID"),
        F.col("Source"),
        street_type(F.col("Street")).alias("Street_Type"),
        F.coalesce(F.lower(F.trim(F.col("Street"))), F.lit("unknown")).alias("Street_Norm"),
    ]
    cols += [desc.rlike(p).cast("int").alias(name) for name, p in DESC_PATTERNS.items()]
    direction = F.regexp_extract(desc, r"(northbound|southbound|eastbound|westbound|\bnb\b|\bsb\b|\beb\b|\bwb\b)", 1)
    cols.append(F.upper(F.substring(direction, 1, 1)).alias("Desc_Direction"))   # N / S / E / W або ""
    cols += [F.col(c).isNull().cast("int").alias(f"Missing_{c}") for c in WEATHER_NUM]
    cols += [
        (F.col("Weather_Condition").isNull() | (F.trim(F.col("Weather_Condition")) == "")).cast("int")
        .alias("Missing_Weather_Condition"),
        (F.col("Wind_Direction").isNull() | (F.trim(F.col("Wind_Direction")) == "")).cast("int")
        .alias("Missing_Wind_Direction"),
        F.greatest(*[F.col(c).isNull().cast("int") for c in TWILIGHT]).alias("Missing_Twilight"),
    ]
    return raw_df.select(*cols).dropDuplicates(["ID"])


def engineer_features(df: DataFrame) -> DataFrame:
    two_pi = 2 * math.pi
    is_weekend = F.col("DayOfWeek").isin(1, 7)
    df = (df
          # день тижня — циклічно (неділя поруч із понеділком)
          .withColumn("DoW_sin", F.sin(two_pi * (F.col("DayOfWeek") - 1) / 7))
          .withColumn("DoW_cos", F.cos(two_pi * (F.col("DayOfWeek") - 1) / 7))
          .withColumn("Wind_Dir", normalize_wind_direction(F.col("Wind_Direction")))
          .withColumn("Weather_Group", weather_group(F.col("Weather_Condition")))
          .withColumn("Is_Windy", F.lower(F.col("Weather_Condition")).contains("windy").cast("int"))
          .withColumn("Light_Level", F.col("Light_Level").cast("double"))
          # нелінійні ефекти часу: година окремо для буднів і вихідних, місяць як категорія
          .withColumn("HourType", F.concat(F.lpad(F.col("Hour").cast("string"), 2, "0"),
                                           F.when(is_weekend, F.lit("_we")).otherwise(F.lit("_wd"))))
          .withColumn("Month_cat", F.lpad(F.col("Month").cast("string"), 2, "0"))
          # джерело даних × рік: методика фіксації Severity і End_Time змінювалась у часі
          .withColumn("Source_Year", F.concat_ws("_", F.col("Source"), F.col("Year").cast("string")))
          .withColumn("Key_Street", F.concat_ws("|", F.col("State"), F.col("Street_Norm")))
          .withColumn("Key_City", F.concat_ws("|", F.col("State"), F.col("City")))
          .withColumn("Key_County", F.concat_ws("|", F.col("State"), F.col("County")))
          .withColumn("Key_Zip", F.col("Zipcode_Base"))
          )
    for c in ROAD_BOOL:
        df = df.withColumn(c, F.col(c).cast("int"))
    return df


def incident_group() -> Column:
    """Ключ групи «місце + день»: записи однієї ДТП від різних джерел потрапляють в одну групу."""
    return F.xxhash64(F.round("Start_Lat", 3), F.round("Start_Lng", 3), F.to_date("Start_Time"))


def snapshot_groups(df: DataFrame, minutes: int = 15) -> DataFrame:
    """Позначає «знімки» однієї події: записи одного джерела в тому самому місці (координати до 0.0001° ≈ 11 м),
    на тій самій вулиці і в тому самому напрямку руху (з опису), що починаються з паузою ≤ minutes одна від одної.

    Збирач датасету багато разів опитував API трафіку і зберігав кожну зміну події (час завершення, тяжкість,
    опис) як новий запис з новим ID. Додає колонки: Snapshot_Group (ідентифікатор групи), Snapshot_Size,
    Snapshot_Severities (скільки різних значень Severity у групі).
    """
    ts = F.unix_timestamp("Start_Time")
    keys = [F.col("Source"), F.round("Start_Lat", 4), F.round("Start_Lng", 4), F.col("Street_Norm"), F.col("Desc_Direction")]
    w = Window.partitionBy(*keys).orderBy(ts, "ID")
    gap = ts - F.lag(ts).over(w)
    df = (df.withColumn("_new", F.when(gap.isNull() | (gap > minutes * 60), 1).otherwise(0))
            .withColumn("_k", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
            .withColumn("Snapshot_Group", F.xxhash64(*keys, F.col("_k")))
            .drop("_new", "_k"))
    g = Window.partitionBy("Snapshot_Group")
    return (df.withColumn("Snapshot_Size", F.count("*").over(g))
              .withColumn("Snapshot_Severities", F.size(F.collect_set("Severity").over(g))))


def is_placeholder_duration(c: Column, value: float = 360.0, tol: float = 0.05) -> Column:
    """Тривалість-заглушка (6 год за замовчуванням у Source1), включно з 359.98 / 360.02 через секунди."""
    return F.abs(c - F.lit(value)) <= F.lit(tol)


def clip_and_log(df: DataFrame, bounds: dict, log_cols) -> DataFrame:
    for c, (lo, hi) in bounds.items():
        df = df.withColumn(c, F.least(F.greatest(F.col(c), F.lit(lo)), F.lit(hi)))
    for c in log_cols:
        df = df.withColumn(c, F.log1p(F.col(c)))
    return df


# --------------------------------------------------------------------------- кодування високо-кардинальних ключів
def encode_locations(frames: dict, keys: dict, smoothing: float, fold_col: str = "fold") -> dict:
    """Target- і частотне кодування ключів місця (вулиця, місто, округ, індекс): sklearn.preprocessing.TargetEncoder.

    frames: {"train" | "valid" | "test": pandas.DataFrame з колонками ID, ключі, Severity, t_logdur; у train — ще fold}.
    Повертає {вибірка: DataFrame(ID, TE_*, Cnt_*)}:
    * TE_<назва>_t_sev{1,3,4} — згладжена частка класу Severity на ключі (target_type="multiclass"; частку класу 2
      відкинуто — частки в сумі дають 1, тобто вона лінійно залежна від інших);
    * TE_<назва>_t_logdur — згладжене середнє log1p(тривалості) (target_type="continuous"; лише рядки з якісною
      тривалістю, t_logdur не null);
    * Cnt_<назва> — log1p(кількість ІНШИХ train-записів з тим самим ключем).
    Train кодується out-of-fold по групових фолдах: для фолду k — енкодер, навчений на інших фолдах (запис не бачить
    власної цілі). Вбудований cv у TargetEncoder ділить дані випадково, тому фолди задаємо самі.
    Valid / test — енкодер, навчений на всьому train; невідомий ключ → загальне середнє.
    """
    names, cols = list(keys), list(keys.values())
    train = frames["train"]
    out = {s: pd.DataFrame({"ID": f["ID"].to_numpy()}) for s, f in frames.items()}

    for target, kind in [("Severity", "multiclass"), ("t_logdur", "continuous")]:
        known = train[target].notna().to_numpy()

        def fit(rows):
            return TargetEncoder(target_type=kind, smooth=smoothing).fit(train.loc[rows, cols], train.loc[rows, target])

        encoded = {"train": None}
        for k in np.unique(train[fold_col]):
            in_fold = (train[fold_col] == k).to_numpy()
            part = fit(known & ~in_fold).transform(train.loc[in_fold, cols])
            if encoded["train"] is None:
                encoded["train"] = np.empty((len(train), part.shape[1]))
            encoded["train"][in_fold] = part
        full = fit(known)
        for s, f in frames.items():
            if s != "train":
                encoded[s] = full.transform(f[cols])

        if kind == "multiclass":
            classes = list(full.classes_)
            col_names = [f"TE_{n}_t_sev{c}" if c != 2 else None for n in names for c in classes]
        else:
            col_names = [f"TE_{n}_t_logdur" for n in names]
        for s, arr in encoded.items():
            for j, name in enumerate(col_names):
                if name is not None:
                    out[s][name] = arr[:, j]

    for name, col in keys.items():
        counts = train[col].value_counts()
        for s, f in frames.items():
            n = f[col].map(counts).fillna(0).to_numpy()
            out[s][f"Cnt_{name}"] = np.log1p(n - 1 if s == "train" else n)
    return out


# --------------------------------------------------------------------------- зв'язок ознак із ціллю
def cramers_v(x: pd.Series, y: pd.Series) -> float:
    """Cramér's V двох категоріальних ознак: pandas.crosstab + scipy.stats.contingency.association."""
    ct = pd.crosstab(x, y)
    if min(ct.shape) < 2:
        return 0.0
    return float(association(ct.to_numpy(), method="cramer"))


def eta(groups: pd.Series, y: pd.Series) -> float:
    """Кореляційне відношення η числової y за групами: однофакторний ANOVA (sklearn.feature_selection.f_classif),
    η² = F·df₁ / (F·df₁ + df₂), df₁ = k − 1, df₂ = n − k."""
    k, n = groups.nunique(), len(groups)
    if k < 2:
        return 0.0
    f_value = f_classif(y.to_numpy().reshape(-1, 1), groups.astype(str).to_numpy())[0][0]
    if not np.isfinite(f_value):
        return 0.0
    df1, df2 = k - 1, n - k
    return float(np.sqrt(f_value * df1 / (f_value * df1 + df2)))
