from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================================
# SILVER LAYER - Add Derived Columns
# ============================================================================
@dp.table(
    name="silver_famous_jarakaman",
    comment="Silver layer - enriched with derived time dimensions and KM extraction"
)
@dp.expect_or_drop("valid_devicetime", "devicetime IS NOT NULL")
def silver_famous_jarakaman():
    """
    Silver transformation:
    - Extract KM from geofence_name
    - Derive Interval (time range) from devicetime
    - Derive Hari (day name in Indonesian) from devicetime
    - Derive week_of_year in format YYYY-Www
    """
    # Read directly from source table
    df = spark.readStream.table("tc.default.famous_jarakaman_202604290427")
    
    # Extract KM from geofence_name (e.g., "KM 11" -> "11", "KM 3.5" -> "3.5")
    df = df.withColumn(
        "KM",
        F.regexp_extract(F.col("geofence_name"), r"KM\s+([\d.]+)", 1)
    )
    
    # Derive Interval (time range) from devicetime
    df = df.withColumn(
        "Interval",
        F.when((F.hour("devicetime") >= 0) & (F.hour("devicetime") < 6), "00:00-05:59")
         .when((F.hour("devicetime") >= 6) & (F.hour("devicetime") < 12), "06:00-11:59")
         .when((F.hour("devicetime") >= 12) & (F.hour("devicetime") < 18), "12:00-17:59")
         .when((F.hour("devicetime") >= 18) & (F.hour("devicetime") < 24), "18:00-23:59")
         .otherwise("Unknown")
    )
    
    # Derive Hari (day name in Indonesian) from devicetime
    df = df.withColumn(
        "Hari",
        F.when(F.dayofweek("devicetime") == 1, "Minggu")
         .when(F.dayofweek("devicetime") == 2, "Senin")
         .when(F.dayofweek("devicetime") == 3, "Selasa")
         .when(F.dayofweek("devicetime") == 4, "Rabu")
         .when(F.dayofweek("devicetime") == 5, "Kamis")
         .when(F.dayofweek("devicetime") == 6, "Jumat")
         .when(F.dayofweek("devicetime") == 7, "Sabtu")
         .otherwise("Unknown")
    )
    
    # Derive week_of_year in format "YYYY-Www" (e.g., "2025-W08")
    df = df.withColumn(
        "week_of_year",
        F.concat(
            F.year("devicetime").cast(StringType()),
            F.lit("-W"),
            F.lpad(F.weekofyear("devicetime").cast(StringType()), 2, "0")
        )
    )
    
    return df


# ============================================================================
# GOLD LAYER - Aggregated Views
# ============================================================================

# 1. Famous Jarak Aman KM
@dp.table(name="gold_v_famous_jarak_aman_km")
def gold_v_famous_jarak_aman_km():
    return (
        spark.readStream.table("silver_famous_jarakaman")
        .filter(F.col("KM").isNotNull() & (F.col("KM") != ""))
        .groupBy("KM", "Hari", "week_of_year")
        .agg(F.avg("jarak_waktu").alias("rerata_jarak_waktu"))
    )


# 2. Famous Jarak Aman GPS (Time Interval)
@dp.table(name="gold_v_famous_jarak_aman_gps")
def gold_v_famous_jarak_aman_gps():
    return (
        spark.readStream.table("silver_famous_jarakaman")
        .filter(F.col("Interval").isNotNull())
        .groupBy("Interval", "Hari", "week_of_year")
        .agg(
            F.avg("jarak_waktu").alias("rerata_jarak_waktu"),
            F.avg("jarak").alias("rerata_jarak")
        )
    )


# 3. Famous Jarak Aman Grup Truk
@dp.table(name="gold_v_famous_jarak_aman_grup_truk")
def gold_v_famous_jarak_aman_grup_truk():
    return (
        spark.readStream.table("silver_famous_jarakaman")
        .filter(F.col("grup_truk").isNotNull())
        .withColumnRenamed("grup_truk", "grup_truk_name")
        .groupBy("grup_truk_name", "Hari", "week_of_year")
        .agg(F.avg("jarak_waktu").alias("rerata_jarak_waktu"))
    )
