# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ============================================================================
# BRONZE LAYER (Source - Already Exists)
# ============================================================================
@dlt.table(
    name="bronze_famous_jarakaman",
    comment="Bronze layer - raw data from source table"
)
def bronze_famous_jarakaman():
    """Read bronze table as streaming source"""
    return spark.readStream.table("tc.default.famous_jarakaman_202604290427")


# ============================================================================
# SILVER LAYER - Add Derived Columns
# ============================================================================
@dlt.table(
    name="silver_famous_jarakaman",
    comment="Silver layer - enriched with derived time dimensions and KM extraction",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_devicetime", "devicetime IS NOT NULL")
def silver_famous_jarakaman():
    """
    Silver transformation:
    - Extract KM from geofence_name
    - Derive Interval (time range) from devicetime
    - Derive Hari (day name in Indonesian) from devicetime
    - Derive week_of_year in format YYYY-Www
    """
    df = dlt.read_stream("bronze_famous_jarakaman")
    
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
    # 1=Minggu, 2=Senin, 3=Selasa, 4=Rabu, 5=Kamis, 6=Jumat, 7=Sabtu
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
@dlt.table(
    name="gold_v_famous_jarak_aman_km",
    comment="Gold aggregation: Average jarak_waktu by KM, Hari, and week_of_year",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_v_famous_jarak_aman_km():
    """
    Aggregate average jarak_waktu by KM location
    Used for: Top 5 best/worst KM charts
    """
    return (
        dlt.read_stream("silver_famous_jarakaman")
        .filter(F.col("KM").isNotNull() & (F.col("KM") != ""))
        .groupBy("KM", "Hari", "week_of_year")
        .agg(
            F.avg("jarak_waktu").alias("rerata_jarak_waktu")
        )
    )


# 2. Famous Jarak Aman GPS (Time Interval)
@dlt.table(
    name="gold_v_famous_jarak_aman_gps",
    comment="Gold aggregation: Average jarak and jarak_waktu by time Interval, Hari, and week_of_year",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_v_famous_jarak_aman_gps():
    """
    Aggregate average jarak and jarak_waktu by time interval
    Used for: Top 5 best/worst time intervals + heatmaps
    """
    return (
        dlt.read_stream("silver_famous_jarakaman")
        .filter(F.col("Interval").isNotNull())
        .groupBy("Interval", "Hari", "week_of_year")
        .agg(
            F.avg("jarak_waktu").alias("rerata_jarak_waktu"),
            F.avg("jarak").alias("rerata_jarak")
        )
    )


# 3. Famous Jarak Aman Grup Truk
@dlt.table(
    name="gold_v_famous_jarak_aman_grup_truk",
    comment="Gold aggregation: Average jarak_waktu by truck group, Hari, and week_of_year",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_v_famous_jarak_aman_grup_truk():
    """
    Aggregate average jarak_waktu by truck group
    Used for: Truck group heatmap
    """
    return (
        dlt.read_stream("silver_famous_jarakaman")
        .filter(F.col("grup_truk").isNotNull())
        .withColumnRenamed("grup_truk", "Group Truk")
        .groupBy("Group Truk", "Hari", "week_of_year")
        .agg(
            F.avg("jarak_waktu").alias("rerata_jarak_waktu")
        )
    )
