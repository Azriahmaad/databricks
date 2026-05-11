from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Layer: Daily speed trend for dashboard visualization

@dp.materialized_view(
    name="workspace.gold.daily_speed_trend",
    comment="Gold: Daily speed trend aggregation for dashboard - filters ACTUAL transactions only",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "quality": "gold"
    }
)
def daily_speed_trend():
    """
    Daily speed trend aggregation for dashboard visualization.
    Aggregates by date and speed category, filtering only ACTUAL transactions.
    """
    df = spark.read.table("workspace.silver.closing_transaction_enriched")
    
    result = (
        df
        # Filter for actual WIM transactions with valid speed data
        .filter(
            (F.col("status_trx_wim") == "ACTUAL") &
            F.col("average_speed").isNotNull()
        )
        # Aggregate by date and speed category
        .groupBy(
            F.date_format(F.col("timestamp_in"), "yyyy-MM-dd").alias("tanggal"),
            F.col("speed_category").alias("kecepatan")
        )
        .agg(
            F.count("truck").alias("jumlah_transaksi")
        )
        .withColumn("gold_processed_at", F.current_timestamp())
        .orderBy("tanggal", "kecepatan")
    )
    
    return result
