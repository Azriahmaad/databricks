from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver layer: Streaming table with speed enrichment for incremental processing
@dp.table(
    name="silver.closing_transaction_enriched",
    comment="Silver layer: Enriched closing transactions with speed categories and time dimensions",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "quality": "silver"
    }
)
@dp.expect_or_drop("valid_speed", "average_speed IS NULL OR average_speed >= 0")
@dp.expect_or_drop("valid_code", "code IS NOT NULL")
@dp.expect("valid_timestamps", "timestamp_in_local IS NOT NULL AND timestamp_out_local IS NOT NULL")
def closing_transaction_enriched():
    """
    Silver streaming table with speed enrichment for scalable incremental processing.
    Reads from bronze streaming table and enriches with:
    - Speed categories (based on average_speed)
    - Compliance flags
    - Time dimensions
    - Data quality checks
    """
    df = spark.readStream.table("bronze.closing_transaction")
    
    # Enrich with speed categories and compliance flags
    df_enriched = (
        df
        # Convert timestamp strings to proper TIMESTAMP type
        .withColumn("timestamp_in", F.to_timestamp("timestamp_in_local"))
        .withColumn("timestamp_out", F.to_timestamp("timestamp_out_local"))
        .withColumn("timestamp_gross", F.to_timestamp("timestamp_gross_local"))
        
        # Extract time dimensions
        .withColumn("date", F.to_date(F.to_timestamp("timestamp_in_local")))
        .withColumn("hour", F.hour(F.to_timestamp("timestamp_in_local")))
        .withColumn("day_of_week", F.dayofweek(F.to_timestamp("timestamp_in_local")))
        .withColumn("week_of_year", F.weekofyear(F.to_timestamp("timestamp_in_local")))
        .withColumn("month", F.month(F.to_timestamp("timestamp_in_local")))
        .withColumn("year", F.year(F.to_timestamp("timestamp_in_local")))
        
        # Add operation_date for dashboard compatibility
        .withColumn("operation_date", F.to_date(F.to_timestamp("timestamp_out_local")))
        
        # Speed category enrichment (based on average_speed from bronze)
        .withColumn(
            "speed_category",
            F.when(F.col("average_speed").isNull(), "Undefined")
            .when(F.col("average_speed") < 7, "Kurang dari 7 Kpj")
            .when((F.col("average_speed") >= 7) & (F.col("average_speed") <= 10), "Antara 7 Kpj sampai 10 Kpj")
            .when(F.col("average_speed") > 10, "Lebih dari 10 Kpj")
            .otherwise("Other")
        )
        
        # Compliance flag (1 if speed is in safe range 7-10 Kpj)
        .withColumn(
            "is_compliant",
            F.when((F.col("average_speed") >= 7) & (F.col("average_speed") <= 10), 1).otherwise(0)
        )
        
        # Standardize company code
        .withColumn("company_code_standard", F.upper(F.col("code")))
        
        # Add processing timestamp
        .withColumn("silver_processed_at", F.current_timestamp())
    )
    
    return df_enriched
