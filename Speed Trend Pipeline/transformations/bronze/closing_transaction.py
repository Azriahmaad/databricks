from pyspark import pipelines as dp
from pyspark.sql import functions as F
import time

def get_jdbc_config():
    """Get JDBC connection configuration with resilience settings"""
    jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/wim"
    props = {
        "user": "postgres.duiatmmlmgnqvabxyjpt",
        "password": "gRMltRBwm4b074yP",
        "driver": "org.postgresql.Driver",
        "fetchsize": "1000",
        "connectTimeout": "30",
        "socketTimeout": "30",
        "loginTimeout": "30",
        "ssl": "true",
        "sslmode": "require"
    }
    return jdbc_url, props

def add_metadata(df):
    """Add metadata columns"""
    return df.select(
        "*",
        F.current_timestamp().alias("_ingested_at"),
        F.lit("supabase.public.closing_transaction").alias("_source"),
        F.concat(F.lit("batch_"), F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"), F.lit("_"), F.expr("uuid()")).alias("_batch_id")
    )

def jdbc_read_with_retry(jdbc_url, query, props, max_retries=3, retry_interval_seconds=30):
    """
    Read from JDBC with retry mechanism for resilience.
    
    Args:
        jdbc_url: JDBC connection URL
        query: SQL query to execute
        props: JDBC connection properties
        max_retries: Maximum number of retry attempts (default: 3)
        retry_interval_seconds: Wait time between retries in seconds (default: 30)
    
    Returns:
        DataFrame if successful
        
    Raises:
        Exception: If all retries exhausted with detailed error message
    """
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[JDBC Read] Attempt {attempt}/{max_retries} - Connecting to PostgreSQL...")
            df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", query).options(**props).load()
            
            # Trigger action to validate connection and data fetch
            row_count = df.count()
            print(f"[JDBC Read] SUCCESS - Fetched {row_count} rows on attempt {attempt}")
            return df
            
        except Exception as e:
            last_error = e
            error_type = type(e).__name__
            error_msg = str(e)
            
            print(f"[JDBC Read] FAILED - Attempt {attempt}/{max_retries}")
            print(f"[JDBC Read] Error Type: {error_type}")
            print(f"[JDBC Read] Error Message: {error_msg}")
            
            if attempt < max_retries:
                print(f"[JDBC Read] Retrying in {retry_interval_seconds} seconds...")
                time.sleep(retry_interval_seconds)
            else:
                # All retries exhausted - construct detailed error message
                error_detail = f"""
==================== PHASE 3 RESILIENCE: ALL RETRIES EXHAUSTED ====================
Database Connection Failed After {max_retries} Attempts

Last Error Type: {error_type}
Last Error Message: {error_msg}

Connection Details:
- Host: aws-1-ap-southeast-1.pooler.supabase.com:5432
- Database: wim
- User: postgres.duiatmmlmgnqvabxyjpt

Retry Configuration:
- Max Retries: {max_retries}
- Retry Interval: {retry_interval_seconds} seconds
- Total Wait Time: {(max_retries - 1) * retry_interval_seconds} seconds

Action Required:
1. Check database connectivity and credentials
2. Verify Supabase pooler is running
3. Check network/firewall rules
4. Review database logs for connection issues

Pipeline has been halted. Fix the connection issue and trigger a new update.
===================================================================================
                """
                print(error_detail)
                raise Exception(error_detail) from last_error
    
    # Should never reach here, but just in case
    raise Exception(f"Unexpected error: retry loop completed without success or raising exception") from last_error

# Step 1: Create target streaming table
dp.create_streaming_table(
    name="bronze.closing_transaction",
    comment="Bronze layer: Incremental ingestion from Supabase PostgreSQL with bidirectional ingestion and Phase 3 resilience (retry mechanism).",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"}
)

# Step 2: Unified ingestion flow with retry mechanism - fetches from both directions in one run
@dp.append_flow(target="bronze.closing_transaction", name="bidirectional_ingest", once=True, comment="Initial: fetch all data. Incremental: fetch forward + backfill (max 10k per run). Phase 3: 3x retry with 30s interval.")
def bidirectional_ingest():
    """
    Unified ingestion with Phase 3 resilience:
    - Initial load: Fetch ALL data from source (no limit)
    - Subsequent runs: Forward (created_date > max) + Backfill (created_date < min), limit 10k total
    - Retry mechanism: 3x retry with 30s interval for connection failures
    - Alert: Pipeline halts if all retries exhausted
    """
    jdbc_url, props = get_jdbc_config()
    
    # Get current watermarks
    try:
        wm_df = spark.read.table("bronze.closing_transaction").select(
            F.max("created_date").alias("max_wm"),
            F.min("created_date").alias("min_wm")
        ).collect()[0]
        max_wm = wm_df["max_wm"] or "1900-01-01 00:00:00"
        min_wm = wm_df["min_wm"] or "9999-12-31 23:59:59"
    except:
        # First run: Initial load
        max_wm = "1900-01-01 00:00:00"
        min_wm = "9999-12-31 23:59:59"
    
    # Strategy: Initial load fetches all, incremental fetches up to 10k
    if max_wm == "1900-01-01 00:00:00":
        # Initial load: Fetch ALL data from source (no LIMIT)
        query = "(SELECT * FROM public.closing_transaction ORDER BY created_date DESC) AS initial"
        print("[Bidirectional Ingest] Mode: INITIAL LOAD (fetching all data)")
    else:
        # Incremental: Forward + Backfill with LIMIT 10k total
        query = f"""
            (SELECT * FROM (
                SELECT *, 1 as priority FROM public.closing_transaction 
                WHERE created_date > '{max_wm}'
                ORDER BY created_date ASC
                LIMIT 10000
            ) forward
            UNION ALL
            SELECT * FROM (
                SELECT *, 2 as priority FROM public.closing_transaction 
                WHERE created_date < '{min_wm}'
                ORDER BY created_date DESC
                LIMIT 10000
            ) backfill
            ORDER BY priority, created_date
            LIMIT 10000) AS bidirectional
        """
        print(f"[Bidirectional Ingest] Mode: INCREMENTAL (forward from {max_wm}, backfill before {min_wm})")
    
    # Execute JDBC read with retry mechanism (Phase 3 resilience)
    df = jdbc_read_with_retry(jdbc_url, query, props, max_retries=3, retry_interval_seconds=30)
    
    # Drop priority column if exists
    if "priority" in df.columns:
        df = df.drop("priority")
    
    print(f"[Bidirectional Ingest] Ingestion successful - returning DataFrame with metadata")
    return add_metadata(df)
