from pyspark import pipelines as dp
from pyspark.sql import functions as F
import time

def get_jdbc_config():
    jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/wim"
    props = {
        "user":           dbutils.secrets.get(scope="jdbc_supabase", key="username"),
        "password":       dbutils.secrets.get(scope="jdbc_supabase", key="password"),
        "driver":         "org.postgresql.Driver",
        "fetchsize":      "1000",
        "connectTimeout": "30",
        "socketTimeout":  "30",
        "ssl":            "true",
        "sslmode":        "require"
    }
    return jdbc_url, props

def add_metadata(df):
    return df.select(
        "*",
        F.current_timestamp().alias("_ingested_at"),
        F.lit("supabase.public.closing_transaction").alias("_source"),
        F.concat(
            F.lit("batch_"),
            F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
            F.lit("_"),
            F.expr("uuid()")
        ).alias("_batch_id")
    )

def jdbc_read_with_retry(jdbc_url, query, props, max_retries=3, retry_interval_seconds=30):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[JDBC] Attempt {attempt}/{max_retries}...")
            df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", query) \
                .options(**props) \
                .load()
            row_count = df.count()
            print(f"[JDBC] SUCCESS - {row_count} rows on attempt {attempt}")
            return df
        except Exception as e:
            last_error = e
            print(f"[JDBC] FAILED attempt {attempt}: {e}")
            if attempt < max_retries:
                print(f"[JDBC] Retry dalam {retry_interval_seconds}s...")
                time.sleep(retry_interval_seconds)
            else:
                raise Exception(f"Semua {max_retries} retry gagal. Pipeline dihentikan.") from last_error

# ── Buat target table ──────────────────────────────────────────
dp.create_streaming_table(
    name="bronze.closing_transaction",
    comment="Bronze layer dari Supabase PostgreSQL",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)

# ── FLOW 1: Initial Load (once=True) ──────────────────────────
@dp.append_flow(
    target="bronze.closing_transaction",
    name="initial_load",
    once=True,  # hanya jalan sekali seumur hidup pipeline
    comment="Initial load: ambil SEMUA data dari source"
)
def initial_load():
    jdbc_url, props = get_jdbc_config()
    query = "(SELECT * FROM public.closing_transaction ORDER BY created_date ASC) AS initial"
    print("[INITIAL] Mengambil semua data dari source...")
    df = jdbc_read_with_retry(jdbc_url, query, props)
    return add_metadata(df)

# ── FLOW 2: Incremental Load (tanpa once=True) ────────────────
@dp.append_flow(
    target="bronze.closing_transaction",
    name="incremental_load",
    # TIDAK pakai once=True agar jalan setiap pipeline trigger
    comment="Incremental load: ambil data baru berdasarkan created_date"
)
def incremental_load():
    jdbc_url, props = get_jdbc_config()

    # Ambil watermark terakhir
    try:
        max_wm = spark.read.table("bronze.closing_transaction") \
            .select(F.max("created_date").alias("max_wm")) \
            .collect()[0]["max_wm"]
        if max_wm is None:
            max_wm = "1900-01-01 00:00:00"
    except Exception:
        max_wm = "1900-01-01 00:00:00"

    print(f"[INCREMENTAL] Watermark: {max_wm}")

    query = f"""
        (SELECT * FROM public.closing_transaction
         WHERE created_date > '{max_wm}'
         ORDER BY created_date ASC) AS incremental
    """

    df = jdbc_read_with_retry(jdbc_url, query, props)

    if df.count() == 0:
        print("[INCREMENTAL] Tidak ada data baru")
        return df

    print(f"[INCREMENTAL] Data baru ditemukan, melanjutkan ingest...")
    return add_metadata(df)