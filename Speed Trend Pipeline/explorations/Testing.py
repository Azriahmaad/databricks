# Databricks notebook source
# NEW NOTEBOOK: Test_Error_Handling_Bronze.py
# Copy functions dari pipeline file untuk test standalone

from pyspark.sql import functions as F
import time

def jdbc_read_with_retry(jdbc_url, query, props, max_retries=3, retry_interval_seconds=10):
    """Same function dari pipeline, tapi pakai shorter interval untuk test"""
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[JDBC Read] Attempt {attempt}/{max_retries} - Connecting to PostgreSQL...")
            df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", query).options(**props).load()
            
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

Pipeline has been halted.
===================================================================================
                """
                print(error_detail)
                raise Exception(error_detail) from last_error
    
    raise Exception(f"Unexpected error: retry loop completed without success or raising exception") from last_error


# ============================================
# TEST 1: Success Case (Normal Operation)
# ============================================
print("="*80)
print("TEST 1: Normal Connection (Should succeed on attempt 1)")
print("="*80)

jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/wim"
props = {
    "user": "postgres.duiatmmlmgnqvabxyjpt",
    "password": "gRMltRBwm4b074yP",  # ✅ CORRECT
    "driver": "org.postgresql.Driver",
    "connectTimeout": "30",
    "socketTimeout": "30"
}
query = "(SELECT * FROM public.closing_transaction LIMIT 5) AS test"

try:
    df = jdbc_read_with_retry(jdbc_url, query, props, max_retries=3, retry_interval_seconds=10)
    print("\n✅ TEST 1 PASSED - Connection successful without retries")
    print(f"Sample data fetched: {df.count()} rows")
    df.show(3)
except Exception as e:
    print("\n❌ TEST 1 FAILED - Should not fail with correct credentials")
    print(str(e)[:300])

print("\n")

# ============================================
# TEST 2: Wrong Password (Failure Case)
# ============================================
print("="*80)
print("TEST 2: Wrong Password (Should retry 3x then fail)")
print("="*80)

props_wrong = {
    "user": "postgres.duiatmmlmgnqvabxyjpt",
    "password": "WRONG_PASSWORD_12345",  # ❌ WRONG
    "driver": "org.postgresql.Driver",
    "connectTimeout": "10",
    "socketTimeout": "10"
}

try:
    df = jdbc_read_with_retry(jdbc_url, query, props_wrong, max_retries=3, retry_interval_seconds=10)
    print("\n❌ TEST 2 FAILED - Should not succeed with wrong password")
except Exception as e:
    print("\n✅ TEST 2 PASSED - Retry mechanism caught error correctly")
    print("\nError message preview (first 500 chars):")
    print(str(e)[:500])

print("\n")

# ============================================
# TEST 3: Wrong Host (Connection Timeout)
# ============================================
print("="*80)
print("TEST 3: Wrong Host (Should timeout after retries)")
print("="*80)

jdbc_url_wrong = "jdbc:postgresql://wrong-host-12345.supabase.com:5432/wim"
props_short_timeout = {
    "user": "postgres.duiatmmlmgnqvabxyjpt",
    "password": "gRMltRBwm4b074yP",
    "driver": "org.postgresql.Driver",
    "connectTimeout": "5",
    "socketTimeout": "5"
}

try:
    df = jdbc_read_with_retry(jdbc_url_wrong, query, props_short_timeout, max_retries=3, retry_interval_seconds=5)
    print("\n❌ TEST 3 FAILED - Should not succeed with wrong host")
except Exception as e:
    print("\n✅ TEST 3 PASSED - Connection timeout handled correctly")
    error_type = type(e.__cause__).__name__ if e.__cause__ else type(e).__name__
    print(f"\nError type: {error_type}")


# ============================================
# SUMMARY
# ============================================
print("="*80)
print("TEST SUMMARY")
print("="*80)
print("TEST 1 (Normal): ✅ Should succeed immediately")
print("TEST 2 (Wrong Password): ✅ Should retry 3x with 10s interval (~20s total)")
print("TEST 3 (Wrong Host): ✅ Should retry 3x with 5s interval (~10s total)")
print("\nTotal test time: ~30-40 seconds")
print("="*80)
