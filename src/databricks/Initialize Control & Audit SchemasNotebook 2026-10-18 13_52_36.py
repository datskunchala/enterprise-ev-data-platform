# Databricks notebook source
# Databricks: Initialize Control & Audit Schemas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

STORAGE_ACCOUNT = "stenterev001"
STORAGE_KEY = dbutils.secrets.get(scope="enterprise-ev-secrets", key="storage-account-key")

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)
CONTROL_BASE = f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# 1. Pipeline Stage Audit Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS delta.`{CONTROL_BASE}/audit/pipeline_stage_audit` (
    pipeline_run_id STRING,
    stage_name STRING,
    layer STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    source_records_read BIGINT,
    target_records_written BIGINT,
    quarantined_records BIGINT,
    error_message STRING,
    logged_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (layer)
""")

# 2. Stage Data Snapshot Registry (Lineage)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS delta.`{CONTROL_BASE}/lineage/stage_snapshots` (
    pipeline_run_id STRING,
    stage_name STRING,
    input_path STRING,
    output_path STRING,
    delta_version_snapshot BIGINT,
    created_at TIMESTAMP
)
USING DELTA
""")

# 3. Cost & Billing Monitor Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS delta.`{CONTROL_BASE}/cost_monitoring/pipeline_billing_summary` (
    pipeline_run_id STRING,
    run_date DATE,
    adf_duration_seconds DOUBLE,
    adf_estimated_cost_usd DOUBLE,
    databricks_dbu_consumed DOUBLE,
    databricks_estimated_cost_usd DOUBLE,
    powerbi_refresh_duration_seconds DOUBLE,
    powerbi_estimated_cost_usd DOUBLE,
    total_estimated_run_cost_usd DOUBLE,
    logged_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_date)
""")

print("Control & Audit tables initialized successfully.")