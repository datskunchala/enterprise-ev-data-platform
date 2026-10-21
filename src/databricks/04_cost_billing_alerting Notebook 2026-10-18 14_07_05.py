# Databricks notebook source
# Databricks Notebook: 04_cost_billing_alerting
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("pipeline_run_id", "local_debug_run")
RUN_ID = dbutils.widgets.get("pipeline_run_id")

STORAGE_ACCOUNT = "stenterev001"
STORAGE_KEY = dbutils.secrets.get(scope="enterprise-ev-secrets", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

CONTROL_BASE = f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# 1. Benchmark Execution Durations from Audit Table
df_audit = (
    spark.read.format("delta").load(f"{CONTROL_BASE}/audit/pipeline_stage_audit")
    .filter(F.col("pipeline_run_id") == RUN_ID)
)

total_runtime_seconds = df_audit.select(F.sum("duration_seconds")).first()[0] or 0.0
total_hours = total_runtime_seconds / 3600.0

# 2. Azure South India Pricing Rates (Single Node D4ds_v5)
DBU_PER_HOUR = 0.75
COST_PER_DBU = 0.15
VM_HOURLY_RATE = 0.192
ADF_HOURLY_RATE = 0.25
POWER_BI_REFRESH_COST = 0.02

# 3. Calculate Financial Metrics
dbu_consumed = total_hours * DBU_PER_HOUR
databricks_cost = (dbu_consumed * COST_PER_DBU) + (total_hours * VM_HOURLY_RATE)
adf_cost = total_hours * ADF_HOURLY_RATE
pbi_cost = POWER_BI_REFRESH_COST
total_cost = databricks_cost + adf_cost + pbi_cost

# 4. Evaluation & Alerting Status
ALERT_THRESHOLD_USD = 0.50  # Alert flag if single pipeline run exceeds $0.50 USD
alert_status = "CRITICAL_COST_OVERRUN" if total_cost > ALERT_THRESHOLD_USD else "NORMAL"

# 5. Log to Billing Monitoring Table
billing_row = [(
    RUN_ID,
    datetime.date.today(),
    float(total_runtime_seconds),
    float(round(dbu_consumed, 4)),
    float(round(databricks_cost, 5)),
    float(round(adf_cost, 5)),
    float(round(pbi_cost, 5)),
    float(round(total_cost, 5)),
    alert_status,
    datetime.datetime.utcnow()
)]

cols = [
    "pipeline_run_id", "run_date", "total_runtime_seconds", "databricks_dbu_consumed",
    "databricks_cost_usd", "adf_cost_usd", "powerbi_cost_usd", "total_run_cost_usd",
    "alert_status", "logged_at"
]

df_billing = spark.createDataFrame(billing_row, cols)
df_billing.write.format("delta").mode("append").partitionBy("run_date").save(f"{CONTROL_BASE}/cost_monitoring/pipeline_billing_summary")

print("=" * 60)
print(f"PIPELINE RUN BILLING SUMMARY [{RUN_ID}]")
print(f"Total Execution Time  : {total_runtime_seconds:.1f} seconds")
print(f"Databricks Spend      : ${databricks_cost:.5f} USD ({dbu_consumed:.4f} DBUs)")
print(f"Azure Data Factory    : ${adf_cost:.5f} USD")
print(f"Power BI Refresh Cost : ${pbi_cost:.5f} USD")
print(f"Total Pipeline Cost   : ${total_cost:.5f} USD")
print(f"Cost Alert Status     : {alert_status}")
print("=" * 60)

# Check for stage errors across this run to signal ADF
failed_stages = df_audit.filter(F.col("status") == "FAILED").count()
if failed_stages > 0:
    print(f"CRITICAL ALERT: {failed_stages} stage(s) encountered failures during this pipeline execution.")
    dbutils.notebook.exit(f"ALERT: Pipeline Finished with {failed_stages} errors.")
else:
    dbutils.notebook.exit(f"SUCCESS: Total Cost ${total_cost:.4f} USD")