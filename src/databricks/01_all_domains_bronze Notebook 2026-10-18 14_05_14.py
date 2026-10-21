# Databricks notebook source
# Databricks Notebook: 01_all_domains_bronze
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("pipeline_run_id", "local_debug_run")
RUN_ID = dbutils.widgets.get("pipeline_run_id")

STORAGE_ACCOUNT = "stenterev001"
STORAGE_KEY = dbutils.secrets.get(scope="enterprise-ev-secrets", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY.strip())

BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CONTROL_BASE = f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net"

DOMAINS = [
    {"domain": "engineering",   "source": f"{BRONZE_BASE}/engineering/engineering_telemetry.csv",         "target": f"{BRONZE_BASE}/engineering/bronze_telemetry/"},
    {"domain": "manufacturing", "source": f"{BRONZE_BASE}/manufacturing/manufacturing_production.csv",   "target": f"{BRONZE_BASE}/manufacturing/bronze_manufacturing/"},
    {"domain": "sales",         "source": f"{BRONZE_BASE}/sales/sales_orders.csv",                         "target": f"{BRONZE_BASE}/sales/bronze_sales/"},
    {"domain": "showrooms",     "source": f"{BRONZE_BASE}/showrooms/showroom_inventory.csv",              "target": f"{BRONZE_BASE}/showrooms/bronze_showrooms/"},
    {"domain": "service",       "source": f"{BRONZE_BASE}/service/service_orders.csv",                     "target": f"{BRONZE_BASE}/service/bronze_service/"},
    {"domain": "warranty",      "source": f"{BRONZE_BASE}/warranty/warranty_claims.csv",                   "target": f"{BRONZE_BASE}/warranty/bronze_warranty/"},
    {"domain": "spares",        "source": f"{BRONZE_BASE}/spares/spares_inventory.csv",                   "target": f"{BRONZE_BASE}/spares/bronze_spares/"},
    {"domain": "finance",       "source": f"{BRONZE_BASE}/finance/finance_invoices.csv",                   "target": f"{BRONZE_BASE}/finance/bronze_finance/"},
    {"domain": "hr",            "source": f"{BRONZE_BASE}/HR/hr_workforce.csv",                           "target": f"{BRONZE_BASE}/hr/bronze_hr/"},
    {"domain": "itops",         "source": f"{BRONZE_BASE}/IT operations/it_pipeline_logs.csv",            "target": f"{BRONZE_BASE}/itops/bronze_itops/"}
]

def log_audit(run_id, domain, stage_name, layer, status, start_time, end_time, read_cnt=0, write_cnt=0, quar_cnt=0, err=None):
    dur = (end_time - start_time).total_seconds()
    audit_row = [(
        str(run_id), str(domain), str(stage_name), str(layer), str(status),
        start_time, end_time, float(dur), int(read_cnt), int(write_cnt), int(quar_cnt),
        str(err) if err else "NONE",
        datetime.datetime.utcnow()
    )]
    cols = [
        "pipeline_run_id", "domain", "stage_name", "layer", "status",
        "start_time", "end_time", "duration_seconds", "source_records_read",
        "target_records_written", "quarantined_records", "error_message", "logged_at"
    ]
    spark.createDataFrame(audit_row, cols).write.format("delta").mode("append").save(f"{CONTROL_BASE}/audit/pipeline_stage_audit")

for d in DOMAINS:
    st = datetime.datetime.utcnow()
    domain = d["domain"]
    try:
        df_raw = spark.read.option("header", "true").option("inferSchema", "false").csv(d["source"])
        read_count = df_raw.count()

        df_bronze = (
            df_raw
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("ingestion_date", F.current_date())
        )

        (
            df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("ingestion_date")
            .save(d["target"])
        )
        write_count = df_bronze.count()
        et = datetime.datetime.utcnow()

        log_audit(RUN_ID, domain, "Landing_to_Bronze", "BRONZE", "SUCCEEDED", st, et, read_count, write_count, 0)
        print(f"[{domain.upper()}] Ingestion Successful: {write_count} records")
    except Exception as e:
        et = datetime.datetime.utcnow()
        log_audit(RUN_ID, domain, "Landing_to_Bronze", "BRONZE", "FAILED", st, et, 0, 0, 0, str(e))
        print(f"[{domain.upper()}] Ingestion Failed: {str(e)}")

dbutils.notebook.exit("Bronze Processing Completed.")