# Databricks notebook source
# Databricks Notebook: 02_all_domains_silver
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("pipeline_run_id", "local_debug_run")
RUN_ID = dbutils.widgets.get("pipeline_run_id")

STORAGE_ACCOUNT = "stenterev001"
STORAGE_KEY = dbutils.secrets.get(scope="enterprise-ev-secrets", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY.strip())

BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CONTROL_BASE = f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net"

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

def write_silver(df_clean, df_quar, clean_path, quar_path, partition_col=None):
    writer = df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(clean_path)
    
    q_count = df_quar.count()
    if q_count > 0:
        df_quar.write.format("delta").mode("append").option("mergeSchema", "true").save(quar_path)
    return df_clean.count(), q_count

# 1. Engineering
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/engineering/bronze_telemetry/")
    rc = df.count()
    df_t = (
        df.withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withColumn("battery_temp_c", F.col("battery_temp_c").cast(DoubleType()))
        .withColumn("motor_rpm", F.col("motor_rpm").cast(DoubleType()))
        .withColumn("battery_soc_percent", F.col("soc").cast(DoubleType()))
        .withColumn("reading_id", F.sha2(F.concat_ws("||", "vin", "timestamp"), 256))
        .dropDuplicates(["reading_id"])
    )
    v_cond = (F.col("vin").isNotNull()) & (F.col("event_timestamp").isNotNull()) & (F.col("battery_soc_percent").between(0.0, 100.0)) & (F.col("motor_rpm") >= 0.0)
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Range/Null Anomaly")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/engineering/telemetry_cleaned/", f"{SILVER_BASE}/engineering/telemetry_quarantine/", "vin")
    log_audit(RUN_ID, "engineering", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[ENGINEERING] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "engineering", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 2. Manufacturing
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/manufacturing/bronze_manufacturing/")
    rc = df.count()
    df_t = (
        df.withColumn("production_order_id", F.col("production_order_id").cast(StringType()))
        .withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("plant_location", F.col("plant_location").cast(StringType()))
        .withColumn("defect_flag", F.col("defect_flag").cast(BooleanType()))
        .withColumn("assembly_date", F.to_date("assembly_date"))
        .dropDuplicates(["production_order_id"])
    )
    v_cond = (F.col("production_order_id").isNotNull()) & (F.col("vin").isNotNull())
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Null Order/VIN")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/manufacturing/manufacturing_cleaned/", f"{SILVER_BASE}/manufacturing/manufacturing_quarantine/")
    log_audit(RUN_ID, "manufacturing", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[MANUFACTURING] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "manufacturing", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 3. Sales
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/sales/bronze_sales/")
    rc = df.count()
    df_t = (
        df.withColumn("sale_id", F.col("sale_id").cast(StringType()))
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        .withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("sale_date", F.to_date("sale_date"))
        .withColumn("sale_amount_usd", F.col("sale_amount_usd").cast(DoubleType()))
        .dropDuplicates(["sale_id"])
    )
    v_cond = (F.col("sale_id").isNotNull()) & (F.col("sale_amount_usd") > 0.0)
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Invalid Amount/ID")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/sales/sales_cleaned/", f"{SILVER_BASE}/sales/sales_quarantine/")
    log_audit(RUN_ID, "sales", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[SALES] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "sales", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 4. Showrooms
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/showrooms/bronze_showrooms/")
    rc = df.count()
    df_t = (
        df.withColumn("showroom_id", F.col("showroom_id").cast(StringType()))
        .withColumn("showroom_city", F.col("showroom_city").cast(StringType()))
        .withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("stock_status", F.col("stock_status").cast(StringType()))
        .dropDuplicates(["showroom_id", "vin"])
    )
    v_cond = F.col("showroom_id").isNotNull()
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Null Showroom ID")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/showrooms/showrooms_cleaned/", f"{SILVER_BASE}/showrooms/showrooms_quarantine/")
    log_audit(RUN_ID, "showrooms", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[SHOWROOMS] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "showrooms", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 5. Service
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/service/bronze_service/")
    rc = df.count()
    df_t = (
        df.withColumn("service_id", F.col("service_id").cast(StringType()))
        .withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("service_date", F.to_date("service_date"))
        .withColumn("labor_cost_usd", F.col("labor_cost_usd").cast(DoubleType()))
        .withColumn("issue_reported", F.col("issue_reported").cast(StringType()))
        .dropDuplicates(["service_id"])
    )
    v_cond = (F.col("service_id").isNotNull()) & (F.col("labor_cost_usd") >= 0.0)
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Invalid Labor Cost")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/service/service_cleaned/", f"{SILVER_BASE}/service/service_quarantine/")
    log_audit(RUN_ID, "service", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[SERVICE] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "service", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 6. Warranty
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/warranty/bronze_warranty/")
    rc = df.count()
    df_t = (
        df.withColumn("claim_id", F.col("claim_id").cast(StringType()))
        .withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("claim_cost_usd", F.col("claim_cost_usd").cast(DoubleType()))
        .withColumn("claim_status", F.col("claim_status").cast(StringType()))
        .dropDuplicates(["claim_id"])
    )
    v_cond = (F.col("claim_id").isNotNull()) & (F.col("claim_cost_usd") >= 0.0)
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Invalid Claim Cost")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/warranty/warranty_cleaned/", f"{SILVER_BASE}/warranty/warranty_quarantine/")
    log_audit(RUN_ID, "warranty", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[WARRANTY] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "warranty", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 7. Spares
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/spares/bronze_spares/")
    rc = df.count()
    df_t = (
        df.withColumn("part_id", F.col("part_id").cast(StringType()))
        .withColumn("part_name", F.col("part_name").cast(StringType()))
        .withColumn("quantity_in_stock", F.col("quantity_in_stock").cast(IntegerType()))
        .withColumn("warehouse_location", F.col("warehouse_location").cast(StringType()))
        .dropDuplicates(["part_id"])
    )
    v_cond = (F.col("part_id").isNotNull()) & (F.col("quantity_in_stock") >= 0)
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Invalid Quantity")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/spares/spares_cleaned/", f"{SILVER_BASE}/spares/spares_quarantine/")
    log_audit(RUN_ID, "spares", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[SPARES] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "spares", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 8. Finance
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/finance/bronze_finance/")
    rc = df.count()
    df_t = (
        df.withColumn("invoice_id", F.col("invoice_id").cast(StringType()))
        .withColumn("customer_id", F.col("customer_id").cast(StringType()))
        .withColumn("invoice_amount_usd", F.col("invoice_amount_usd").cast(DoubleType()))
        .withColumn("payment_status", F.col("payment_status").cast(StringType()))
        .dropDuplicates(["invoice_id"])
    )
    v_cond = (F.col("invoice_id").isNotNull()) & (F.col("invoice_amount_usd").isNotNull())
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Null Invoice/Amount")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/finance/finance_cleaned/", f"{SILVER_BASE}/finance/finance_quarantine/")
    log_audit(RUN_ID, "finance", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[FINANCE] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "finance", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 9. HR
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/hr/bronze_hr/")
    rc = df.count()
    df_t = (
        df.withColumn("employee_id", F.col("employee_id").cast(StringType()))
        .withColumn("department", F.col("department").cast(StringType()))
        .withColumn("employee_name", F.col("employee_name").cast(StringType()))
        .withColumn("salary_usd", F.col("salary_usd").cast(DoubleType()))
        .dropDuplicates(["employee_id"])
    )
    v_cond = F.col("employee_id").isNotNull()
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Null Employee ID")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/hr/hr_cleaned/", f"{SILVER_BASE}/hr/hr_quarantine/")
    log_audit(RUN_ID, "hr", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[HR] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "hr", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

# 10. IT Operations
st = datetime.datetime.utcnow()
try:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/itops/bronze_itops/")
    rc = df.count()
    df_t = (
        df.withColumn("run_id", F.col("run_id").cast(StringType()))
        .withColumn("pipeline_name", F.col("pipeline_name").cast(StringType()))
        .withColumn("status", F.col("status").cast(StringType()))
        .withColumn("execution_time_sec", F.col("execution_time_sec").cast(DoubleType()))
        .dropDuplicates(["run_id"])
    )
    v_cond = F.col("run_id").isNotNull()
    df_c = df_t.filter(v_cond).withColumn("_silver_processed_at", F.current_timestamp())
    df_q = df_t.filter(~v_cond).withColumn("_quarantine_reason", F.lit("Null Run ID")).withColumn("_quarantined_at", F.current_timestamp())
    wc, qc = write_silver(df_c, df_q, f"{SILVER_BASE}/itops/itops_cleaned/", f"{SILVER_BASE}/itops/itops_quarantine/")
    log_audit(RUN_ID, "itops", "Bronze_to_Silver", "SILVER", "SUCCEEDED", st, datetime.datetime.utcnow(), rc, wc, qc)
    print(f"[ITOPS] Clean: {wc} | Quarantined: {qc}")
except Exception as e:
    log_audit(RUN_ID, "itops", "Bronze_to_Silver", "SILVER", "FAILED", st, datetime.datetime.utcnow(), err=str(e))

dbutils.notebook.exit("Silver Processing Completed.")