# Databricks notebook source
# Databricks Notebook: 03_all_domains_gold
import datetime
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("pipeline_run_id", "local_debug_run")
RUN_ID = dbutils.widgets.get("pipeline_run_id")

STORAGE_ACCOUNT = "stenterev001"
STORAGE_KEY = dbutils.secrets.get(scope="enterprise-ev-secrets", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY.strip())

SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/warehouse"
CONTROL = f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net"

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
    spark.createDataFrame(audit_row, cols).write.format("delta").mode("append").save(f"{CONTROL}/audit/pipeline_stage_audit")

start_time = datetime.datetime.utcnow()

try:
    # 1. Date Dimension
    start_d = datetime.datetime.strptime("2024-10-17", "%Y-%m-%d")
    date_list = [(start_d + timedelta(days=d)).date() for d in range(731)]
    df_dim_date = (
        spark.createDataFrame([(d,) for d in date_list], ["full_date"])
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("month_num", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
    )
    df_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/dim_date/")
    print("dim_date written successfully.")

    # 2. Vehicle Dimension
    df_eng = spark.read.format("delta").load(f"{SILVER}/engineering/telemetry_cleaned/")
    dim_vehicle = (
        df_eng.select("vin").distinct()
        .withColumn("vehicle_sk", F.monotonically_increasing_id() + 1000)
        .withColumn("model_category", F.lit("Commercial EV"))
        .withColumn("is_active", F.lit(1))
    )
    dim_vehicle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/dim_vehicle/")
    print("dim_vehicle written successfully.")

    # 3. Fact: Telemetry
    df_fact_telemetry = (
        df_eng.join(dim_vehicle, on="vin", how="inner")
        .withColumn("date_key", F.date_format("event_timestamp", "yyyyMMdd").cast(IntegerType()))
        .select(
            F.col("reading_id").alias("telemetry_key"),
            "date_key", "vehicle_sk", "vin", "event_timestamp",
            "battery_soc_percent", "motor_rpm", "battery_temp_c"
        )
    )
    df_fact_telemetry.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date_key").save(f"{GOLD}/fact_vehicle_telemetry/")
    print("fact_vehicle_telemetry written successfully.")

    # 4. Daily Aggregated Telemetry KPI
    df_daily_kpi = (
        df_eng.withColumn("report_date", F.to_date("event_timestamp"))
        .groupBy("report_date", "vin")
        .agg(
            F.round(F.avg("motor_rpm"), 2).alias("avg_motor_rpm"),
            F.round(F.max("motor_rpm"), 2).alias("peak_motor_rpm"),
            F.round(F.avg("battery_temp_c"), 2).alias("avg_battery_temp_c"),
            F.round(F.min("battery_soc_percent"), 2).alias("min_battery_soc"),
            F.round(F.max("battery_soc_percent"), 2).alias("max_battery_soc"),
            F.count("reading_id").alias("total_records")
        )
    )
    df_daily_kpi.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/agg_daily_vehicle_performance/")
    print("agg_daily_vehicle_performance written successfully.")

except Exception as core_err:
    log_audit(RUN_ID, "core_telemetry", "Gold_Core_Warehouse", "GOLD", "FAILED", start_time, datetime.datetime.utcnow(), err=str(core_err))
    raise core_err

# 5. Optional Domain Facts
# Sales Fact & Showroom Dimension
try:
    df_showrooms = spark.read.format("delta").load(f"{SILVER}/showrooms/showrooms_cleaned/")
    dim_showroom = (
        df_showrooms.select("showroom_id", "showroom_city").distinct()
        .withColumn("showroom_sk", F.monotonically_increasing_id() + 500)
    )
    dim_showroom.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/dim_showroom/")

    df_sales = spark.read.format("delta").load(f"{SILVER}/sales/sales_cleaned/")
    df_fact_sales = (
        df_sales.join(dim_vehicle, on="vin", how="inner")
        .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast(IntegerType()))
        .select("sale_id", "date_key", "vehicle_sk", "vin", "customer_id", "sale_amount_usd")
    )
    df_fact_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date_key").save(f"{GOLD}/fact_vehicle_sales/")
    print("fact_vehicle_sales written successfully.")
except Exception as e:
    print(f"[SKIPPED] Sales: {e}")

# Service Fact
try:
    df_svc = spark.read.format("delta").load(f"{SILVER}/service/service_cleaned/")
    df_fact_svc = (
        df_svc.join(dim_vehicle, on="vin", how="inner")
        .withColumn("date_key", F.date_format("service_date", "yyyyMMdd").cast(IntegerType()))
        .select("service_id", "date_key", "vehicle_sk", "vin", "labor_cost_usd", "issue_reported")
    )
    df_fact_svc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/fact_vehicle_service/")
    print("fact_vehicle_service written successfully.")
except Exception as e:
    print(f"[SKIPPED] Service: {e}")

# Warranty Fact
try:
    df_war = spark.read.format("delta").load(f"{SILVER}/warranty/warranty_cleaned/")
    df_fact_war = (
        df_war.join(dim_vehicle, on="vin", how="inner")
        .select("claim_id", "vehicle_sk", "vin", "claim_cost_usd", "claim_status")
    )
    df_fact_war.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD}/fact_warranty/")
    print("fact_warranty written successfully.")
except Exception as e:
    print(f"[SKIPPED] Warranty: {e}")

# Delta Compaction
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.sql(f"OPTIMIZE delta.`{GOLD}/fact_vehicle_telemetry/` ZORDER BY (vin)")
spark.sql(f"OPTIMIZE delta.`{GOLD}/agg_daily_vehicle_performance/` ZORDER BY (report_date)")

end_time = datetime.datetime.utcnow()
log_audit(
    RUN_ID, "enterprise_warehouse", "Silver_to_Gold_StarSchema", "GOLD",
    "SUCCEEDED", start_time, end_time, 0, df_fact_telemetry.count(), 0, None
)

dbutils.notebook.exit(f"Gold Execution Succeeded. Modeled {df_fact_telemetry.count()} telemetry records.")

# COMMAND ----------

