# Databricks notebook source
# Databricks Notebook: 02_bronze_to_silver
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

dbutils.widgets.text("pipeline_run_id", "local_debug_run")
RUN_ID = dbutils.widgets.get("pipeline_run_id")

STORAGE_ACCOUNT = "stenterev001"
STORAGE_KEY = dbutils.secrets.get(scope="enterprise-ev-secrets", key="storage-account-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

start_time = datetime.datetime.utcnow()
bronze_delta_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/engineering/bronze_telemetry/"
silver_clean_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/engineering/telemetry_cleaned/"
silver_quarantine_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/engineering/telemetry_quarantine/"

try:
    df_bronze = spark.read.format("delta").load(bronze_delta_path)
    records_read = df_bronze.count()

    df_typed = (
        df_bronze
        .withColumn("vin", F.col("vin").cast(StringType()))
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withColumn("battery_soc_percent", F.col("soc").cast(DoubleType()))
        .withColumn("motor_rpm", F.col("motor_rpm").cast(DoubleType()))
        .withColumn("reading_id", F.sha2(F.concat_ws("||", F.col("vin"), F.col("timestamp")), 256))
        .dropDuplicates(["reading_id"])
    )

    valid_condition = (
        (F.col("vin").isNotNull()) &
        (F.col("event_timestamp").isNotNull()) &
        (F.col("battery_soc_percent") >= 0.0) &
        (F.col("battery_soc_percent") <= 100.0) &
        (F.col("motor_rpm") >= 0.0)
    )

    df_valid = df_typed.filter(valid_condition).withColumn("_silver_processed_at", F.current_timestamp())
    df_quarantine = (
        df_typed.filter(~valid_condition)
        .withColumn(
            "_quarantine_reason",
            F.when(F.col("vin").isNull(), "Null VIN")
             .when((F.col("battery_soc_percent") < 0) | (F.col("battery_soc_percent") > 100), "Invalid SoC")
             .otherwise("RPM or Timestamp Anomaly")
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    df_valid.write.format("delta").mode("overwrite").partitionBy("vin").save(silver_clean_path)
    valid_count = df_valid.count()
    
    quarantine_count = df_quarantine.count()
    if quarantine_count > 0:
        df_quarantine.write.format("delta").mode("append").save(silver_quarantine_path)

    end_time = datetime.datetime.utcnow()

    log_pipeline_audit(
        run_id=RUN_ID,
        stage_name="Bronze_to_Silver_Refinement",
        layer="SILVER",
        status="SUCCEEDED",
        start_time=start_time,
        end_time=end_time,
        rows_read=records_read,
        rows_written=valid_count,
        rows_quarantined=quarantine_count
    )

    # Lineage Version Tracking
    version = spark.sql(f"DESCRIBE HISTORY delta.`{silver_clean_path}`").select("version").first()[0]
    spark.createDataFrame([(
        RUN_ID, "Bronze_to_Silver", bronze_delta_path, silver_clean_path, version, datetime.datetime.utcnow()
    )], ["pipeline_run_id", "stage_name", "input_path", "output_path", "delta_version_snapshot", "created_at"])\
    .write.format("delta").mode("append").save(f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net/lineage/stage_snapshots")

    dbutils.notebook.exit(f"SUCCESS: Clean {valid_count}, Quarantined {quarantine_count}")

except Exception as err:
    end_time = datetime.datetime.utcnow()
    log_pipeline_audit(
        run_id=RUN_ID,
        stage_name="Bronze_to_Silver_Refinement",
        layer="SILVER",
        status="FAILED",
        start_time=start_time,
        end_time=end_time,
        error_msg=str(err)
    )
    raise err