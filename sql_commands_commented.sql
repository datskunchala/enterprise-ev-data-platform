-- EV IoT Data Platform - SQL command history and reusable reporting queries
-- Catalog: adb_enterprise_ev_mvp_001
-- Schema:  default
--
-- These are SQL commands used or proposed during our conversation, not the full
-- PySpark source code for notebooks 00-04.
-- In a Databricks notebook SQL cell, add %sql on its own first line if needed.
-- SELECT/SHOW/DESCRIBE statements are read-only. CREATE OR REPLACE TABLE creates
-- a persistent Delta table but replaces its contents whenever rerun.

-- 01. Current catalog and schema (read-only)
SELECT current_catalog(), current_schema();

-- 02. List catalogs (read-only)
SHOW CATALOGS;

-- 03. List schemas in the project catalog (read-only)
SHOW SCHEMAS IN adb_enterprise_ev_mvp_001;

-- 04. List tables in the project's default schema (read-only)
SHOW TABLES IN adb_enterprise_ev_mvp_001.default;

-- 05. Count Gold telemetry records (read-only baseline check)
SELECT COUNT(*) AS total_records
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry;

-- 06. Count SCD Type 2 vehicle dimension records (read-only)
-- Note: multiple versions per VIN may be valid in an SCD2 dimension.
SELECT COUNT(*) AS total_records
FROM adb_enterprise_ev_mvp_001.default.dim_vehicle_scd2;

-- 07. Preview telemetry records to inspect real columns (read-only)
SELECT *
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry
LIMIT 10;

-- 08. Check repeated VIN + timestamp combinations in Gold telemetry (read-only)
-- Assumption: one event per VIN per timestamp. No rows means no duplicate key
-- combinations in this table, not proof that all ingestion is idempotent.
SELECT vin, timestamp, COUNT(*) AS duplicate_count
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry
GROUP BY vin, timestamp
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 09. Telemetry table summary (read-only)
-- Distinct timestamps are counted across all vehicles, not unique vehicle events.
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT vin) AS distinct_vehicles,
    COUNT(DISTINCT timestamp) AS distinct_timestamps,
    COUNT(DISTINCT _source_file) AS source_files
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry;

-- 10. Telemetry records by source file (read-only lineage check)
SELECT _source_file, COUNT(*) AS record_count
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry
GROUP BY _source_file
ORDER BY record_count DESC;

-- 11. Monthly engineering KPI preview (read-only; does not create a table)
SELECT
    DATE_TRUNC('MONTH', timestamp) AS report_month,
    COUNT(*) AS telemetry_events,
    COUNT(DISTINCT vin) AS reporting_vehicles,
    ROUND(AVG(battery_temp_c), 2) AS avg_battery_temp_c,
    ROUND(MAX(battery_temp_c), 2) AS max_battery_temp_c,
    ROUND(AVG(motor_rpm), 2) AS avg_motor_rpm,
    ROUND(AVG(soc), 2) AS avg_state_of_charge
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry
GROUP BY DATE_TRUNC('MONTH', timestamp)
ORDER BY report_month;

-- 12. Create/rebuild permanent monthly engineering KPI Delta table
-- CAUTION: CREATE OR REPLACE replaces the table contents each time it runs.
CREATE OR REPLACE TABLE
    adb_enterprise_ev_mvp_001.default.monthly_engineering_kpis
USING DELTA
AS
SELECT
    DATE_TRUNC('MONTH', timestamp) AS report_month,
    COUNT(*) AS telemetry_events,
    COUNT(DISTINCT vin) AS reporting_vehicles,
    ROUND(AVG(battery_temp_c), 2) AS avg_battery_temp_c,
    ROUND(MAX(battery_temp_c), 2) AS max_battery_temp_c,
    ROUND(AVG(motor_rpm), 2) AS avg_motor_rpm,
    ROUND(AVG(soc), 2) AS avg_state_of_charge
FROM adb_enterprise_ev_mvp_001.default.fact_vehicle_telemetry
GROUP BY DATE_TRUNC('MONTH', timestamp);

-- 13. Verify the permanent monthly engineering KPI table (read-only)
SELECT *
FROM adb_enterprise_ev_mvp_001.default.monthly_engineering_kpis
ORDER BY report_month;

-- 14. Inspect warranty claim columns and types (read-only)
DESCRIBE TABLE adb_enterprise_ev_mvp_001.default.fact_warranty_claims_temp;

-- 15. Preview warranty claim records (read-only)
SELECT *
FROM adb_enterprise_ev_mvp_001.default.fact_warranty_claims_temp
LIMIT 10;

-- 16. Overall warranty claim summary (read-only)
-- Validate duplicate claim IDs and cost definitions before treating totals as final.
SELECT
    COUNT(*) AS total_claim_records,
    COUNT(DISTINCT claim_id) AS distinct_claims,
    COUNT(DISTINCT vin) AS vehicles_with_claims,
    SUM(claim_cost_usd) AS total_claim_cost_usd,
    AVG(claim_cost_usd) AS average_claim_cost_usd
FROM adb_enterprise_ev_mvp_001.default.fact_warranty_claims_temp;

-- 17. Warranty claim metrics by status (read-only; not monthly)
SELECT
    claim_status,
    COUNT(*) AS claim_count,
    SUM(claim_cost_usd) AS total_claim_cost_usd,
    ROUND(AVG(claim_cost_usd), 2) AS average_claim_cost_usd
FROM adb_enterprise_ev_mvp_001.default.fact_warranty_claims_temp
GROUP BY claim_status
ORDER BY claim_count DESC;

-- 18. OPTIONAL: Create a permanent warranty KPI table by status.
-- This is not monthly because the inspected source table has no claim_date.
-- CAUTION: CREATE OR REPLACE replaces the table contents whenever rerun.
CREATE OR REPLACE TABLE
    adb_enterprise_ev_mvp_001.default.warranty_claims_kpis_by_status
USING DELTA
AS
SELECT
    claim_status,
    COUNT(*) AS claim_count,
    SUM(claim_cost_usd) AS total_claim_cost_usd,
    ROUND(AVG(claim_cost_usd), 2) AS average_claim_cost_usd,
    COUNT(DISTINCT claim_id) AS distinct_claims,
    COUNT(DISTINCT vin) AS vehicles_with_claims,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM adb_enterprise_ev_mvp_001.default.fact_warranty_claims_temp
GROUP BY claim_status;

-- 19. Verify the optional warranty KPI table (read-only)
SELECT *
FROM adb_enterprise_ev_mvp_001.default.warranty_claims_kpis_by_status
ORDER BY claim_count DESC;

-- NEXT STEPS / SAFETY NOTES:
-- * Section 18 is optional; run it only if you want the status-level table.
-- * Add a genuine claim_date from source data before building monthly warranty KPIs.
-- * Make Bronze ingestion idempotent before scheduling repeated pipeline runs.
-- * ADF notebook activities are separate executions; variables from one notebook
--   do not automatically carry into the next activity.
