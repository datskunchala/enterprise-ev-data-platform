# 3. Implementation Walkthrough

## Step 1 — Prepare inputs
Use the synthetic sample package at `data/sample_inputs/EV_IoT_10_Departments_Source_Files.zip`. Inspect file names, schema, formats and row counts before uploading to your own ADLS Gen2 landing area.

## Step 2 — Configure ADLS Gen2
Create or verify the required storage containers/folders (for example `source`, `bronze`, `silver`, `gold`, `control`). Paths in notebooks may be environment-specific. Replace account/container/catalog/schema values with your own settings.

## Step 3 — Configure Databricks
Import the `.py` notebook exports from `src/databricks/`. Review widgets, storage paths, catalog/schema names, output modes and table names before executing.

## Step 4 — Configure ADF
Use `infra/adf/pl_ev_lakehouse_master_sanitized.json` as a reference. Recreate or import the pipeline according to your ADF environment and reconnect linked services. The reference is sanitized and should not be expected to run without environment configuration.

## Step 5 — Execute orchestration
The captured ADF evidence shows a four-activity sequence:
1. Bronze ingestion
2. Silver cleansing/transformation
3. Gold warehouse/curated outputs
4. Cost and alerting/audit processing

Run in order and inspect each activity output. Confirm the actual pipeline dependency graph in your ADF factory before relying on it.

## Step 6 — Verify data
Run `sql_commands_commented.sql` in the appropriate Databricks SQL context. Compare counts, distinct keys, duplicate checks and KPI outputs against the current input snapshot. A successful activity status alone is not proof of business-data correctness.

## Step 7 — Connect Power BI
Open `reports/powerbi/Enterprise_EV_IoT_Dashboard.pbix`. Reconfigure data-source settings to your own Databricks SQL endpoint. Validate the loaded tables and refresh the report. Use the PDF in the same folder as a visual reference.

## Step 8 — Capture evidence
Keep screenshots of successful ADF activities, Databricks outputs, ADLS layer layout, validation queries and each Power BI page. Avoid screenshots that expose access tokens or secrets.
