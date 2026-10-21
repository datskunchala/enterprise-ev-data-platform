# 6. Code Walkthrough

This guide explains the intended role of the exported notebooks. For exact transformations and parameters, use the corresponding `.py` source file. Do not treat this overview as a substitute for reading the actual code.

## 6.1 Notebook 00 — Control initialization
**Source:** `src/databricks/00_control_init Notebook 2026-10-18 14:04:44.py`

Purpose: initialize the catalog/schema or control objects needed by downstream processing. Review the configured catalog, schema, storage paths, and whether operations are create-if-not-exists or destructive replacements.

## 6.2 Notebook 01 — All-domain Bronze
**Source:** `src/databricks/01_all_domains_bronze Notebook 2026-10-18 14:05:14.py`

Purpose: read domain source files and land them in Bronze with source/run metadata where implemented. Key concepts to explain in an interview:
- file discovery and format-specific reading;
- schema inference versus explicit schemas;
- metadata columns for lineage;
- output path and write mode;
- behaviour when the same files are processed again.

Check rerun semantics: append can duplicate rows unless the pipeline has an explicit deduplication or idempotency strategy.

## 6.3 Notebook 02 — All-domain Silver
**Source:** `src/databricks/02_all_domains_silver Notebook 2026-10-18 14:06:12.py`

Purpose: transform raw domain data into standardized outputs. Review data-type casting, null/invalid handling, domain rules, rejected/quarantine handling and output modes. A successful notebook run does not by itself prove every rule is correct; compare input, accepted and rejected counts.

## 6.4 Notebook 02 (alternate) — Bronze to Silver
**Source:** `src/databricks/02_bronze_to_silver Notebook 2026-10-18 13:46:20.py`

This is a separate exported implementation/variant. Treat it as a reference unless the ADF pipeline explicitly points to it. Avoid running multiple variants against the same destination without understanding their write modes.

## 6.5 Notebook 03 — All-domain Gold
**Source:** `src/databricks/03_all_domains_gold Notebook 2026-10-18 14:06:40.py`

Purpose: prepare curated reporting outputs such as dimensions, facts and aggregate KPIs. Inspect join keys, grain, surrogate/business keys, date derivation, aggregation logic and write modes. SCD Type 2 should only be claimed as complete after validating current-row flags, effective dates, change detection and historical versions.

## 6.6 Notebook 04 — Cost, billing and alerting
**Source:** `src/databricks/04_cost_billing_alerting Notebook 2026-10-18 14:07:05.py`

Purpose: capture cost/audit/alert-related information where implemented. Distinguish estimated or calculated cost from actual Azure invoice charges. Confirm that every metric is traceable to a source and that alert thresholds are explicitly configured.

## 6.7 Alternate control/audit schema initialization
**Sources:**
- `src/databricks/Initialize Control & Audit SchemasNotebook 2026-10-18 13:52:36.py`
- `src/databricks/00_control_init Notebook 2026-10-18 14:04:44.py`

These may overlap in purpose. Identify which notebook is invoked by ADF before deleting or consolidating either file.

## 6.8 SQL verification
**Source:** `sql_commands_commented.sql`

Contains commented SQL examples for table inspection, row counts, duplicate checks and KPI table creation/rebuilds. Check the catalog/schema at the top of each query and be cautious with `CREATE OR REPLACE TABLE`, because it replaces the target table contents.

## 6.9 ADF JSON
**Source:** `infra/adf/pl_ev_lakehouse_master_sanitized.json`

Sanitized reference for the master pipeline. Linked services, parameters, notebook paths, workspace IDs and storage configuration may require environment-specific edits. Recreate missing dependencies in ADF rather than assuming this JSON is a full deployable factory export.
