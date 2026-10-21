# 8. Source Code Review and Interpretation

This repository contains notebook exports from the project and an ADF pipeline reference. Their presence documents the implementation artifacts; it does not prove every feature commonly associated with a production lakehouse has been completed.

## Review before rerunning
1. **Write modes:** identify append, overwrite, merge and `CREATE OR REPLACE` operations. Overwrite/replacement can destroy prior outputs; append can duplicate data.
2. **Idempotency:** run the pipeline twice against the same inputs and compare row counts and business keys. A successful second run is not sufficient if duplicate rows are possible.
3. **Watermarks:** confirm where watermark state is stored, how it advances after successful processing, and how late-arriving data is handled.
4. **SCD Type 2:** confirm change detection, effective start/end timestamps, current-row flag and historical retention.
5. **Audit:** verify that audit writes use the expected Delta table path and schema, and that audit failures are not silently ignored.
6. **Quarantine:** check whether rejected records are preserved with reason codes and can be reprocessed.
7. **Schema evolution:** ensure unexpected columns or incompatible type changes fail or are handled intentionally.
8. **Credentials:** use managed identity or secret management; do not embed secrets in notebooks or JSON.
9. **Power BI grain:** ensure measures do not double-count monthly aggregates when filtered or summed.

## Known environment-specific items
- The notebooks use environment-specific catalog, schema and ADLS paths; replace them for another workspace.
- The sanitized ADF JSON may not include every linked service, dataset, trigger, integration runtime or global parameter required for deployment.
- Use the primary notebook sequence referenced by ADF; alternative notebook exports are retained for traceability, not necessarily for parallel execution.
