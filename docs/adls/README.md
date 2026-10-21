# ADLS Gen2 Storage Evidence

This folder contains screenshots of the Azure Data Lake Storage Gen2 layout used by the Enterprise EV Data Platform.

## Storage containers

![ADLS containers](01_adls_storage_containers.png)

The screenshot shows the `bronze`, `silver`, `gold`, and `control` containers. The storage account and resource identifiers visible in screenshots should be reviewed before publishing the repository publicly.

## Bronze layer — source CSV files

![Bronze domain folders](02_bronze_csv_files_by_domain.png)

![Bronze raw CSV files](03_bronze_raw_csv_files.png)

Bronze contains source/raw data organized into business-domain folders. Treat the contents as raw/landing data, before Silver cleansing.

## Silver layer — cleansed and transformed data

![Silver domain folders](04_silver_layer_domain_folders.png)

![Silver transformed data files](05_silver_transformed_data_files.png)

These screenshots show domain-oriented folders and output files in Silver. File names and folder paths should be checked against the current notebooks if documenting exact table/file contracts.

## Gold layer — analytics-ready outputs

![Gold layer](06_gold_layer.png)

![Gold facts and dimensions](07_gold_fact_and_dimension_tables.png)

The screenshots show Gold warehouse outputs, including folders corresponding to dimension, fact, and aggregate datasets. The exact visible names may be truncated in the Azure portal; use notebook code or a full listing for definitive names.

## Control layer

![Control layer](08_control_audit_cost_lineage.png)

The control container is organized into audit, cost-monitoring, and lineage folders, as shown in the Azure portal screenshot.

## Notes before publishing

- Screenshots are implementation evidence, not a substitute for the source code or schema documentation.
- Confirm no secrets, access keys, SAS tokens, connection strings, personal data, or sensitive tenant details are visible.
- Avoid publishing large or confidential source datasets; prefer small synthetic CSV samples under `data/samples/`.
- Azure resource names and timestamps can be environment-specific.
