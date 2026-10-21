# 4. Data Model and KPI Dictionary

## 4.1 Reporting datasets visible in the MVP
The Power BI report uses monthly engineering/telemetry metrics, monthly vehicle-health metrics and warranty-claim status metrics. The exact fully qualified table names depend on the catalog/schema configured in the workspace.

| Dataset / report area | Purpose | Main fields visible in report or project evidence |
|---|---|---|
| Monthly engineering KPIs | Monthly telemetry and engineering summary | report month, telemetry events, reporting vehicles, average battery temperature, maximum battery temperature, average motor RPM, average state of charge |
| Monthly vehicle-health KPIs | Monthly health-flag summary | `year_month`, `total_readings`, `vehicles_with_readings`, `abnormal_readings`, high-battery-temperature, low-SOC and high-motor-RPM counts |
| Warranty claim KPI by status | Claims and cost grouped by status | `claim_status`, `claim_count`, `total_claim_cost_usd`, `average_claim_cost_usd`, distinct claims/vehicles, refresh timestamp |
| Vehicle health abnormal readings | Rows flagged by health rules | battery temperature, motor RPM, SOC, vehicle/date keys, year/month, health flags and abnormal-reading flag |
| Vehicle SCD2 dimension | Historical vehicle attributes where implemented | vehicle key and business key/validity attributes as defined by the notebook |
| Telemetry fact | Telemetry event-level reporting | VIN, timestamp, telemetry metrics and ingestion/source metadata |

## 4.2 KPI definitions
- **Telemetry Events:** number/sum of telemetry events represented in the selected KPI table. Check whether the source is a monthly aggregate or event-level fact to avoid counting aggregate rows incorrectly.
- **Monthly Vehicle Reports:** the Power BI card displays the sum of `reporting_vehicles` from the monthly engineering KPI dataset. This is a sum of month-level counts, not necessarily a distinct fleet-wide vehicle count across all months.
- **Abnormal Readings:** sum of the monthly abnormal-reading count or count of rows in the abnormal-only dataset, depending on visual binding.
- **Total Warranty Cost (USD):** sum of warranty claim cost, displayed in rounded form as 193.74K.
- **Health Flag Comparison:** monthly counts for high battery temperature, low state of charge and high motor RPM. Flag categories can overlap on the same reading; their counts must not be summed to estimate unique abnormal readings.

## 4.3 Observed values
- 300 telemetry events
- 50 distinct VINs in telemetry validation
- 153 abnormal readings in the abnormal subset
- 80 warranty claims
- USD 193,735 total warranty claim cost
- Two monthly engineering KPI rows (September and October 2026)

These values are snapshot-specific and may change after refresh or a new sample-data load.
