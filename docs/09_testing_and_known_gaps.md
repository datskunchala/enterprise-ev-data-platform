# 9. Testing, Evidence and Known Gaps

## 9.1 Execution evidence
The repository includes screenshots under `screenshots/output/` for Databricks notebook activity and ADF master-pipeline execution. The supplied Power BI PDF is included in `reports/powerbi/` and rendered as three PNG pages under `outputs/powerbi_pdf_pages/`.

## 9.2 Snapshot validation
| Check | Observed result | Interpretation |
|---|---:|---|
| Telemetry fact row count | 300 | Snapshot count, not a throughput benchmark |
| Distinct VINs | 50 | Business-key coverage in telemetry sample |
| Distinct telemetry timestamps | 300 | Reported validation result |
| Duplicate `(VIN, timestamp)` combinations | 0 | Reported validation result |
| Abnormal subset | 153 | Each row in this subset had abnormal flag = 1 |
| Warranty claim count | 80 | Claims in the current warranty KPI source |
| Distinct claim IDs | 80 | No duplicate claim IDs observed in that snapshot |
| Total warranty cost | USD 193,735 | Reported sum across claim statuses |
| Monthly engineering KPI rows | 2 | September and October 2026 in the recorded run |

## 9.3 Monthly health flags
Recorded monthly totals:
- September 2026: 60 abnormal readings, 38 high-battery-temperature flags, 20 low-SOC flags, 12 high-motor-RPM flags.
- October 2026: 93 abnormal readings, 56 high-battery-temperature flags, 41 low-SOC flags, 20 high-motor-RPM flags.

Individual flags overlap. A reading can have more than one flag; do not sum flag counts and call the result unique abnormal readings.

## 9.4 Known gaps / caveats
- The current dashboard is an MVP with a small synthetic dataset and only two monthly buckets.
- The “Monthly Vehicle Reports” KPI shows 93 in the supplied report; confirm whether the intended metric is a sum of monthly reporting-vehicle counts or distinct vehicles.
- The warranty claim data shown in the KPI table does not necessarily include a claim date; do not infer monthly claim trends without a valid date field.
- The health-only table contains only abnormal readings, so its abnormal flag count is expected to equal its row count.
- Thresholds for high battery temperature, low SOC and high motor RPM should be documented and reviewed with engineering/domain stakeholders.
- Rerun/idempotency and incremental watermark behaviour must be tested explicitly.
- The Power BI report must be refreshed against the target workspace before publishing updated figures.
- No claim of production readiness, SLA compliance or real-time ingestion is made.

## 9.5 Suggested final acceptance checklist
- [ ] All notebooks run in sequence with no errors.
- [ ] Input/output counts reconcile.
- [ ] Rerun does not create duplicates or lose history.
- [ ] Delta tables are readable at their expected paths.
- [ ] ADF parameters and linked services are environment-correct.
- [ ] Power BI refresh succeeds from the configured source.
- [ ] KPI measures are checked against SQL.
- [ ] Secrets and environment identifiers are reviewed before public upload.
