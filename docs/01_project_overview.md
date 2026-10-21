# 1. Project Overview

## 1.1 Objective
Build an Azure data-engineering MVP that turns EV/IoT and operational sample data into curated datasets for engineering, vehicle-health and warranty reporting.

## 1.2 Business questions
- How many telemetry events are present in the current dataset?
- How does telemetry activity vary by month?
- How many vehicle-health readings are flagged as abnormal?
- Which health flags occur most frequently?
- What is the total warranty claim cost, broken down by claim status?
- How do health indicators compare between reporting months?

## 1.3 Scope
Included: synthetic/source-file package, medallion-layer processing notebooks, ADF orchestration reference, Gold reporting outputs, verification queries, and Power BI Desktop report with a PDF snapshot.

Not automatically included: production-grade streaming, SLA monitoring, full CI/CD, comprehensive automated unit tests, complete multi-environment deployment, or guaranteed idempotency. These require separate implementation and validation.

## 1.4 Intended audience
Azure Data Engineer recruiters, technical interviewers and engineers reviewing an end-to-end lakehouse portfolio project.

## 1.5 Success criteria
- ADF can orchestrate the intended notebook sequence.
- Bronze/Silver/Gold outputs are created in the target environment.
- Validation queries return documented counts for the same input snapshot.
- Power BI visuals use the curated KPI datasets and can be refreshed after connection setup.
- Credentials and sensitive environment configuration are not stored in Git.
