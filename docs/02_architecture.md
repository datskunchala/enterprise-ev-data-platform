# 2. Architecture

## 2.1 High-level flow

![High-level diagram](architecture/high_level_architecture.png)
Source/sample files → ADLS Gen2 → ADF orchestration → Databricks notebooks → Bronze/Silver/Gold datasets → Power BI.

![Architecture image](architecture/enterprise_ev_architecture.png)

## 2.2 High-level component responsibilities
- **Source files:** synthetic datasets representing EV telemetry and related business domains.
- **ADLS Gen2:** durable storage for raw and curated files/tables. The project uses medallion-style containers/folders.
- **Azure Data Factory:** coordinates the notebook activities in the master pipeline.
- **Databricks / PySpark:** performs ingestion, transformation, aggregation and table-writing operations.
- **Delta Lake:** supports Delta-formatted outputs where configured in the notebooks.
- **Control/audit:** stores run/audit/cost-related outputs where implemented.
- **Power BI Desktop:** connects to reporting tables and renders report pages.

## 2.3 Low-level architecture

![Low-level diagram](architecture/low_level_architecture.png)
```mermaid
flowchart TB
  subgraph Inputs
    CSV[CSV domain files]
    JSON[JSON inputs]
    TXT[Delimited TXT inputs]
  end
  subgraph Storage[ADLS Gen2]
    SOURCE[source / landing]
    BRONZE[bronze]
    SILVER[silver]
    GOLD[gold]
    CONTROL[control: audit / cost / lineage]
  end
  subgraph Orchestration[ADF master pipeline]
    C[Control initialization]
    B[Bronze processing]
    S[Silver processing]
    G[Gold processing]
    A[Cost / audit / alerting]
  end
  subgraph Compute[Databricks]
    PYS[PySpark notebook jobs]
    DELTA[Delta tables / aggregates]
  end
  subgraph Consumption
    WH[Databricks SQL serving connection]
    PBI[Power BI Desktop]
    PDF[PDF export]
  end
  CSV --> SOURCE
  JSON --> SOURCE
  TXT --> SOURCE
  SOURCE --> B
  C --> B --> BRONZE
  B --> S --> PYS
  BRONZE --> PYS
  PYS --> SILVER
  SILVER --> G
  G --> DELTA --> GOLD
  A --> CONTROL
  GOLD --> WH --> PBI --> PDF
```

## 2.4 Implementation/execution diagram

![Implementation flow](architecture/implementation_architecture.png)
```mermaid
sequenceDiagram
  participant User
  participant ADF as Azure Data Factory
  participant DBX as Databricks
  participant Lake as ADLS Gen2
  participant PBI as Power BI
  User->>ADF: Trigger master pipeline
  ADF->>DBX: Run Bronze notebook
  DBX->>Lake: Read source and write Bronze
  ADF->>DBX: Run Silver notebook
  DBX->>Lake: Validate/transform and write Silver
  ADF->>DBX: Run Gold notebook
  DBX->>Lake: Write curated facts/dimensions/KPIs
  ADF->>DBX: Run cost/audit notebook
  DBX->>Lake: Write control/audit/cost outputs where configured
  User->>PBI: Refresh report
  PBI->>Lake: Read via configured Databricks serving connection
```

## 2.5 Medallion layers
- **Bronze:** raw or minimally processed source records.
- **Silver:** cleaned/standardized records and domain-oriented outputs; quarantine behaviour depends on notebook logic.
- **Gold:** business-ready fact, dimension and aggregate outputs for analytics.
- **Control:** run metadata, audit, cost and lineage information where implemented.

## 2.6 Identity and networking
The architecture depends on correctly configured Databricks workspace permissions, ADLS access, ADF linked services and Power BI connectivity. Configure these in the Azure environment; do not put secrets in repository files.
