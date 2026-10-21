# 7. Cost, Security and Operational Notes

## 7.1 Cost components
- ADLS Gen2 storage and transactions
- Azure Data Factory orchestration/activity execution
- Databricks compute and SQL warehouse/serverless compute, depending on configuration
- Power BI licensing and sharing requirements

A notebook's cost estimate is not the same as a billed amount. Verify actual Azure Cost Management and Databricks usage data.

## 7.2 Keep costs controlled
- Stop/pause interactive compute and SQL warehouses when not in use, where supported.
- Avoid repeated large refreshes while developing visuals.
- Use small synthetic datasets for development.
- Set budgets/alerts in Azure and review the billing scope.
- Validate that the Power BI refresh is not triggering unexpectedly frequent compute.

## 7.3 Secrets
Never commit:
- Databricks personal access tokens;
- Azure storage account keys;
- service-principal client secrets;
- passwords or connection strings containing credentials;
- `.env` files or local credential caches.

Use managed identities, secret scopes, Key Vault and approved authentication flows where appropriate.

## 7.4 Public repository review
Before making this repository public, review screenshots and JSON for tenant IDs, workspace IDs, account names, URLs, email addresses, tokens, and other details you do not want disclosed. Resource names alone are not always credentials, but they may reveal environment details.

## 7.5 Power BI artifact
The PBIX can contain data-source connection metadata and cached data. Open it and inspect data-source settings before public sharing. The PDF is easier to share but may expose the same business metrics as the report.
