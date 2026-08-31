# Search Export API Contracts

One API surface, three source engines. What a client sends, what it polls, and what the
`search_export_processor` worker is guaranteed to receive for AWS Security Lake, Azure Data
Explorer, and Microsoft Sentinel.

| | |
| --- | --- |
| Search service | context path `/api/search` |
| Core service | context path `/v2` |
| Worker | databahn-jobs · `search_export_processor` |

Companion to `docs/adx-search-export.md` and `docs/sentinel-search-export.md`, which cover *why*
each engine works the way it does. This document is the interface only.

**Status:** Security Lake is on master. ADX and Sentinel are implemented and unit-tested but not
yet committed — branches `search/adx-export` (databahn-jobs) and `export-adx` (backend-service).
Neither has been run against a live cluster or workspace.

---

## 1. Request flow

Export is a side effect of a search, not its own endpoint. A client asks for a search with
`exportOptions.exportData = true`, gets back a report id, and polls that report until a download
link appears.

**1. Resolve the destination** — optional

```
GET /api/search/data_stores/{dataStoreId}/data_sets/{dataSetId}/export/global_destination
```

Returns the tenant global destination whose type matches the data store's query engine. Skip it if
the client already knows which `globalDestinationId` to use, but calling it first is how a UI
avoids offering an unsupported pairing.

**2. Submit the search with export enabled**

```
POST /api/search
```

Runs the interactive search and, if `exportOptions.exportData` is set, queues an export alongside
it. The response carries `exportReportId` — the `audit_report` row the worker will pick up.

**3. Poll the report**

```
GET /v2/audit-report/{exportReportId}
```

`status` moves `REQUESTED → PROCESSING → COMPLETED`, or `FAILED`. There is no push notification
and no webhook; polling is the contract.

**4. Download**

On `COMPLETED`, `download_link` holds a presigned S3 URL or an Azure blob SAS URL — a single file,
ready to fetch directly. `download_link_expiry` is **7 days** out.

---

## 2. Export request

Identical for all three sources. Nothing in the request names an engine — the engine is derived
server-side from the data store, and an unsupported pairing is rejected before anything is queued.

```jsonc
// POST /api/search
{
  "dataStoreId": "7c3f…",
  "dataSetId":   "a91b…",
  "query":       "SecurityEvent | where EventID == 4625",
  "startTime":   1700000000000,
  "endTime":     1700086400000,
  "limit":       5000,
  "selfContainedQuery": false,
  "exportOptions": {
    "exportData":          true,
    "globalDestinationId": "4d20…",
    "format":              "csv",
    "delimiter":           ",",
    "includeHeader":       true
  }
}
```

| Field | | Notes |
| --- | --- | --- |
| `dataStoreId` | required | Determines the query engine, credentials, and which destinations are legal |
| `dataSetId` | required | Supplies the table guardrail and, for KQL sources, the time column |
| `query` | optional | SQL or KQL depending on the source. Omitted means the dataset's own table, unfiltered |
| `startTime` / `endTime` | required | Epoch millis. Injected into the query server-side as a partition or time filter |
| `limit` | optional | Absent, `0`, or `-1` means "as many as allowed" — capped at `1,000,000`, then clamped again per engine (§4) |
| `selfContainedQuery` | optional | The query already carries its own time bounds. Skips filter injection; the table allow-list is still enforced |
| `exportOptions.exportData` | required | Must be `true`. Anything else runs a plain search and queues nothing |
| `exportOptions.globalDestinationId` | required | Where the finished file is written. Must be a type the engine supports |
| `exportOptions.format` | optional | `csv` (default) · `json` (NDJSON) · `xlsx` |
| `exportOptions.delimiter` | optional | Default `,`. CSV only; multi-character support varies by engine |
| `exportOptions.includeHeader` | optional | Default `true`. CSV only |

### Response

```jsonc
{
  "columns": [], "data": [], "stats": {},   // the interactive search result
  "query": "…", "actualQuery": "…",         // as submitted vs. as planned
  "exportReportId": "9f4c…",                // poll this
  "exportError": null                       // set when queueing was refused
}
```

A populated `exportError` with a null `exportReportId` means the search succeeded but the export
was never queued — an unsupported engine or destination, a missing workspace, a rejected query.
The message says which.

---

## 3. Polling and download

```
GET /v2/audit-report/{exportReportId}
```

```jsonc
{
  "id": "9f4c…",
  "name": "SecurityEvent 2026-08-21 15:04:11",
  "report_type": "SEARCH_EXPORT",
  "status": "COMPLETED",                       // REQUESTED | PROCESSING | COMPLETED | FAILED
  "report_configuration": {
    "searchExportConfig": { }                  // the per-source contract in §6–8
  },
  "download_link": "https://…?sig=…",
  "download_link_expiry": "2026-08-28T09:34:11Z"
}
```

`GET /v2/audit-report` without an id lists reports page-wise, which is how a UI shows an export
history. `report_configuration.searchExportConfig` is readable throughout — it is the same object
the worker consumes, so it doubles as the debugging surface when an export fails.

---

## 4. Support matrix

The engine is derived from the data store; the destination must be one the engine can write. A
request that violates this table is rejected at submit time, not at run time.

| Source | Engine | Destinations | Row ceiling | Mechanism | Resumable |
| --- | --- | --- | --- | --- | --- |
| **Security Lake** | `ATHENA` | `S3`, `S3_PARQUET` | 1,000,000 | Athena `UNLOAD` → S3 staging | yes |
| **Data Explorer** | `KUSTO_ADX` | `AZURE_BLOB` | 1,000,000 | Kusto `.export async` → blob staging | yes |
| **Sentinel** (analytics) | `KUSTO_LAW` | `S3`, `S3_PARQUET`, `AZURE_BLOB` | 200,000 | One query-API call, encoded in the worker | no — restarts |
| **Sentinel** (data lake) | `KUSTO_LAKE` | — | — | Not supported — rejected at submit time | — |

The ceilings differ because the mechanisms do. Athena and ADX both write server-side to storage,
so the only limit is the export limit itself. Sentinel returns rows inside an HTTP response, so it
is bounded by the Log Analytics service limits — which is why its cap sits at a fifth of the others.

---

## 5. Shared config fields

Every `searchExportConfig` carries these, regardless of source. The per-source sections add to this
set rather than replacing it.

| Field | | Value |
| --- | --- | --- |
| `queryEngine` | required | Selects the worker's execution path |
| `destinationType` | required | `S3` · `S3_PARQUET` · `AZURE_BLOB` |
| `query` | required | Fully planned. The worker runs it verbatim and never rewrites it |
| `dataStoreId` | required | The worker reloads source credentials from it; secrets never travel in the config |
| `destinationId` | required | Resolved destination — **not** the global destination id |
| `globalDestinationId` | optional | Echoed back from the request for traceability |
| `dataSetId` / `dataSetName` / `tableName` | optional | `tableName` becomes part of the output filename |
| `dataStoreType` | optional | `EXTERNAL_STORAGE` · `DERIVED_DATASTORE` · `DATABAHN_DESTINATION` · … |
| `format` / `delimiter` / `includeHeader` | optional | Carried through from `exportOptions`, with defaults applied |
| `startTime` / `endTime` | optional | Informational for ADX and Sentinel — the filter is already inside `query` |
| `externalSearchProvider` | optional | Set for external stores; null for databahn-managed ones |
| `queryExecutionId` | **worker writes** | Never sent by the backend. See each source for whether it is produced at all |
| `executionStartedAt` | **worker writes** | Drives stale-job detection |

---

## 6. AWS Security Lake — `ATHENA`

Reaches search two ways, and the export config differs between them. The query is SQL, planned
with an OCSF partition filter and a `LIMIT`.

### Data store shapes

**External store** — `type = EXTERNAL_STORAGE` (or `DERIVED_DATASTORE`) with
`externalSearchProvider = SECURITY_LAKE`. Legacy stores may declare `S3` plus
`connectorConfig.security_lake = "true"`; both are treated identically.

| `connectorConfig` key | | Notes |
| --- | --- | --- |
| `region` | required | AWS region for the Athena and S3 clients |
| `output_bucket` / `bucket` | required | Athena staging bucket |
| `auth_type` | required | `role_based` or `key_based` |
| `role_arn`, `external_id` | conditional | Role-based auth. Confidential — held in Secrets Manager |
| `access_key_id`, `secret_access_key` | conditional | Key-based auth. Confidential |
| `glue_database` | optional | Glue catalog database backing the dataset |

**Pipeline destination store** — `type = DATABAHN_DESTINATION` linked to an `AWS_SECURITY_LAKE`
destination. Credentials live on the destination, under different key names: `aws_region`,
`aws_security_lake_role_arn`, `aws_external_id`,
`aws_security_data_lake_custom_source_location`, `aws_account_id`, `ocsf_id`.

### searchExportConfig

```jsonc
{
  "queryEngine":            "ATHENA",
  "destinationType":        "S3",
  "query":                  "SELECT … WHERE eventday BETWEEN … LIMIT 5000",
  "database":               "amazon_security_lake_glue_db_us_east_1",
  "externalSearchProvider": "SECURITY_LAKE",
  "region":                 "us-east-1",
  "athenaOutputLocation":   "s3://<output_bucket>/.databahn_out",
  "dataStoreId":            "…",
  "destinationId":          "…",
  "queryExecutionId":       "<Athena execution id — worker writes>"
}
```

### Behaviour

- **Staging.** `UNLOAD` writes to `athenaOutputLocation`; the worker streams those files to the
  destination and deletes them afterward.
- **Resume.** The Athena execution id is durable, so a stale `PROCESSING` report reattaches to a
  running or finished query instead of re-running it.
- **Formats.** `csv` and `json` stream straight through; `xlsx` and multi-character delimiters
  route via Parquet and a local encoder.

---

## 7. Azure Data Explorer — `KUSTO_ADX`

The query is KQL, planned complete — table guardrail, time filter, sort, and `take` — and handed to
a Kusto `.export async` control command that writes directly to blob.

### Data store shapes

**External store** — `type = EXTERNAL_STORAGE`, `externalSearchProvider = AZURE_DATA_EXPLORER`.

| `connectorConfig` key | | Notes |
| --- | --- | --- |
| `adx_cluster_uri` | required | e.g. `https://cluster.eastus.kusto.windows.net` |
| `adx_database` | required | Copied into `config.database` at submit time |
| `azure_tenant_id`, `azure_client_id` | required | Entra app credentials |
| `azure_client_secret` | required | The only confidential attribute — resolved from `secretId` |

**Pipeline destination store** — `type = DATABAHN_DESTINATION` over an `AZURE_DATA_EXPLORER`
destination, whose keys are named differently: `adx_cluster_endpoint_url`, `adx_database_name`,
`adx_table_name`, `adx_tenant_id`, `adx_client_id`, `adx_client_secret`.

### searchExportConfig

```jsonc
{
  "queryEngine":            "KUSTO_ADX",
  "destinationType":        "AZURE_BLOB",          // the only legal value
  "query":                  "SigninLogs\n| where TimeGenerated …\n| sort by …\n| take 5000",
  "database":               "SecurityLogs",
  "externalSearchProvider": "AZURE_DATA_EXPLORER",
  "dataStoreId":            "…",
  "destinationId":          "…",
  "queryExecutionId":       "<Kusto OperationId — worker writes>"
}
```

> **Destination constraint.** `.export` stages into the export destination's own blob container, so
> `AZURE_BLOB` is the only destination that works. An S3 global destination is rejected at submit
> time with *"ADX export requires an AZURE_BLOB destination"*.

### Behaviour

- **No result cap.** The command is prefixed `set notruncation;`, so the 500k-row query truncation
  does not apply — the `take` in the query is the real ceiling.
- **Resume.** The Kusto OperationId is durable and pollable, so a stale report reattaches rather
  than re-exporting.
- **Cleanup.** Staged blobs are namespaced per report and deleted after a successful upload, or
  after the final failed retry, which also cancels a still-running operation.
- **Permissions.** The service principal must be allowed to run `.export` control commands on the
  database, not merely query it.
- **Formats.** Kusto emits comma-CSV and tab-TSV natively; any other delimiter, and `xlsx`, route
  via Parquet.

---

## 8. Microsoft Sentinel — `KUSTO_LAW`

Analytics tier only. Neither Sentinel tier accepts control commands, so there is no `.export` and
nothing to stage — the worker runs one Log Analytics query, encodes the rows itself, and uploads
the result.

### Data store shape

`type = EXTERNAL_STORAGE`, `externalSearchProvider = AZURE_SENTINEL`.

| `connectorConfig` key | | Notes |
| --- | --- | --- |
| `workspace_id` | required | Log Analytics workspace GUID |
| `storage_tier` | optional | Blank means `ANALYTICS`. `LAKE` is rejected |
| `azure_tenant_id`, `azure_client_id` | required | Entra app credentials |
| `azure_client_secret` | required | The only confidential attribute — resolved from `secretId` |
| `workspace_name` | lake only | Unused by export; the lake tier has no export path |

### searchExportConfig

```jsonc
{
  "queryEngine":            "KUSTO_LAW",
  "destinationType":        "AZURE_BLOB",          // or S3 / S3_PARQUET
  "query":                  "SecurityEvent\n| where TimeGenerated …\n| sort by …\n| take 5000",
  "storageTier":            "ANALYTICS",
  "externalSearchProvider": "AZURE_SENTINEL",
  "kqlTable":               "SecurityEvent",       // forward-compat, unread today
  "kqlTimeColumn":          "TimeGenerated",       // forward-compat, unread today
  "dataStoreId":            "…",
  "destinationId":          "…"
  // no "database"        — Sentinel has no server-side database to address
  // no "queryExecutionId" — no server-side operation exists to reattach to
}
```

> **Row ceiling is 200,000, not 1,000,000.** Log Analytics returns rows inside the HTTP response,
> bounded at 500,000 rows, ~64 MB, and 10 minutes per request. Bytes bind first on a wide table full
> of `dynamic` columns, so the export cap sits well below the row limit. A query that exceeds the
> limits anyway **fails** — Log Analytics answers `HTTP 200` with partial results, and accepting
> that would publish a truncated file that looks complete. The report status becomes `FAILED` with
> guidance to narrow the range or add filters.

### Behaviour

- **Any destination.** Rows are encoded in the worker and uploaded client-side, so unlike ADX there
  is no staging requirement forcing a blob destination.
- **No resume.** Nothing runs server-side, so a stale `PROCESSING` report restarts from scratch.
  `queryExecutionId` is never written.
- **All formats local.** No server-side format to choose, so every delimiter — including
  multi-character — and `xlsx` are handled directly, with no Parquet round trip.
- **Permissions.** The service principal needs **Log Analytics Reader** on the workspace, and
  `api.loganalytics.io` must be reachable from the jobs pod.
- **Aggregations work unchanged.** A `summarize` pipeline is one query like any other.

---

## 9. Worker guarantees

True for all three sources — what a client can rely on once a report is queued.

- **Atomic claim.** A report is claimed by exactly one pod. Duplicate processing is not possible.
- **Retries.** Up to 3 attempts. A stale `PROCESSING` report is reclaimed after the staleness
  cutoff and either resumed or restarted, depending on the engine.
- **Output.** One file per report at
  `exports/<reportId>/SearchExport_<table>_<datetime>.<ext>` — never a directory of parts, whatever
  the engine staged along the way.
- **Link lifetime.** 7 days, recorded in `download_link_expiry`.
- **Empty results succeed.** A range with no rows completes with a header-only file, not a failure.
- **Secrets stay put.** Credentials are never written into `report_configuration`; the worker
  resolves them from `dataStoreId` via Secrets Manager.

---

## 10. Failure modes

| Surfaces as | When | What the client should do |
| --- | --- | --- |
| `exportError` on the search response | Queueing was refused — unsupported engine or destination, missing workspace or database, rejected query | Fix the request. No report exists to poll |
| `status: FAILED` | The worker ran and could not finish — bad credentials, unreachable source, a result exceeding the engine's limits | Read the failure message; for Sentinel, narrow the time range or add filters |
| `status: PROCESSING` for a long time | Normal for large Athena and ADX exports, which run server-side | Keep polling. The staleness sweep will reclaim a genuinely dead job |
| `COMPLETED` with no `download_link` | The query matched no rows and produced no bytes | Treat as an empty result, not an error |

---

Sources: `SearchServiceImpl`, `SearchExportDestinationResolver`, `KqlQueryPlanner`
(backend-service) · `internal/searchExport` (databahn-jobs).
