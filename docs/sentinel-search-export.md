# Search Export for Microsoft Sentinel (Analytics tier)

Plan for adding Microsoft Sentinel as a search-export **source** in `databahn-jobs`:
run one KQL query against the Log Analytics query API, encode the rows, write them to an
Azure Blob global destination, and hand back a SAS download link.

Companion to `docs/adx-search-export.md`. The contract with backend-service is the same one
ADX uses; only the execution mechanism differs.

Status: **implemented on both sides** (2026-08-21), uncommitted in both working trees —
databahn-jobs on `search/adx-export` and backend-service on `export-adx`. Section 10 records
what landed and what was verified. Not yet run against a real workspace.

**Scope, deliberately narrow:**

- `ANALYTICS` tier only (`KUSTO_LAW`). The `LAKE` tier (`KUSTO_LAKE`) is out of scope.
- **One query, no chunking.** The export row cap is set low enough that a single Log
  Analytics request stays inside the API's limits.
- The window-planning seam is built in (§5) so chunking is an additive change later, not a
  rewrite.

---

## 1. What already exists

### backend-service (`~/Desktop/Workspace/backend-service`, branch `export-adx`)

Sentinel analytics is a fully supported **search** source:

| Concern | Where |
| --- | --- |
| KQL execution | `search/.../search/kql/LogAnalyticsKqlSupport.java`, `LogAnalyticsKqlExecutor.java` |
| Credential validation | `search/.../search/auth/impl/SentinelLogAnalyticsAuthProviderImpl.java` |
| Data store CRUD | `search/.../search/datastore/impl/SentinelExternalDataStoreServiceImpl.java` |
| Query planning | `search/.../search/kql/KqlQueryPlanner.java` |
| Provider → engine mapping | `search/.../search/helper/ExternalDatasetFormatRegistry.java` |
| Config keys | `common/.../constants/AzureConstants.java` |

Data store shape:

- `search_data_store.type = EXTERNAL_STORAGE`
- `configuration.externalSearchDataStoreConfiguration.externalSearchProvider = AZURE_SENTINEL`
- `connectorConfig`:
  - `workspace_id` (required, GUID)
  - `storage_tier` — `ANALYTICS` (default) or `LAKE`; this change handles `ANALYTICS`
  - `azure_tenant_id`, `azure_client_id`, `azure_client_secret`
- `azure_client_secret` is the only confidential attribute (`SENTINEL_CONFIDENTIAL_ATTRIBUTES`),
  stored in Secrets Manager and referenced via `secretId` — identical to ADX.
- Entra scope: `https://api.loganalytics.io/.default` (`AzureSentinelDestination.SCOPE_LOG_ANALYTICS`).

Export is **blocked** in the same two places the ADX change had to open:

- `SearchExportDestinationResolver.isUnsupportedKqlEngine()` → `KUSTO_LAW`, `KUSTO_LAKE`
- `SearchServiceImpl.buildActualExportQuery()` →
  `case AZURE_SENTINEL -> throw "Export not supported for AZURE_SENTINEL external storage"`,
  plus `validateExportSupported()` → `isUnsupportedKqlEngine`.

### databahn-jobs (this repo)

Three engines after the ADX change:

| Engine | Interface | Shape |
| --- | --- | --- |
| `ATHENA` | `UnloadExecutor` | async `UNLOAD` → S3 staging → stream staged files out |
| `KUSTO_ADX` | `UnloadExecutor` | async `.export` → blob staging → stream staged blobs out |
| `SYNAPSE` | `RowStreamExecutor` / `CETASExecutor` | CETAS to blob staging, or pull rows over JDBC |

No Sentinel / Log Analytics code at all.

---

## 2. Design

```
backend-service                    databahn-jobs worker
─────────────────                  ──────────────────────────────────────────────
plans complete export KQL   ──→    POST api.loganalytics.io/v1/workspaces/{id}/query
(time filter + sort + take,          │   Prefer: wait=600
 take capped for Sentinel)           │
                                     ├─ stream-decode the JSON rows array
                                     ├─ rows → FormatEncoder (csv / json / xlsx)
                                     └─ encoded bytes → uploader.UploadPart (staged blocks)
                                                      → CommitBlockList
                                                      → GenerateBlobReadSASURL → download_link
```

The worker runs the planned KQL **verbatim**, exactly as it does for ADX. No time filter is
injected, no `take` is added, no query rewriting happens in the worker at all.

### Why no staging, unlike ADX

ADX export rests on `.export async`, a control command the cluster runs server-side, writing
straight to blob and returning a durable operation id. The Log Analytics query API
(`POST /v1/workspaces/{id}/query`) accepts queries only — there is no `.export`, no
`.show operations`, no server-side write target, and no operation id.

So rows come back in the HTTP response and the worker encodes and uploads them itself. That is
the `RowStreamExecutor` shape Synapse's JDBC path already uses — not the `UnloadExecutor`
shape ADX uses. Concretely this means Sentinel export has **no staging artifacts to clean up**
and **no resume** (§4).

### The limits, and the cap that keeps us inside them

Log Analytics `/query`, per request:

| Limit | Value |
| --- | --- |
| Rows returned | 500,000 |
| Response size | ~64 MB — surfaced by our own `LogAnalyticsKqlSupport` as a `PARTIAL_FAILURE` |
| Execution time | 10 minutes max (`Prefer: wait=600`), 3 min default |
| Request rate | ~200 requests / 30 s per Entra app |

The ADX change set `KqlQueryPlanner.MAX_EXPORT_TAKE_LIMIT = 1_000_000`, which is fine for
`.export` (it streams server-side, no result cap) and **too high for Log Analytics** — it is
double the row limit before response size is even considered.

Sentinel needs its own cap: `MAX_SENTINEL_EXPORT_TAKE_LIMIT`, proposed **200,000**.

Rows are not the binding constraint; **bytes are**. A typical Sentinel table is wide and full
of `dynamic` columns, so 64 MB arrives well before 500k rows — 200k rows leaves headroom at
~330 bytes/row, and a genuinely wide table can still exceed it. Which is why:

### Exceeding the limit must be a loud, actionable failure

Log Analytics returns **HTTP 200 with partial results** when a query exceeds its memory or
64 MB limit. Accepting that silently produces a truncated file that looks like a successful
export — the worst possible outcome for an audit-facing feature.

The Go transport must detect partial results and fail the report with the message the user can
act on: *"Result exceeded Log Analytics limits — narrow the time range or add filters to the
query."* `LogAnalyticsKqlSupport.requireSuccessfulResult` already does exactly this on the Java
side and is the wording to mirror.

This replaces what chunking would have done. With one query, the limit is a **product
boundary** communicated to the user, not an engineering problem the worker solves.

### No `getschema` round trip

The response carries its own column schema (`tables[0].columns`), so the columns come straight
from it. The ADX path needs a separate `| getschema` query only because Kusto's `.export`
writes headerless blobs. Nothing similar is needed here.

### Streaming decode, not `ReadAll`

A 64 MB JSON response unmarshalled whole into `[][]interface{}` costs several hundred MB of Go
heap — the interface boxing and per-value allocations multiply it well past the wire size.

Decode with `json.Decoder`: walk tokens to the `rows` array, then `Decode` one row at a time
into the encoder callback. Peak memory becomes one row plus the encode buffer, and the
existing `encodeRowsToUploader` already flushes that buffer to a staged block every
`MaxSegmentSizeMB`. This is also what makes the future chunking change cheap — the row-emitting
loop already looks like a stream.

---

## 3. Work — databahn-jobs

### New files

1. **`internal/searchExport/query/sentinel.go`** — `SentinelExecutor` implementing
   `query.RowStreamExecutor`:
   - Entra client-credential token, cached until a minute before expiry (same pattern as
     `ADXExecutor.accessToken`, scope `https://api.loganalytics.io/.default`).
   - `StreamRows` — drives `queryWindows()` (§5), today a single window covering the whole
     request, and emits rows through the callback.
   - `GetQueryColumns` — from the response schema; no extra query.
   - `ValidateExportQuery` — non-empty query and resolvable workspace config.
   - `CancelQueryExecution` — no-op; there is nothing server-side to cancel.
2. **`internal/searchExport/query/sentinel_transport.go`** — the LAW HTTP call:
   `POST https://api.loganalytics.io/v1/workspaces/{workspace_id}/query`, body
   `{"query": "..."}`, header `Prefer: wait=600`. Streaming row decode, partial-result
   detection, and 429 + `Retry-After` backoff (port `SentinelLakeKqlSupport.resolveRateLimitRetryDelayMillis`
   — header, then the `retry after … UTC` message body, then fixed fallbacks).
3. **`internal/searchExport/query/sentinel_values.go`** — LAW JSON value → `interface{}`:
   `dynamic` re-serialized as compact JSON, `datetime` normalized, nulls preserved.
4. **`internal/store/destination/sentinel_config.go`** — `SentinelConfig{WorkspaceID,
   StorageTier, TenantID, ClientID, ClientSecret}` from `connectorConfig`,
   `ApplySentinelCredentialOverrides`, `Validate()`. Direct analogue of `adx_config.go`;
   rejects `storage_tier = LAKE` with an explicit "not supported yet" error.
5. **`internal/searchExport/pipeline/sentinel_stream.go`** — `runSentinelStreamExport`:
   `uploader.Init`, drive `StreamRows` through `encodeRowsToUploader`, presign. The simple
   half of `runSynapseStreamExport` — no partition filters, no `db_edge_ts` cursor, no
   hour plan.

### Edits

| File | Change |
| --- | --- |
| `internal/searchExport/models/model.go` | `QueryEngineKustoLAW = "KUSTO_LAW"`; `SearchExportConfig` gains `StorageTier`, plus the forward-compat `KqlTable` / `KqlTimeColumn` fields (§5) |
| `internal/searchExport/query/executor.go` | `EngineSentinelLAW` const; `var _ RowStreamExecutor = (*SentinelExecutor)(nil)` |
| `internal/store/datastore/store.go` | `ExternalProviderSentinel = "AZURE_SENTINEL"`; `DeriveQueryEngine` → `KUSTO_LAW` for the `ANALYTICS` tier; `ExportDataStore.Sentinel`; `loadSentinelConfig` mirroring `loadADXConfig` |
| `internal/searchExport/factory/factory.go` | `case models.QueryEngineKustoLAW`: build `SentinelExecutor`; `IsSupportedExportMatrix` allows `KUSTO_LAW × {S3, S3_PARQUET, AZURE_BLOB}` |
| `internal/searchExport/pipeline/pipeline.go` | **Generalize the row-stream slot.** `Pipeline.synapse` and the `if p.synapse != nil` dispatch in `Run` are Synapse-specific; rename to `rowStream` and dispatch on `Engine()` — CETAS/JDBC for `SYNAPSE`, `runSentinelStreamExport` for `KUSTO_LAW`. `ResumeRun` already restarts for any row-stream engine, which is the wanted behaviour |
| `internal/searchExport/query/stream_options.go` | `SEARCH_EXPORT_SENTINEL_QUERY_TIMEOUT_SECONDS` (600), `SEARCH_EXPORT_SENTINEL_MAX_RETRIES` (3), `SEARCH_EXPORT_SENTINEL_MAX_ROWS` (defensive worker-side cap) |
| `internal/searchExport/main.go` | Nothing to clean up on permanent failure. The existing `stagingCleanup` closure already no-ops for a non-ADX, non-CETAS executor; assert it with a test |

No new vendored dependency — `azidentity` and `net/http` are already in the tree.

### Format mapping

Everything goes through the local encoder; there is no server-side format to pick.

| Export format | Path |
| --- | --- |
| `csv` (any delimiter, including multi-char) | `format.CSVEncoder`, header from the response schema |
| `json` | `format.JSONEncoder`, NDJSON |
| `xlsx` / `excel` | `format.ExcelEncoder` |

Strictly simpler than ADX, which falls back to Parquet for delimiters Kusto cannot emit.

---

## 4. Work — backend-service

Small, because the contract is the ADX one.

1. `SearchExportDestinationResolver` — `isUnsupportedKqlEngine` narrows to `KUSTO_LAKE` only;
   `allowedExportDestinationTypes` gains `KUSTO_LAW → SENTINEL_EXPORT_TYPES` (reuse
   `SYNAPSE_EXPORT_TYPES` — S3, S3_PARQUET and AZURE_BLOB all work, since the worker uploads
   client-side).
2. `SearchServiceImpl.validateExportSupported` — stop throwing for `KUSTO_LAW`.
3. `SearchServiceImpl.buildActualExportQuery` — `case AZURE_SENTINEL ->
   kqlSearchService.planSentinelExportQuery(dataSet, request.getQuery(), startTime, endTime, exportLimit)`.
4. `KqlQueryPlanner` — `MAX_SENTINEL_EXPORT_TAKE_LIMIT = 200_000` alongside
   `MAX_EXPORT_TAKE_LIMIT = 1_000_000`, and `buildSentinelExportQuery` passing it as the
   `takeCap`. `resolveEffectiveTakeLimit(limit, takeCap)` is already cap-parameterised by the
   ADX change, so this is a constant plus a two-line method.
5. `KqlSearchService.planSentinelExportQuery` — thin wrapper, mirroring `planExportQuery`.
6. `SearchServiceImpl.applySentinelExportConfig` (alongside `applyAdxExportConfig`) — set
   `externalSearchProvider`, `workspaceId`, `storageTier`, and the forward-compat
   `kqlTable` / `kqlTimeColumn` (§5).
7. `SearchServiceImpl.validateExportQuery` — require a KQL configuration for Sentinel, as the
   ADX branch already does.
8. Reject `storage_tier = LAKE` at request time with a clear message, rather than letting it
   reach the worker.

`request.isSelfContainedQuery()` routes through `buildSelfContainedExportQuery`, which the ADX
change already made KQL-aware — it needs the same Sentinel cap applied.

---

## 5. The extensibility seam

Chunking is not built, but the shape that admits it is. Three cheap decisions now that keep the
later change additive:

**1. A window planner with one window.** `SentinelExecutor.StreamRows` iterates
`queryWindows(cfg, startMs, endMs)`, which today returns a single window covering the whole
range and a query that is the planned KQL untouched. Adding chunking replaces that one
function with an adaptive planner (count each window, bisect when it exceeds a page cap); the
transport, decoding, encoding, and upload code is unchanged.

**2. Streaming row emission from day one** (§2). The pipeline already consumes rows through a
callback, so a second, third, and twentieth query feed the same encoder with no restructuring.

**3. Carry `kqlTable` and `kqlTimeColumn` in the export config now**, even though the worker
does not read them yet. They are one line each in `applySentinelExportConfig`.

That third point is the one that actually matters, because chunking needs a **different query
contract** and this is what softens it. A chunked worker must inject its own per-window
predicate, and it cannot do that to a fully-planned query: appending
`| where TimeGenerated in <window>` to a pipeline ending in `| sort by TimeGenerated desc |
take 200000` filters *after* the sort and take, so every window returns the same global top-N.
Prepending requires locating the table reference — which is what `kqlTable` and
`kqlTimeColumn` are for, and is a Go port of `KqlQueryPlanner.insertTimeFilter`.

So when chunking arrives, backend-service adds a second planner method that emits the
guardrailed pipeline *without* the trailing `sort`/`take` and sends the row budget as a number;
the worker already has the fields it needs to do the injection. Recording it here so the
decision is not rediscovered.

This is the Synapse precedent, not a new idea: Synapse chunks, so the worker applies its
partition filter itself via `query.AddPartitionFilter`.

---

## 6. Contract

### backend-service → databahn-jobs (`report_configuration.searchExportConfig`)

| Field | Value | Required |
| --- | --- | --- |
| `queryEngine` | `"KUSTO_LAW"` (exactly `QueryEngine.KUSTO_LAW.name()`) | yes |
| `destinationType` | `AZURE_BLOB` \| `S3` \| `S3_PARQUET` | yes |
| `query` | complete, planned KQL: table guardrail, time filter, sort, and `take` already applied. The worker runs it verbatim — same as ADX | yes |
| `dataStoreId` | `search_data_store` UUID; the worker reloads workspace credentials from it | yes |
| `destinationId` | global destination UUID | yes |
| `format` | `csv` \| `json` \| `xlsx` | yes |
| `delimiter` | any delimiter, including multi-character — the local encoder honours it | no |
| `includeHeader` | header comes from the response schema | no |
| `storageTier` | `ANALYTICS`; falls back to `connectorConfig.storage_tier`. `LAKE` is rejected | no |
| `startTime` / `endTime` | informational — the time filter is already in `query` | no |
| `kqlTable` / `kqlTimeColumn` | written now, unread by the worker; forward-compat for chunking (§5) | no |
| `queryExecutionId` | **never written** — Log Analytics has no server-side operation | n/a |

Data store shape the worker expects (`search_data_store`):

- `type = EXTERNAL_STORAGE` (or `DERIVED_DATASTORE`)
- `externalSearchProvider = AZURE_SENTINEL`, `connectorConfig.storage_tier = ANALYTICS`
- `connectorConfig`: `workspace_id`, `azure_tenant_id`, `azure_client_id`, and
  `azure_client_secret` — inline or, normally, resolved from `secretId` via Secrets Manager.

### Worker behaviour the backend can rely on

1. Claims a `REQUESTED`/`FAILED` report atomically. Writes no `queryExecutionId`.
2. A stale `PROCESSING` report **restarts from scratch** — same as Synapse. Up to
   `MaxRetries = 3` attempts. A single query is short enough that this is cheap.
3. On success: `status = COMPLETED`, `download_link` = SAS URL of a single file at
   `exports/<reportId>/SearchExport_<table>_<datetime>.<ext>`, `download_link_expiry` set.
4. No staged artifacts exist, so nothing is left behind on failure beyond an aborted (deleted)
   destination blob.
5. A result exceeding the Log Analytics limits fails the report with an actionable message; it
   is never silently truncated.
6. Azure requirements: the service principal needs **Log Analytics Reader** on the workspace,
   and `api.loganalytics.io` must be reachable from the jobs pod.

---

## 7. Risks / verify during implementation

- **Partial results are the top risk.** HTTP 200 with truncated data. Must be detected and
  failed, never accepted. Verify against a query that genuinely exceeds 64 MB — this is the one
  test most worth doing on a real workspace.
- **Is 200,000 the right cap?** It is a starting point, chosen for headroom, not measured.
  Verify against the widest real Sentinel table available (`SecurityEvent`, `CommonSecurityLog`)
  and tune. Wrong in the safe direction costs users rows; wrong in the unsafe direction costs
  them a failed export, which is recoverable and honest.
- **10-minute query ceiling.** A 200k-row scan over a wide table across a long range can exceed
  it. `Prefer: wait=600` is the maximum; there is no higher setting. Surface the timeout with
  the same "narrow the range" guidance.
- **Rate limiting.** ~200 requests / 30 s is **per Entra app**, shared with interactive search
  across every tenant using the same credentials. One query per export makes this unlikely, but
  429 handling still belongs in from day one.
- **`dynamic` columns.** Sentinel tables are full of them. Confirm CSV emits compact JSON (not
  Go's `%v` rendering of `map[string]interface{}`) and NDJSON emits nested values, not
  double-encoded strings.
- **Empty exports.** A range with no rows must complete with 0 rows and a valid header-only
  file, not fail. `encodeRowsToUploader` already handles this; test it.
- **Aggregation pipelines** (`summarize`, `count`, `distinct`) work unchanged here — one query
  is one query. Worth an explicit test, because they are the case that *would* have broken under
  chunking, and the test documents why.

---

## 8. Sequencing

1. **databahn-jobs** — executor, transport, config, pipeline wiring, unit tests. Land first so
   the worker understands `KUSTO_LAW` before any report can carry it.
2. **backend-service** — cap, planner method, resolver and validation unblocking,
   `applySentinelExportConfig`. Opens the gate.
3. **Workspace verification** — real csv / json / xlsx exports; an oversized query to prove the
   partial-result failure; an empty range; a `summarize` pipeline.

**Deferred, tracked:**
- Chunking for large ranges (§5).
- `LAKE` tier — second transport, Kusto v2 frame parser, `workspace_name` (`db` is
  `workspaceName-workspaceId`, not the GUID), lake-specific backoff.
- ADX-proxy fast path: tenants with both an ADX cluster and Sentinel could query LAW via
  `cluster('https://ade.loganalytics.io/...')` and let `.export` do the work server-side,
  reusing the ADX path with no limits at all. Needs a linked ADX store and cross-service
  permissions.

**Branching**: touches `factory.go`, `models/model.go`, `datastore/store.go`, and
`pipeline/pipeline.go` — the same files as the ADX change. Branch off `search/adx-export`
rather than resolving the same conflicts twice.

Estimated size: ~450–600 LOC in databahn-jobs (incl. tests), ~80 in backend-service.

---

## 9. Test plan

- `sentinel_transport_test.go` — `httptest`-backed (the `adx_test.go` pattern): token
  acquisition and caching, `Prefer: wait=600`, streaming row decode, partial-result → error
  with the expected message, 429 + `Retry-After` backoff, HTTP error surfacing.
- `sentinel_values_test.go` — `dynamic` / `datetime` / null / numeric rendering for both CSV and
  NDJSON.
- `sentinel_test.go` — `StreamRows` emits every row once, in order; columns come from the
  response schema; empty result set; defensive row cap.
- `sentinel_config_test.go` — connector parsing, secret override, tier defaulting to
  `ANALYTICS`, `LAKE` rejected, missing-field errors.
- `store_test.go` — `DeriveQueryEngine` for `AZURE_SENTINEL`.
- `factory_test.go` — `IsSupportedExportMatrix` for `KUSTO_LAW` across destination types.
- `pipeline_test.go` — engine dispatch after the `synapse` → `rowStream` rename: `SYNAPSE` still
  reaches CETAS and JDBC paths, `KUSTO_LAW` reaches the Sentinel path, `ResumeRun` restarts for
  both, `stagingCleanup` no-ops.
- backend: `KqlQueryPlannerTest` for the Sentinel cap, `SearchExportDestinationResolverTest` for
  `KUSTO_LAW`, and a `SearchServiceImplSentinelExportTest` mirroring the ADX one.
- Manual: csv / json / xlsx against a live workspace; a query that exceeds 64 MB; an empty
  range; a `summarize` pipeline; pod kill mid-export (must restart cleanly, no orphaned blob).

> **Note on backend tests:** always pass `-am` to Maven
> (`./mvnw -o -pl search/web -am test`). Without it Maven resolves a stale `common-1.0.0.jar`
> from `~/.m2` and the export tests fail with `NoSuchMethodError` — an environment artifact,
> not a code problem. Learned during the ADX change.

---

## 10. What landed

### databahn-jobs

New files:

- `internal/searchExport/query/sentinel.go` — `SentinelExecutor`, a `query.RowStreamExecutor`
  over the Sentinel query API. Selects a transport by storage tier, resolves columns from the
  response schema, enforces a defensive row budget, and guards against an inconsistent column
  set across plans (unreachable until chunking lands, but it makes the requirement visible).
- `internal/searchExport/query/sentinel_transport.go` — the `sentinelTransport` seam plus its
  Log Analytics implementation: Entra token cached until a minute before expiry, scope derived
  from the endpoint, `Prefer: wait=600`, and 429/503/504 retry with `Retry-After` (header,
  then the ladder the backend uses when the header is absent).
- `internal/searchExport/query/sentinel_response.go` — streaming decoder, partial-result
  detection, and value conversion.
- `internal/searchExport/query/sentinel_plan.go` — `PlanSentinelQueries`, the chunking seam.
- `internal/store/destination/sentinel_config.go` — `SentinelConfig` from `connectorConfig`
  plus Secrets Manager overrides and validation, mirroring `adx_config.go`.
- `internal/searchExport/pipeline/sentinel_stream.go` — `runSentinelStreamExport`.
- Tests: `sentinel_test.go`, `sentinel_response_test.go`, `sentinel_config_test.go`,
  `pipeline/sentinel_stream_test.go`, `format/encoder_test.go`.

Edited:

- `models/model.go` — `QueryEngineKustoLAW`, plus `StorageTier`, `KqlTable`, `KqlTimeColumn`.
- `datastore/store.go` — `ExternalProviderSentinel`, `AZURE_SENTINEL` → `KUSTO_LAW`,
  `ExportDataStore.Sentinel`, `loadSentinelConfig`.
- `factory/factory.go` — `ExportDeps.Synapse` renamed to `ExportDeps.RowStream`; Sentinel
  engine case; `IsSupportedExportMatrix` allows `KUSTO_LAW × {S3, S3_PARQUET, AZURE_BLOB}`.
- `pipeline/pipeline.go` — `Pipeline.synapse` renamed to `rowStream`, and `Run` now dispatches
  row-stream engines on `Engine()` rather than assuming Synapse.
- `query/executor.go` — `EngineSentinelLAW` and the interface assertion.
- `query/stream_options.go` — `SentinelStreamOptionsFromEnv`, `SentinelMaxRetriesFromEnv`.
- `main.go` — `deps.Synapse` renamed; the permanent-failure cleanup already no-ops for an
  executor with no staging.

**A bug the tests caught, in shared code.** `format.FormatValue` matched `[]byte` but a Go
type switch compares exact types, so `json.RawMessage` — how a KQL `dynamic` column travels —
fell through to the default and rendered as a list of byte values. Both `FormatValue` and the
Excel encoder now handle `json.RawMessage` and `json.Number`. Athena and Synapse never produce
those types, so the change is inert for them, and `format/encoder_test.go` pins the behaviour.

Verification run: `go build ./...`, `go vet ./internal/searchExport/... ./internal/store/...`,
`go test ./internal/...` — all pass.

### backend-service

- `KqlQueryPlanner` — `MAX_SENTINEL_EXPORT_TAKE_LIMIT = 200_000` alongside the ADX
  `MAX_EXPORT_TAKE_LIMIT = 1_000_000`, plus cap-parameterised `buildExportQuery` /
  `buildSelfContainedExportQuery` overloads.
- `KqlSearchService` — `exportTakeCap(kql)` picks the cap from the resolved engine, so
  `planExportQuery` and `planSelfContainedExportQuery` cap Sentinel and ADX correctly with no
  new call sites.
- `SentinelExportTier` (new) — resolves an external store's export engine, splitting Sentinel
  by `storage_tier`. `ExternalDatasetFormatRegistry` maps `AZURE_SENTINEL` to `KUSTO_LAW` for
  both tiers, which is right for search but wrong for export; without the split a lake store
  would pass validation and fail late in the job.
- `SearchExportDestinationResolver` — `isUnsupportedKqlEngine` narrowed to `KUSTO_LAKE`;
  `SENTINEL_EXPORT_TYPES` covers S3, S3_PARQUET, and AZURE_BLOB.
- `SearchServiceImpl` — `buildActualExportQuery` plans `AZURE_SENTINEL` through
  `kqlSearchService.planExportQuery`; `validateExportSupported` allows `KUSTO_LAW`;
  `applySentinelExportConfig` sets the provider, tier, and the forward-compat KQL fields;
  `validateExportQuery` requires a KQL configuration for Sentinel.
- `ReportConfiguration.SearchExportConfig` — `storageTier`, `kqlTable`, `kqlTimeColumn`.
- Tests: `SearchServiceImplSentinelExportTest` (new, 7 tests over the config contract),
  `KqlSearchServiceExportCapTest` (new, 5 tests over the per-engine cap), and Sentinel cases in
  `SearchExportDestinationResolverTest`.

Verification run: `./mvnw -o -pl search/web -am test` — 1029 tests, 0 failures, 0 errors
(1 pre-existing skip).

Note: **always pass `-am`**. Without it Maven resolves a stale `common-1.0.0.jar` from `~/.m2`
that predates the new config fields, and the export tests fail with `NoSuchMethodError` — an
environment artifact, not a code problem. Learned during the ADX change.

