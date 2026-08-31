# Search Export for Azure Data Explorer (ADX)

Plan for adding ADX as a search-export **source** in `databahn-jobs`, leveraging the
native `.export` control command. The Security Lake work on
`search/security-lake-export` is the reference for how a new source gets wired; it is
not otherwise related to this change.

Status: **implemented on both sides** (2026-08-20), uncommitted in both working trees —
databahn-jobs on `search/security-lake-export` (on top of `56d73a2d "Export for security
lake"`) and backend-service on `export-security-lake` (on top of `b0e87be26 "Export
support for security lake source"`). Section 11 records what was verified before
hand-off; not yet run against a real ADX cluster.

---

## 1. What already exists

### backend-service (`~/Desktop/Workspace/backend-service`)

ADX is already a fully supported **search** source:

| Concern | Where |
| --- | --- |
| KQL execution + AAD client build | `search/.../search/kql/AdxKqlSupport.java`, `AdxKqlExecutor.java` |
| Credential validation | `search/.../search/auth/impl/AdxAuthProviderImpl.java` |
| Data store CRUD | `search/.../search/datastore/impl/AdxExternalDataStoreServiceImpl.java` |
| Query planning (time filter + `take`) | `search/.../search/kql/KqlQueryPlanner.java` |
| Provider → engine mapping | `search/.../search/helper/ExternalDatasetFormatRegistry.java` |
| Config keys | `common/.../constants/AzureConstants.java` |

Shape of an ADX data store:

- `search_data_store.type = EXTERNAL_STORAGE`
- `configuration.externalSearchDataStoreConfiguration.externalSearchProvider = AZURE_DATA_EXPLORER`
- `connectorConfig`:
  - `adx_cluster_uri` (required)
  - `adx_database` (required)
  - `azure_tenant_id`, `azure_client_id`, `azure_client_secret`
- `azure_client_secret` is the only confidential attribute (`ADX_CONFIDENTIAL_ATTRIBUTES`),
  so it is stored in Secrets Manager and referenced via `secretId`.
- Query engine: `QueryEngine.KUSTO_ADX` (the enum **name** is what lands in
  `report_configuration.searchExportConfig.queryEngine`).

Export is currently **blocked** for ADX in two places:

- `SearchExportDestinationResolver.isKqlEngine()` → `"Export not supported for this data store"`
- `SearchServiceImpl.buildActualExportQuery()` → `case AZURE_SENTINEL, AZURE_DATA_EXPLORER -> throw`
  (and `validateExportSupported()` throws for any KQL engine)

### databahn-jobs (this repo)

No ADX/Kusto code at all — the only mention is a negative assertion in
`internal/searchExport/factory/factory_test.go:26`.

Two engines exist today:

- **ATHENA** — async `UNLOAD` → S3 staging → stream staged files to the export
  destination. Async execution id makes stale jobs resumable (`Pipeline.ResumeRun`).
- **SYNAPSE** — CETAS → Azure Blob staging → stream staged blobs to the destination.
  No execution id, so a stale job always restarts.

---

## 2. Design

`.export async` maps onto the **Athena** shape, not the Synapse one:

```kql
.export async to csv (
    h@"https://<account>.blob.core.windows.net/<container>;<write SAS>"
)
with (
    namePrefix="databahn_export_<shortReportId>",
    includeHeaders=none,
    sizeLimit=1073741824
)
<| set notruncation; <KQL planned by backend-service>
```

1. The command returns an **OperationId**.
2. `.show operations <id>` reports `InProgress` / `Completed` / `Failed` / `Throttled`.
3. `.show operation <id> details` lists the blobs written (`Path`, `NumRecords`, `SizeInBytes`).

Because the operation id is durable and pollable, ADX inherits the **Athena resume
story**: a stale `PROCESSING` report reattaches to a running export instead of
re-running the query. That is strictly better than the Synapse CETAS path.

Therefore: **`ADXExecutor` implements `query.UnloadExecutor` verbatim**, and the
existing athena branch of `Pipeline.Run` / `Pipeline.ResumeRun` drives it with no
structural change. Staging reads use the existing `unload.BlobReader`; per-file
cleanup after a successful upload is already handled by `runUploadPhase`
(`internal/searchExport/pipeline/pipeline.go`).

### Interface mapping

| `query.UnloadExecutor` | ADX implementation |
| --- | --- |
| `ExecuteUnloadAsync` | `.export async …` → OperationId |
| `CheckQueryStatus` | `.show operations <id>` → `QueryStateRunning/Succeeded/Failed` |
| `WaitForExecution` | poll `.show operations` with backoff |
| `GetExecutionResult` | `.show operation <id> details` → staging prefix / file list |
| `GetQueryColumns` | `<query> \| getschema` (CSV header) |
| `NewStagingReader` | `unload.NewBlobReader(blobClient, container, tempDir)` |
| `CancelQueryExecution` | `.cancel operation <id>` |
| `GetOutputLocation` | staging container prefix |
| `GetAWSConfig` | `nil` |

### Transport

Raw REST (`POST <cluster>/v1/rest/mgmt`, bearer token from
`azidentity.NewClientSecretCredential`) instead of adding `azure-kusto-go` to
`vendor/`:

- same auth model as the Java side (AAD app client credentials);
- we never stream rows out of ADX — all data goes to blob — so only four control
  commands plus one `getschema` query are needed;
- `azidentity` and `net/http` are already available; no vendor churn.

The choice is isolated behind `query.UnloadExecutor`, so swapping in the official SDK
later is a contained change.

### Scope for v1

- Source: ADX only (`KUSTO_ADX`).
- Export destination: **`AZURE_BLOB` only** (the global destination blob doubles as
  `.export` staging). No ADX → S3.
- Formats: `csv`, `json`, `xlsx` via the existing pipeline paths.
- No `compressed` export (`BlobReader` streams raw bytes and would need gunzip).

---

## 3. Work — databahn-jobs

### New files

1. **`internal/searchExport/query/adx.go`** — `ADXExecutor` implementing
   `query.UnloadExecutor` (REST client, token acquisition, polling, staging reader).
2. **`internal/searchExport/query/adx_commands.go`** — pure command builders and
   response parsers (`.export`, `.show operations`, `.show operation … details`,
   `getschema`), mirroring how `synapse_cetas.go` isolates DDL strings so everything
   is unit-testable without a cluster.
3. **`internal/store/destination/adx_config.go`** — `ADXConfig{ClusterURI, Database,
   TenantID, ClientID, ClientSecret}`, parsed from `connectorConfig` with
   `ResolveCredentialOverrides` applied for `azure_client_secret` (mirrors
   `loadExternalAthenaStaging` in `internal/store/datastore/store.go`).

### Edits

| File | Change |
| --- | --- |
| `internal/searchExport/models/model.go` | add `QueryEngineKustoADX = "KUSTO_ADX"` (must match the Java enum name) |
| `internal/store/datastore/store.go` | `DeriveQueryEngine`: `EXTERNAL_STORAGE`/`DERIVED_DATASTORE` + `AZURE_DATA_EXPLORER` → `KUSTO_ADX`; add `ExportDataStore.ADX`; load ADX connector config + secret |
| `internal/searchExport/factory/factory.go` | new engine case: build `ADXExecutor`, require `destType == AZURE_BLOB`, reuse the export destination's `AzureBlobConfig` as `.export` staging; `IsSupportedExportMatrix` gains `KUSTO_ADX × AZURE_BLOB` only |
| `internal/store/destination/azure_sas.go` | add a **container-scoped write SAS**: shared-key path exists as `generateWriteSAS` (currently in `query/synapse_cetas.go`); the `AUTH_SERVICE_PRINCIPAL` path needs a user-delegation SAS — the `GetUserDelegationCredential` machinery is already there but is blob-scoped and read-only. ADX storage connection strings accept a SAS or account key, **not** an SP client secret |
| `internal/searchExport/pipeline/pipeline.go` | make `unloadOptions()` / `useDirectUpload()` engine-aware; rename the `athena` field to something engine-neutral |
| `internal/searchExport/main.go` | permanent-failure cleanup for ADX: `.cancel operation` + delete staged blobs by prefix |

### Format mapping

| Export format | ADX `.export to` | Upload path |
| --- | --- | --- |
| `csv`, delimiter `,` | `csv` | direct stream + `getschema` header |
| `csv`, delimiter `\t` | `tsv` | direct stream + header |
| `csv`, other/multi-char delimiter | `parquet` | local decode + encoder (honours delimiter) |
| `json` | `json` (line-delimited, matches our NDJSON content type) | direct stream |
| `xlsx` / `excel` | `parquet` | local decode + Excel encoder |

---

## 4. Work — backend-service (done; see section 8b for what landed)

Without these, no ADX export request is ever queued.

1. `SearchExportDestinationResolver` — allow `KUSTO_ADX` with `AZURE_BLOB`
   destinations (reuse `SYNAPSE_EXPORT_TYPES`); keep `KUSTO_LAW` / `KUSTO_LAKE` blocked.
2. `SearchServiceImpl.buildActualExportQuery` — `case AZURE_DATA_EXPLORER ->
   kqlSearchService.planQuery(dataSet, request.getQuery(), startTime, endTime, exportLimit)`,
   exactly as `SPLUNK` does; keep `AZURE_SENTINEL` blocked.
3. `SearchServiceImpl.validateExportSupported` — stop throwing for `KUSTO_ADX`.
4. `SearchServiceImpl.createExportRequest` — set `config.setDatabase(adx_database)` for
   the ADX branch (today the non-Synapse branch assumes an Athena database).
5. **`KqlQueryPlanner.MAX_TAKE_LIMIT = 10_000`** clamps the injected `take`. Export
   needs the `MAX_EXPORT_ROWS = 1_000_000` path, otherwise every ADX export silently
   truncates at 10k rows. Needs an export-aware limit.

Unlike Synapse (where databahn-jobs applies the time-partition filter), the KQL that
reaches the worker is already complete — time filter, sort, and `take` included — so
the worker runs it verbatim.

---

## 5. Risks / verify during implementation

- **Truncation** — the default 500k-row query result limit applies to `.export`;
  `set notruncation;` (or `set truncationmaxrecords=…`) must prefix the inner query or
  a 1M-row export fails.
- **Headers** — `includeHeaders=firstFile` is unreliable for us: ADX names blobs
  `<prefix>_<n>_<guid>.csv` and lexical sort is not `_1_,_2_,…,_10_`. Use
  `includeHeaders=none` and build the header from `getschema`, like the CETAS path.
- **ADX permissions** — the service principal must be allowed to run `.export` control
  commands on the database, not just query it.
- **Parquet decode** — confirm `xitongsys/parquet-go` reads ADX-written parquet for the
  `xlsx` path.
- **SAS lifetime** — the staging SAS must outlive a long `.export` (CETAS uses 24h;
  match or exceed).
- **Blob prefix isolation** — `namePrefix` per report keeps concurrent exports from
  colliding in a shared container, and makes prefix-based cleanup safe.
- **Known limitation (pre-existing, shared with Athena)** — `runUploadPhase` deletes the
  staged files when the *upload* fails. A later resume then reattaches to the already
  `Succeeded` operation, finds no staged blobs, and completes the report with zero rows
  instead of re-exporting. ADX can fix this properly because `.show operation <id>
  details` reports the paths it wrote: if details lists files but the container has none,
  the export must be re-run rather than reported empty. Not done here — it changes retry
  semantics shared with Athena, so it wants its own change.

---

## 6. Sequencing

1. Land databahn-jobs first (worker understands `KUSTO_ADX` before any report can carry it).
2. Then backend-service, which opens the gate.

**Branching**: this touches `factory.go`, `models/model.go`, and
`datastore/store.go` — the same files as the in-flight Security Lake change. Branch off
`search/security-lake-export` and rebase onto `master` after it merges rather than
resolving the same conflicts twice.

Estimated size: ~700–900 LOC in databahn-jobs (incl. tests), ~60 in backend-service.

---

## 7. Test plan

- `adx_commands_test.go` — command text for each format/delimiter combination,
  escaping, `namePrefix` derivation, operation-state mapping, `.show operation details`
  parsing (table-driven, no cluster needed).
- `adx_config_test.go` — connector parsing, secret override, missing-field errors.
- `store_test.go` — `DeriveQueryEngine` for `AZURE_DATA_EXPLORER`.
- `factory_test.go` — `IsSupportedExportMatrix`: `KUSTO_ADX × AZURE_BLOB` true,
  `KUSTO_ADX × S3` false.
- `pipeline_test.go` — engine-aware `unloadOptions` / `useDirectUpload`.
- Manual: csv / json / xlsx export against a real cluster, plus a stale-`PROCESSING`
  resume (kill the pod mid-export and confirm reattachment to the operation id).

---

## 8. What landed in databahn-jobs

New files:

- `internal/searchExport/query/adx.go` — `ADXExecutor`, a `query.UnloadExecutor` over the
  Kusto v1 REST API (`/v1/rest/mgmt`, `/v1/rest/query`) authenticated with an
  `azidentity.ClientSecretCredential` token cached until a minute before expiry.
- `internal/searchExport/query/adx_commands.go` — command builders (`.export async`,
  `.show operations`, `.show operation … details`, `.cancel operation`, `| getschema`),
  state mapping, and v1 response parsing.
- `internal/store/destination/adx_config.go` — `ADXConfig` from `connectorConfig` +
  Secrets Manager overrides + validation.
- `internal/store/destination/azure_staging_sas.go` — `GenerateContainerWriteSAS`
  (account-key, SAS-connection-string, and user-delegation paths) and
  `BlobStagingSAS.KustoConnectionString()`.
- Tests: `adx_commands_test.go`, `adx_test.go` (httptest-backed REST layer),
  `adx_config_test.go`, `azure_staging_sas_test.go`, `pipeline/adx_stream_test.go`
  (format selection and staging cleanup).

Edited:

- `models/model.go` — `QueryEngineKustoADX = "KUSTO_ADX"`.
- `datastore/store.go` — `AZURE_DATA_EXPLORER` → `KUSTO_ADX`, `ExportDataStore.ADX`,
  `loadADXConfig`.
- `factory/factory.go` — `ExportDeps.Athena` renamed to `ExportDeps.Unload`; ADX engine
  case; `IsSupportedExportMatrix` restructured so `KUSTO_ADX` allows `AZURE_BLOB` only.
- `pipeline/pipeline.go` — `Pipeline.athena` renamed to `unloadExec`; `adxUnloadOptions`;
  `useDirectUpload` extended to `csv`/`tsv`.
- `pipeline/adx_stream.go` — `CleanupADXStaging` (cancel the operation + delete staged
  blobs) for the final-retry path, alongside the existing `CleanupCETASArtifacts`.
- `main.go` — tracks the operation id this attempt started (the report row only carries
  one for a resumed job) and hands it to `CleanupADXStaging` on permanent failure.
- `unload/blob_reader.go` — list log message no longer says "CETAS".

Verification run: `go build ./...`, `go vet ./internal/searchExport/... ./internal/store/...`,
`go test ./internal/searchExport/... ./internal/store/...` — all pass. No cluster test yet.

## 8b. What landed in backend-service

- `KqlQueryPlanner` — `MAX_EXPORT_TAKE_LIMIT = 1_000_000` alongside the interactive
  `MAX_TAKE_LIMIT = 10_000`, plus `buildExportQuery` / `buildSelfContainedExportQuery`
  and a cap-parameterised `resolveEffectiveTakeLimit`. Interactive search is untouched.
- `KqlSearchService` — `planExportQuery` and `planSelfContainedExportQuery`.
- `SearchExportDestinationResolver` — `isKqlEngine` narrowed to `isUnsupportedKqlEngine`
  (LAW/LAKE only); `KUSTO_ADX` resolves against `AZURE_BLOB` global destinations.
- `SearchServiceImpl` —
  `validateExportSupported` allows ADX but rejects any non-`AZURE_BLOB` destination;
  `buildActualExportQuery` plans ADX through `kqlSearchService.planExportQuery`
  (`AZURE_SENTINEL` still throws); `buildSelfContainedExportQuery` takes the dataset and
  routes KQL datasets to the KQL planner instead of the SQL limit builders;
  `validateExportQuery` requires a KQL configuration for ADX; `applyAdxExportConfig`
  sets `externalSearchProvider` and the ADX database from the store's non-secret
  `connectorConfig`; `resolveExportTableName` now falls back to the KQL table so export
  filenames are meaningful.
- Tests: `SearchServiceImplAdxExportTest` (new, 5 tests over the config contract),
  plus ADX cases in `SearchExportDestinationResolverTest` and `KqlQueryPlannerTest`.

Verification run: `./mvnw -o -pl search/web -am test` — 1003 tests, 0 failures, 0 errors
(1 pre-existing skip). Note: **always pass `-am`**. Without it Maven resolves a stale
`common-1.0.0.jar` from `~/.m2` that predates the Security Lake fields, and the export
tests fail with `NoSuchMethodError` on `setExternalSearchProvider` — an environment
artifact, not a code problem.

---

## 9. Contract

### backend-service → databahn-jobs (audit_report row)

`report_configuration.searchExportConfig` for an ADX export:

| Field | Value | Required |
| --- | --- | --- |
| `queryEngine` | `"KUSTO_ADX"` (exactly `QueryEngine.KUSTO_ADX.name()`) | yes |
| `destinationType` | `"AZURE_BLOB"` — the only supported ADX export destination | yes |
| `query` | complete, planned KQL: table guardrail, time filter, sort, and `take` already applied. The worker runs it verbatim and never injects a time filter (unlike Synapse) | yes |
| `database` | ADX database; falls back to `connectorConfig.adx_database` when blank | no |
| `dataStoreId` | `search_data_store` UUID; the worker reloads cluster credentials from it | yes |
| `destinationId` | `AZURE_BLOB` destination UUID; doubles as `.export` staging | yes |
| `format` | `csv` \| `json` \| `xlsx` | yes |
| `delimiter` | `,` and `\t` export natively; anything else routes through Parquet | no |
| `includeHeader` | header is built from `\| getschema`, never from Kusto | no |
| `startTime` / `endTime` | informational for ADX — the time filter is already in `query` | no |
| `queryExecutionId` | **written by the worker**, not the backend: the Kusto OperationId | no |
| `executionStartedAt` | written by the worker | no |

Data store shape the worker expects (`search_data_store`):

- `type = EXTERNAL_STORAGE` (or `DERIVED_DATASTORE`)
- `configuration.externalSearchDataStoreConfiguration.externalSearchProvider = AZURE_DATA_EXPLORER`
- `connectorConfig`: `adx_cluster_uri`, `adx_database`, `azure_tenant_id`,
  `azure_client_id`, and `azure_client_secret` — the secret either inline or, normally,
  resolved from `secretId` via Secrets Manager (key `azure_client_secret`).

Break any of these and the worker fails the report with an explicit message rather than
guessing.

### Worker behaviour the backend can rely on

1. Claims a `REQUESTED`/`FAILED` report atomically; writes the Kusto OperationId to
   `queryExecutionId` as soon as `.export async` returns.
2. A stale `PROCESSING` report reattaches to that OperationId — running or already
   completed — instead of re-running the query. Up to `MaxRetries = 3` attempts.
3. On success: `status = COMPLETED`, `download_link` = presigned/SAS URL of a single
   concatenated file at `exports/<reportId>/SearchExport_<table>_<datetime>.<ext>`,
   `download_link_expiry` set.
4. Staged `.export` blobs (`databahn_export_<first 12 hex of reportId>_*`) are deleted
   after a successful upload, and after the final failed retry (which also cancels a
   still-running operation). They are deliberately left in place between retries.
5. Kusto requirements the cluster/service principal must satisfy: permission to run
   `.export` control commands on the database, and network reachability of
   `<adx_cluster_uri>` from the jobs pod.

### Internal contract (databahn-jobs)

`ADXExecutor` implements `query.UnloadExecutor` in full — that is the seam. Anything the
pipeline needs from an engine goes through that interface; swapping the REST transport
for `azure-kusto-go` later touches `adx.go` only.

---

## 10. Handover prompt

> Continue the ADX search-export work.
>
> **Done:** the databahn-jobs side is implemented and unit-tested in the working tree of
> `search/security-lake-export` (see section 8 for the file list; `go build ./...`,
> `go vet`, and `go test ./internal/searchExport/... ./internal/store/...` all pass).
> It is **uncommitted** — decide whether it belongs on its own branch off
> `56d73a2d "Export for security lake"` before committing.
>
> **Not done, in priority order:**
> 1. **backend-service** (`~/Desktop/Workspace/backend-service`) — section 4 of this doc:
>    unblock `KUSTO_ADX` in `SearchExportDestinationResolver.isKqlEngine` /
>    `allowedExportDestinationTypes` (Azure Blob only), add the
>    `case AZURE_DATA_EXPLORER -> kqlSearchService.planQuery(...)` branch in
>    `SearchServiceImpl.buildActualExportQuery`, stop throwing in
>    `validateExportSupported`, set `config.setDatabase(adx_database)`, and give
>    `KqlQueryPlanner` an export-aware take limit — `MAX_TAKE_LIMIT = 10_000` currently
>    caps every ADX export at 10k rows.
> 2. **Cluster verification** — run a real csv, json, and xlsx export end to end and
>    confirm: `set notruncation;` lets a >500k-row export through; the `| getschema`
>    header matches the exported column order; `xitongsys/parquet-go` reads
>    ADX-written Parquet; the staging SAS outlives a long export; the service principal
>    is allowed to run `.export`.
> 2. **Resume test** — kill the worker mid-export and confirm the next poll reattaches to
>    the OperationId in `queryExecutionId` instead of re-running the query.
> 3. **Known limitation** in section 5 (staged blobs deleted on upload failure → a later
>    resume reports zero rows). Fix using `.show operation <id> details`, which reports
>    the paths the export wrote.
>
> Constraints to preserve: ADX exports only to `AZURE_BLOB`; the KQL arrives fully
> planned from backend-service and must be run verbatim; `namePrefix` must stay derived
> from the report id (stable across retries) or resume breaks; Kusto storage connection
> strings take a SAS or account key, never a service principal secret. The full contract
> is section 9.

---

---

## 11. What was verified before hand-off

Unit tests (both repos) are in sections 8 and 8b. Beyond those, the export path was
exercised once against a local Azurite emulator with a throwaway harness — a stub cluster
that answered the control commands and wrote the exported blobs itself using only the SAS
from the `.export` command, driving the real executor, `BlobReader`, `AzureUploader`, and
`Pipeline`. That harness was scratch work and is deliberately **not** in the repo; what it
established is recorded here:

- CSV end to end: header from `| getschema`, two staged chunks concatenated in blob-name
  order, exact file contents correct.
- JSON: one NDJSON record per row.
- The emitted `.export` carries `set notruncation;` and `includeHeaders=none`.
- Staged blobs are deleted after a successful upload.
- A resume against a recorded operation id reattaches and uploads without re-exporting.
- `datastore.LoadExportDataStore` and `factory.NewExportDeps` resolve `KUSTO_ADX`, load the
  ADX config, and reject an S3 destination — run against a real control-plane Postgres
  schema, with an inline client secret so Secrets Manager stayed out of it.

Two things learned that outlive the harness:

- **The staging SAS is HTTPS-only.** `GenerateContainerWriteSAS` signs with
  `Protocol: ProtocolHTTPS`, and storage enforces it — a plain-HTTP write fails with
  `AuthorizationProtocolMismatch`. Anything that talks to that SAS must use HTTPS.
- **`GenerateContainerWriteSAS` builds `https://<account>.blob.core.windows.net/...`**,
  matching `azure_sas.go` and `synapse_cetas.go` but ignoring a connection string's
  `BlobEndpoint`. Correct for Azure public cloud; it would need revisiting for sovereign
  clouds or an emulator.

Still unverified, and still item 1 of the handover: Kusto's own behaviour — whether the
cluster accepts our exact property list, `set notruncation;` on a >500k-row query, and the
service principal's permission to run `.export` — plus Entra token acquisition and
pod-to-cluster networking. Those need a real cluster.
