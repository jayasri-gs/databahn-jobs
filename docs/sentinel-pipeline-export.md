# Sentinel & Sentinel Lake export for pipeline (`DATABAHN_DESTINATION`) stores

Search export from Microsoft Sentinel already worked when Sentinel was configured as an
**external** search data store (`type = EXTERNAL_STORAGE` / `DERIVED_DATASTORE`, provider
`AZURE_SENTINEL`). It did **not** work when the same workspace was reached as a **pipeline**
store — a `search_data_store` of `type = DATABAHN_DESTINATION` linked to an `AZURE_SENTINEL`
or `AZURE_SENTINEL_DATA_LAKE` destination. Interactive search on those stores worked;
export returned `Export not supported for this data store`.

This change closes that gap. Nothing about the export *mechanism* changes — the same
`RowStreamExecutor` runs the same KQL against the same two transports. Only the resolution of
"which engine is this store, and where do its workspace credentials live" was missing.

## The two store shapes

|  | External Sentinel store | Pipeline Sentinel store |
| --- | --- | --- |
| `search_data_store.type` | `EXTERNAL_STORAGE` / `DERIVED_DATASTORE` | `DATABAHN_DESTINATION` |
| Provider signal | `externalSearchProvider = AZURE_SENTINEL` | linked `destination.destination_type` |
| Tier signal | `connectorConfig.storage_tier` (`ANALYTICS` \| `LAKE`, blank = analytics) | destination type: `AZURE_SENTINEL` → analytics, `AZURE_SENTINEL_DATA_LAKE` → lake |
| Workspace + credentials | store `connectorConfig` + store secret | destination configuration + destination secret |

The tier signal is the important asymmetry: a pipeline store has **no `connectorConfig` of its
own**, so `storage_tier` is never present and the destination type is the only thing separating
the two engines.

## backend-service

Branch `search/sentinel-pipeline-export` (cut from `release/v5.8.45`).

| Change | File |
| --- | --- |
| `resolveExportQueryEngine`: `AZURE_SENTINEL → KUSTO_LAW`, `AZURE_SENTINEL_DATA_LAKE → KUSTO_LAKE` | `SearchExportDestinationResolver.java`, `SearchServiceImpl.java` (both copies) |
| `buildActualExportQuery`: both Sentinel destination types plan through `kqlSearchService.planExportQuery` alongside ADX | `SearchServiceImpl.java` |
| `validateExportQuery`: the pipeline KQL branch now keys on `KqlPipelineStoreSupport.isKqlPipelineDestination` instead of `== AZURE_DATA_EXPLORER`, so it covers all three KQL destination types | `SearchServiceImpl.java` |
| `applySentinelExportConfig` resolves its connector through the new `resolveSentinelExportConnector`, which branches on store type | `SearchServiceImpl.java` |
| `storageTierFor(DestinationType)` and `toSearchMetadata(...)` — the workspace/tier half of the mapper, without credentials | `SentinelDestinationConfigMapper.java` |
| `resolveForDestination` reuses `storageTierFor` rather than repeating the tier mapping | `KqlConnectorConfigResolver.java` |

`toSearchMetadata` exists because export planning needs only the workspace and its tier.
`toConnectorConfig` (interactive search) additionally resolves the Entra client secret; calling
it from the export path would decrypt a secret that the planner never reads — the **worker**
resolves credentials itself from `dataStoreId`. `toConnectorConfig` is now defined as
`toSearchMetadata` plus credentials, so the two cannot drift.

The destination-type → engine mapping is duplicated in two `resolveExportQueryEngine` methods.
That duplication predates this change (ADX, Synapse and SPL are all in both); it is left as-is
rather than unified in the same commit.

## databahn-jobs

Branch `search/sentinel-pipeline-export` (cut from `release/v5.8.45`).

**New — `internal/store/destination/pipeline_sentinel.go`**, mirroring `pipeline_adx.go`:

- `SentinelTierForDestinationType(destType)` — the worker-side twin of `storageTierFor`.
- `LoadPipelineSentinelConfig(ctx, db, destID, tenantID, destType)` — reads the destination's
  merged configuration (inline **plus** secret overrides, so rotated secrets are picked up),
  maps `azure_sentinel_auth_azure_{tenant,client}_id` / `..._client_secret` / `workspace_id` /
  `workspace_name` onto a `SentinelConfig`, and validates it.

**Edited — `internal/store/datastore/store.go`:**

- `DeriveQueryEngine`: `DATABAHN_DESTINATION` + `AZURE_SENTINEL` → `KUSTO_LAW`,
  `+ AZURE_SENTINEL_DATA_LAKE` → `KUSTO_LAKE`.
- `LoadExportDataStore`: the linked-destination switch loads a pipeline Sentinel config and
  stamps `ExternalSearchProvider = AZURE_SENTINEL`.
- The external-store Sentinel loader is now guarded by `result.Sentinel == nil`, so it does not
  overwrite a config the pipeline branch already loaded — the same guard the ADX path uses.

`factory.go` needed no change: it keys on `store.Sentinel`, not on the store shape.

## Why `workspace_name` is required on both tiers

Only the lake tier uses `workspace_name` (its KQL `db` is `workspaceName-workspaceId`, not the
GUID). The pipeline mapper nonetheless requires it for both tiers, because
`SentinelDestinationConfigMapper.toConnectorConfig` — the interactive-search path — already
does. A Sentinel destination without `workspace_name` cannot be searched at all, so relaxing it
for export alone would only produce an inconsistency, not a working export.

## Verification

- backend-service: `mvn -o -pl search/common,search/web test` — 63 + 1263 tests, all passing.
  New cases cover `storageTierFor`, `toSearchMetadata`'s credential-free shape, and resolution of
  pipeline Sentinel/Sentinel-Lake stores to both S3 and Azure Blob global destinations.
- databahn-jobs: `go build ./... && go test ./internal/...` — passing. New cases cover
  `DeriveQueryEngine` for both pipeline destination types (including that a stray `storage_tier`
  on the store is ignored), the destination-config mapping, and lake `db` derivation.
- **Not yet exercised against a live pipeline Sentinel destination.** Neither tier has been run
  end-to-end from a real workspace; the lake v2 frame decoder in particular remains inferred
  from the API contract rather than observed.
