# Search Export for the Microsoft Sentinel Data Lake

Plan for extending search export to the Sentinel **lake** tier (`KUSTO_LAKE`), alongside the
analytics tier (`KUSTO_LAW`) that already works.

Companion to `docs/sentinel-search-export.md`, which covers the analytics tier and the
reasoning behind the row-stream design. This document covers only what differs.

Status: **implemented in databahn-jobs** (2026-08-24), uncommitted on
`search/security-lake-export`, which already carries Security Lake, ADX and Sentinel
analytics export. backend-service is **not** done — see section 4. Not run against a real
lake workspace; the v2 decoder is written against the format inferred in section 5.

---

## 1. Short answer: yes, and it is the smallest of the four

The Sentinel work was built with this in mind. `sentinelTransport` is a two-method interface
and `logAnalyticsTransport` is one implementation of it; everything above that line — window
planning, canonical columns, the row budget, encoding, upload, the SAS download link — is
tier-agnostic and already written.

Adding the lake tier is a second transport plus a second response decoder. Nothing in the
pipeline, factory dispatch, or upload path changes shape.

Today the tier is refused in exactly two places:

- `destination/sentinel_config.go` — `Validate()` returns *"Sentinel data lake export is not
  supported yet (storage_tier=LAKE)"*
- `query/sentinel.go` — `NewSentinelExecutor` returns the same error instead of building a
  transport

Both become a branch rather than an error.

---

## 2. What actually differs

Both tiers speak KQL and return tabular results, but almost every transport detail is
different. This is the whole of the change.

| | Analytics (`KUSTO_LAW`) | Lake (`KUSTO_LAKE`) |
| --- | --- | --- |
| Endpoint | `api.loganalytics.io/v1/workspaces/{id}/query` | `api.securityplatform.microsoft.com/lake/kql/v2/rest/query` |
| Entra scope | `https://api.loganalytics.io/.default` | `4500ebfb-89b6-4b14-a480-7f749797bfcd/.default` |
| Workspace identity | GUID in the URL path | `db` field in the body — `workspaceName-workspaceId` |
| Request body | `{"query": "..."}` | `{"csl": "...", "db": "...", "properties": {"Options": {...}}}` |
| Server timeout | `Prefer: wait=600` header (10 min ceiling) | `Options.servertimeout` — backend uses `00:04:00` |
| Response | v1 envelope: `{"tables":[{"columns":[…],"rows":[…]}]}` | Kusto **v2**: a JSON *array of frames*; the `DataTable` frames carry the rows |
| Errors | top-level `error` object | an error frame within the array |
| Extra config | — | `workspace_name` required |
| Rate limiting | ~200 req / 30 s | aggressive 429s, with `Retry-After` **and** a `retry after <ts> UTC` message body |
| Table reference | table name as written | normalized to a bare table — `external_table(...)` often returns empty |
| API maturity | GA | preview (`2024-05-01-preview` on the sibling metadata API) |

Two consequences worth pulling out.

**The v2 frame format is the real work.** The analytics decoder streams a known envelope:
walk to `tables`, then `columns`, then `rows`. A v2 response is a top-level array where each
element is a frame, and only `DataTable` frames matter — backend-service's
`KustoJsonResponseParser.dataTablesFromFrames` filters them out of the stream. The same
streaming approach applies, but it is a separate decoder, not a tweak to the existing one.

**The `db` field is a composite, and getting it wrong fails obscurely.**
`workspaceName-workspaceId`, not the GUID alone — `AzureConstants.WORKSPACE_NAME` documents
this as a footgun already. A store tiered to LAKE without `workspace_name` cannot be exported,
so that has to be a validation error at load time rather than a query failure later.

---

## 3. Work — databahn-jobs

### New files

1. **`internal/searchExport/query/sentinel_lake_transport.go`** — a second
   `sentinelTransport`. Entra token for the lake scope, `POST` to the v2 query endpoint with
   the `csl`/`db`/`properties` body, and 429 handling that reads both the `Retry-After` header
   and the `retry after … UTC` message body. The existing
   `parseRetryAfter` / `sentinelFallbackRetryDelay` helpers are reusable as-is.
2. **`internal/searchExport/query/sentinel_lake_response.go`** — streaming v2 frame decoder,
   with the same signature as `decodeLogAnalyticsResponse` so `StreamRows` does not care which
   tier produced the rows: walk the top-level array, skip non-`DataTable` frames without
   materialising them (the token-walking `skipValue` already does this), and emit rows from the
   first data frame. Value conversion (`rawToExportValue`) is shared unchanged.

### Edits

| File | Change |
| --- | --- |
| `query/sentinel.go` | `case SentinelTierLake:` builds the lake transport instead of returning an error |
| `query/sentinel_transport.go` | endpoint allowlist gains the lake host; the scope is per-tier rather than derived from the endpoint |
| `store/destination/sentinel_config.go` | stop rejecting `LAKE`; require `workspace_name` when the tier is `LAKE`, and expose the composite `db` value |
| `searchExport/models/model.go` | `QueryEngineKustoLake = "KUSTO_LAKE"` |
| `store/datastore/store.go` | `DeriveQueryEngine` returns `KUSTO_LAKE` when `storage_tier = LAKE` — it currently returns `KUSTO_LAW` for both and lets config validation reject the lake |
| `searchExport/factory/factory.go` | engine case for `KUSTO_LAKE`; `IsSupportedExportMatrix` treats it like `KUSTO_LAW` (any destination the uploader supports) |
| `query/stream_options.go` | lake-specific timeout and retry defaults — the 4-minute server timeout and heavier throttling do not match the analytics numbers |

No new dependency: `azidentity` and `net/http` cover it, as they do for the other two Azure
engines.

---

## 4. Work — backend-service

Small, because the plumbing was written for both tiers.

1. `SearchExportDestinationResolver.isUnsupportedKqlEngine` and
   `SearchServiceImpl.isUnsupportedKqlEngine` currently return true only for `KUSTO_LAKE`.
   Once the lake exports, both methods always return false and should be deleted rather than
   left as dead guards.
2. `allowedExportDestinationTypes` gains `KUSTO_LAKE → SENTINEL_EXPORT_TYPES`.
3. `SentinelExportTier.resolveExternalEngine` already returns `KUSTO_LAKE` for a lake store —
   **no change**. That split was added precisely so this day would be a one-line unblock.
4. `buildActualExportQuery` already routes `AZURE_SENTINEL` through
   `kqlSearchService.planExportQuery` for both tiers — **no change**.
5. `KqlQueryPlanner` — the lake needs `SentinelLakeKqlSupport.rewriteLakeTablePrefix` applied
   to the planned query, as interactive lake search already does. This is the one real
   addition: without it the exported query may return empty.
6. `exportTakeCap` already returns `MAX_SENTINEL_EXPORT_TAKE_LIMIT` for any non-ADX KQL
   engine, so the lake inherits the 200,000 cap. Whether that is the right number is open —
   see below.

---

## 5. Risks, and what I cannot answer from here

- **The v2 frame shape is the main unknown.** I have modelled it from
  `KustoJsonResponseParser`, which is defensive precisely because the payload varies — it
  unwraps `value` / `data` / `result` / `response` / `queryResult` envelopes before looking for
  frames. That defensiveness suggests the real responses were not uniform. A captured response
  from a live lake workspace would settle the decoder design in an afternoon; without one, the
  decoder is written against inference.
- **The row cap is inherited, not measured.** 200,000 was chosen for the analytics tier from
  its documented 500k-row / ~64 MB limits. The lake API's limits are not documented the same
  way, and it is built for larger volumes — so the cap may be too low (wasting the tier's main
  advantage) or too high (failing late). Needs a real workspace.
- **Throttling is the operational risk.** Backend-service carries a bespoke retry ladder for
  the lake with configurable max retries, which the analytics tier never needed. An export is
  a long sequence of requests, so the worker will meet this harder than interactive search does.
- **Preview API.** The sibling metadata endpoint is pinned to `2024-05-01-preview`. A preview
  contract can change under us in a way the GA Log Analytics API will not.
- **No test target.** As with ADX and analytics, none of this can be verified without a
  tenant that has a lake-tiered workspace.

---

## 6. Effort and sequencing

Roughly **300–400 LOC plus tests** in databahn-jobs, and **~20 lines** in backend-service.
Smaller than the analytics tier was, because only the transport and decoder are new.

1. Capture a real v2 response from a lake workspace. Everything else is cheap; this is the
   only step that removes real uncertainty, and doing it first avoids writing a decoder twice.
2. databahn-jobs: transport, decoder, config, wiring, unit tests against the captured payload.
3. backend-service: delete the two `isUnsupportedKqlEngine` guards, add the destination types,
   apply `rewriteLakeTablePrefix` when planning a lake export.
4. Verify against a live workspace: a csv export, an empty range, a throttled run, and a range
   large enough to find the real row ceiling.

**Recommendation:** worth doing, and cheap relative to the other three — but gate it on step 1
and on a customer actually asking. The lake tier is where large historical exports would live,
which is exactly where an unmeasured row cap and an undocumented throttling profile hurt most.

---

## 7. What landed in databahn-jobs

New files:

- `query/sentinel_lake_transport.go` — the second `sentinelTransport`: Entra token for the
  lake resource scope (a GUID, so unlike Log Analytics it cannot be derived from the host),
  the `csl`/`db`/`properties` request body, and 429 handling that reads `Retry-After` and
  falls back to the `retry after … UTC` message the API writes into the body instead.
- `query/sentinel_lake_response.go` — streaming Kusto v2 frame decoder. Emits only the
  `PrimaryResult` frame, skips metadata frames without materialising them, fails on a
  `DataSetCompletion` frame reporting errors, and also accepts the v1 object shape.
- Tests: `sentinel_lake_response_test.go`, `sentinel_lake_transport_test.go`.

Edited:

- `destination/sentinel_config.go` — `LAKE` accepted; `workspace_name` required for it;
  `LakeDatabase()` builds the composite identifier.
- `query/sentinel.go` — builds the lake transport, reports `KUSTO_LAKE` from `Engine()`, and
  validates the endpoint against a per-tier host allowlist rather than one shared list.
- `query/executor.go`, `models/model.go` — `KUSTO_LAKE` engine constant.
- `datastore/store.go` — `DeriveQueryEngine` takes the storage tier and returns `KUSTO_LAKE`
  for a lake store, instead of reporting analytics for both and rejecting later.
- `factory/factory.go` — the Sentinel case covers both engines; the support matrix treats
  them alike.
- `pipeline/pipeline.go`, `pipeline/sentinel_stream.go` — dispatch accepts both engines, and
  the stream options are chosen per tier.
- `query/stream_options.go` — `SentinelStreamOptionsForTier` clamps the lake to the
  4-minute server timeout.

Verification: `go build ./...`, `go vet`, `go test ./internal/...` all pass.

Three decisions worth reviewing:

1. **The endpoint allowlist is now per-tier.** A lake host is invalid for analytics and vice
   versa, because the Entra scopes differ — accepting either would mint a token for the wrong
   audience.
2. **`DeriveQueryEngine` gained a parameter.** It previously returned `KUSTO_LAW` for both
   tiers and left config validation to reject the lake. That no longer works once the lake is
   supported, since the engine selects the transport.
3. **The decoder accepts both response shapes.** Only the v2 frame array should occur, but
   `KustoJsonResponseParser` accepts the v1 object too, and matching that costs one branch.
