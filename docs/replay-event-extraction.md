# Replay Event Extraction

This document describes the **full unparsed pipeline**: how normalization services publish failed events to Kafka, how global-destination dispensers write them to object storage, and how **databahn-jobs** reads those files during replay.

**Scope:**

| Layer | Service | Responsibility |
|-------|---------|----------------|
| Produce | `normalization-service`, `custom-normalization-service`, `schemaless-normalization-service` | Build unparsed payload, publish to Kafka |
| Store | `globaldestination-s3-dispenser`, `globaldestination-azure-blob-dispenser` | Consume Kafka, write one JSON line per event to S3 / Azure Blob |
| Replay | `databahn-jobs` | Read stored lines, extract raw log, publish to Kafka |

Replay implementation:

| Piece | Path |
|-------|------|
| Line routing | `internal/replay/processor/reader.go` — `prepareReplayLine` |
| UNPARSED extraction | `internal/replay/model/unparsed_event.go` — `ExtractUnparsedRawLog` |
| CUSTOM parsed extraction | `internal/replay/processor/reader.go` — `getRawDataFromDataBahnParsedObject` |

Tests: `internal/replay/model/unparsed_event_test.go`, `internal/replay/processor/reader_test.go`.

---

## End-to-end flow

```
  OutOfBox (Vector) ──┐
  Custom (Vector)   ├──► Kafka: db.global.destination.unparsed.{s3|azureblob}.N
  Schemaless (Go)  ──┘              │
                                    ├──► S3 dispenser (Logstash)  ──► S3 object (1 JSON line / event)
                                    └──► Azure dispenser (Vector) ──► Blob object (1 JSON line / event)
                                                    │
                                                    ▼
                              databahn-jobs replay_type=UNPARSED
                              getRawDataFromUnparsedObject → plain raw log → Kafka
```

**Storage contract:** Files on the global unparsed destination contain a **JSON envelope per line**, not the raw syslog string. Replay is responsible for stripping that envelope.

---

## Replay types at a glance

| Replay type | When used | Line transformation | Kafka payload on replay |
|-------------|-----------|---------------------|-------------------------|
| **UNPARSED** | Files from global unparsed destination (S3 / Azure / Snowflake) | Extract original raw log from stored JSON envelope | Plain string (the original event) |
| **CUSTOM** (or empty) | Default replay (parsed backups, raw archives) | Depends on `forward_data_type` and file type | See [CUSTOM replay](#custom-replay) |
| **UNDELIVERED** | Undelivered / stored pipeline events | None — publish line as stored | Full stored JSON line |

### Replay routing (`prepareReplayLine`)

```go
switch {
case matchReplayType(req.ReplayType, "UNDELIVERED"):
    return line, nil

case matchReplayType(req.ReplayType, "UNPARSED"):
    return getRawDataFromUnparsedObject(line)

case matchReplayType(req.ReplayType, "CUSTOM"), strings.TrimSpace(req.ReplayType) == "":
    if isParquetFile { return line, nil }
    if strings.EqualFold(forwardDataType, "parsed") {
        return getRawDataFromDataBahnParsedObject(line)
    }
    return line, nil
// ...
}
```

---

## Layer 1 — Normalization producers (Kafka)

Three normalization paths publish unparsed events when `db_glob_dest` includes `UNPARSED`. They use **two different Kafka wire formats**.

### Producer comparison

| Source | Engine | `parser_name` | Kafka body shape | Routing metadata |
|--------|--------|---------------|------------------|------------------|
| **OutOfBox** | Vector (`normalization-service`) | `on_error_decision` | Vector event: `{ headers, message, topic }` | In body `headers` + Kafka headers |
| **Custom** | Vector (`custom-normalization-service`) | `on_error_decision` | Same Vector wrap | Same |
| **Schemaless** | Go (`schemaless-normalization-service`) | `schemaless` | Flat JSON only | Kafka headers only |

### A. Vector-based (OutOfBox, Custom) — Kafka value

Published via `glob_dest_kafka_output` with `encoding.codec: json` (entire Vector event serialized).

**Topics:** `db.global.destination.unparsed.s3.N` or `db.global.destination.unparsed.azureblob.N`

```json
{
  "headers": {
    "db_glob_path": "unparsed",
    "db_tenant_id": "9c1a1fcb-7b21-4772-99ef-d4ea424af451",
    "db_source_name": "unparsed-scenario",
    "destination_id": "564225e3-977d-4ff4-847c-13d09100c6d7",
    "db_event_source_id": "...",
    "db_device_type": "entra_id",
    "db_device_vendor": "microsoft"
  },
  "message": {
    "error": "function call error for \"parse_json\" ...",
    "rawevent": {
      "error": "function call error for \"parse_json\" ...",
      "metadata": { "db_device_type": "...", "db_log_type": "json" },
      "parser_name": "on_error_decision",
      "msg": "...",
      "rawevent": "..."
    }
  },
  "topic": "db.global.destination.unparsed.s3.0"
}
```

Inner `message` is built in `custom-normalization-service/configs/output/1.error-handler.yaml` (`form_db_glob_dest`):

```vrl
message = { "msg": .metadata.rawevent, "error": .error, "parser_name": .parser_name, "metadata": { ... } }
.message.rawevent = del(.message)
.message.error = del(.error)
.headers.db_glob_path = "unparsed"
```

OutOfBox uses the same wrap pattern; the inner field for the raw log was renamed from `msg` to `rawevent` in the newer format (see [OutOfBox old vs new](#outofbox-old-vs-new--side-by-side)).

### B. Schemaless — Kafka value

Published directly from `schemaless-normalization-service/pkg/processor/processor.go` → `handleFailedEvent`. The Kafka **message body** is only the payload; headers are separate Kafka headers.

```json
{
  "error": "Failed to parse data: ReadMapCB: expect { or n, but found J ...",
  "metadata": {
    "db_device_type": "test-unparsed",
    "db_device_vendor": "test-unparsed",
    "db_event_source_id": "c55d431a-dd9b-4137-9eab-008d34144bee",
    "db_log_type": "json"
  },
  "msg": "Jun 9 16:00:01 server01 CRON[1001]: (root) CMD (/usr/local/bin/backup.sh)",
  "parser_name": "schemaless"
}
```

Kafka headers include `db_glob_path: "unparsed"`, `db_tenant_id`, `db_source_name`, etc.

---

## Layer 2 — Global destination dispensers (object storage)

Dispensers consume the Kafka topics above and write **one line per event** to S3 or Azure Blob.

### What should be stored (canonical file line)

| Normalization | Correct stored line |
|---------------|---------------------|
| OutOfBox / Custom | Inner envelope: `{ "error": "...", "rawevent": { ... } }` |
| Schemaless | Full flat JSON (same as Kafka body): `{ "error", "metadata", "msg", "parser_name" }` |

**Not stored:** the raw syslog string alone, the outer Vector `{ headers, message, topic }` wrapper, or Logstash sprintf literals like `%{[event][message]}`.

### Storage path layout

| `db_glob_path` | S3 prefix (Logstash) | Azure prefix (Vector) |
|----------------|----------------------|------------------------|
| `unparsed` | `{path}unparsed/{db_source_name}/year=.../month=.../date=.../hour=...` | `unparsed/{db_source_name}/year=.../month=.../day=.../hour=...` |
| `undelivered` | `{path}undelivered/{db_source_name}/{destination_id}/year=...` | `undelivered/{db_source_name}/{destination_id}/year=...` |

---

## Why Custom and Schemaless files differ on Azure (template walkthrough)

Azure uses **one unparsed code path** for all normalization types. There is no `if custom` / `if schemaless` branch. The stored file shape differs because **producers publish different Kafka payloads**, and the template only unwraps payloads that contain a top-level `message` key.

### Azure Kafka input (before template)

`configs/input.yaml` consumes topics matching `db.global.destination.(undelivered|unparsed).azureblob.N`.  
`main_pipeline_transform` does **not** parse `.message` — it only derives `event_type` from the topic name.

After consume, every event has:

```
.message  = Kafka message VALUE (payload bytes as string)
.headers  = Kafka message HEADERS (db_glob_path, db_tenant_id, db_source_name, ...)
```

### The only unparsed write logic

In `transform_extract_raw_event` (per-tenant `vector.tpl`):

```vrl
if .headers.db_glob_path == "unparsed" {
    .messagejson, err = parse_json(.message)
    if err == null {
        .message = .messagejson.message
    }
}
```

The blob sink writes **only** `.message` (`encoding.codec: raw_message`).

---

### Flow A — Custom / OutOfBox (Vector producer)

```
  CUSTOM VECTOR (form_db_glob_dest)
  ─────────────────────────────────
  .headers  = { db_glob_path: "unparsed", db_tenant_id: "...", ... }
  .message  = { error: "...", rawevent: { msg|rawevent: "...", metadata, parser_name } }
  .topic     = "db.global.destination.unparsed.azureblob.0"
           │
           │  glob_dest_kafka_output (codec: json)
           ▼
  KAFKA VALUE (entire Vector event serialized):
  ┌─────────────────────────────────────────────────────────────┐
  │ {                                                           │
  │   "headers": { "db_glob_path": "unparsed", ... },          │
  │   "message": { "error": "...", "rawevent": { ... } },  ◄── inner envelope
  │   "topic": "db.global.destination.unparsed.azureblob.0"   │
  │ }                                                           │
  └─────────────────────────────────────────────────────────────┘
  KAFKA HEADERS: same routing fields (db_tenant_id, db_glob_path, ...)
           │
           │  Azure Kafka source
           ▼
  DISPENSER EVENT:
  .message = '{"headers":{...},"message":{"error":"...","rawevent":{...}},"topic":"..."}'
  .headers = { db_glob_path: "unparsed", ... }
           │
           │  parse_json(.message)  →  messagejson
           │  .message = messagejson.message
           ▼
  .message = { "error": "...", "rawevent": { "msg": "...", ... } }   ← outer wrap STRIPPED
           │
           │  azure_blob sink (raw_message)
           ▼
  FILE LINE:
  {"error":"...","rawevent":{"msg":"hello world...","parser_name":"on_error_decision",...}}
```

**Why the file is the inner envelope:** Line `.message = .messagejson.message` finds the nested `message` property in the Vector Kafka value and replaces `.message` with that object. The `{ headers, message, topic }` wrapper is discarded before write.

---

### Flow B — Schemaless (Go producer)

```
  SCHEMALESS Go (handleFailedEvent)
  ─────────────────────────────────
  Kafka BODY only (UnparsedEventPayload):
  ┌─────────────────────────────────────────────────────────────┐
  │ {                                                           │
  │   "error": "Failed to parse data: ...",                     │
  │   "metadata": { db_device_type, db_event_source_id, ... },  │
  │   "msg": "Jun 9 16:00:01 server01 CRON[1001]: ...",    ◄── not "message"
  │   "parser_name": "schemaless"                               │
  │ }                                                           │
  └─────────────────────────────────────────────────────────────┘
  KAFKA HEADERS: db_glob_path, db_tenant_id, db_source_name, ... (NOT in body)
           │
           │  Azure Kafka source
           ▼
  DISPENSER EVENT:
  .message = '{"error":"...","metadata":{...},"msg":"Jun 9...","parser_name":"schemaless"}'
  .headers = { db_glob_path: "unparsed", ... }
           │
           │  parse_json(.message)  →  messagejson = { error, metadata, msg, parser_name }
           │  .message = messagejson.message  →  key "message" DOES NOT EXIST
           ▼
  .message unchanged (still the full flat JSON string from Kafka)
           │
           │  azure_blob sink (raw_message)
           ▼
  FILE LINE (same as Kafka body):
  {"error":"...","metadata":{...},"msg":"Jun 9 16:00:01 ...","parser_name":"schemaless"}
```

**Why the file is flat JSON:** Schemaless never publishes a Vector-style `{ headers, message, topic }` wrap. The raw log field is `msg`, not `message`. The template rule `.message = .messagejson.message` has nothing to unwrap, so the full Kafka payload is written as-is.

---

### Side-by-side: same template, different Kafka input

```
                    CUSTOM / OUTOFBOX                    SCHEMALESS
                    ─────────────────                    ──────────
Kafka value has     top-level "message" key              NO "message" key
                    (inner envelope)                     (uses "msg" for raw log)

messagejson.message EXISTS                         messagejson.message MISSING

After line 25       .message = inner envelope          .message = flat JSON (unchanged)

File on disk        { error, rawevent: { ... } }       { error, metadata, msg, parser_name }
```

| | Custom / OutOfBox | Schemaless |
|--|-------------------|------------|
| Publisher | Vector (`codec: json` on whole event) | Go (`errorPayloadJSON` only) |
| Payload key for content | `message` → `{ error, rawevent }` | `msg` → raw log string |
| Template line 25 | Unwraps `messagejson.message` | No unwrap (no `message` key) |
| Azure file | Inner envelope only | Full flat JSON |

Both shapes are valid for UNPARSED replay (`rawevent.msg` / `rawevent.rawevent` vs top-level `msg`).

---

### S3 dispenser fix — what and where to change

**Repository:** `globaldestination-s3-dispenser`

| Item | Location |
|------|----------|
| **Fix (required)** | `manifests/destination.tpl` — unparsed `else` branch when `db_glob_path != "undelivered"` |
| **Prerequisite** | `pipeline/logstash.conf` — `decorate_events => extended` (provides `[event][original]`) |
| **Rendered output** | `/usr/share/logstash/pipeline/{tenant-uuid}.conf` — regenerated by Go control plane on deploy / config upsert |
| **Entrypoint** | `cmd/aws-s3-dispenser.go` starts Logstash with `-r` (auto-reload pipeline dir) |

**Root cause:** The old unparsed branch always set `output_message => "%{[event][message]}"`. Schemaless Kafka bodies use `msg`, not `message`, so Logstash wrote the literal `%{[event][message]}` to S3.

**Fix:** Branch after `json { source => "message" target => "event" }`:

1. `_jsonparsefailure` → `%{[event][original]}`
2. `[event][message]` present → `%{[event][message]}` (Vector: Custom / OutOfBox)
3. else → `%{[event][original]}` (Schemaless flat JSON)

**Deploy steps:**

1. Merge / release `globaldestination-s3-dispenser` with updated `destination.tpl`.
2. Redeploy the service (ArgoCD / k8s rollout).
3. Confirm per-tenant `.conf` files under `/usr/share/logstash/pipeline/` contain `handle_unparsed_events_schemaless_` mutate block.
4. Send test Schemaless unparsed event; verify S3 line is full JSON (not `%{[event][message]}`).
5. Check Logstash stats: `handle_unparsed_events_schemaless_*` counter increments.

**Does not fix:** Lines already on S3 written as `%{[event][message]}` — replay rejects these via `model.ExtractUnparsedRawLog` (see `unparsed_event.go`).

---

### S3 dispenser (Logstash) — explicit branching

S3 cannot rely on missing-key behavior. The unparsed branch in `destination.tpl` explicitly branches:

| Condition | `output_message` source | Used for |
|-----------|-------------------------|----------|
| `_jsonparsefailure` | `[event][original]` | Parse errors |
| `[event][message]` present | `[event][message]` | Vector: OutOfBox / Custom |
| else | `[event][original]` | Schemaless flat JSON |

Requires `decorate_events => extended` in `pipeline/logstash.conf` so `[event][original]` holds the raw Kafka payload.

> **Historical bug (pre-fix):** S3 used only `%{[event][message]}` for all unparsed events. Schemaless has `msg`, not `message`, so files contained the literal `%{[event][message]}` and replay failed.

### Dispenser verification matrix (tested)

| Normalization | Azure stored | S3 stored (after fix) |
|---------------|--------------|------------------------|
| OutOfBox new (`rawevent.rawevent`) | Envelope | Envelope |
| OutOfBox old (`rawevent.msg`) | Envelope | Envelope |
| Custom (`rawevent.msg`) | Envelope | Envelope |
| Schemaless (flat `msg`) | Full flat JSON | Full flat JSON |

JSON key ordering in stored files may differ from Kafka; that does not affect replay.

---

## Layer 3 — UNPARSED replay (`databahn-jobs`)

**Goal:** Read each stored JSON envelope line and republish only the **original raw log** so the pipeline can re-process it.

### `model.UnparsedStoredLine` and `ExtractUnparsedRawLog`

UNPARSED extraction lives in `internal/replay/model/unparsed_event.go` (not in the processor). `reader.go` delegates via `getRawDataFromUnparsedObject` → `model.ExtractUnparsedRawLog`.

```go
// Types
type UnparsedStoredLine struct {
    RawEvent *UnparsedNestedPayload `json:"rawevent"`
    Message  string                 `json:"msg"`
}
type UnparsedNestedPayload struct {
    Msg      string `json:"msg"`
    RawEvent string `json:"rawevent"`
}

// Entry point
func ExtractUnparsedRawLog(line string) (string, error)
```

Extraction order matches [stored shape → replay extraction](#stored-shape--replay-extraction) below. Also rejects empty lines and the exact bad S3 line `%{[event][message]}` (pre-fix Schemaless).

### Stored shape → replay extraction

| Source | Stored line shape | Field replay extracts |
|--------|-------------------|----------------------|
| Schemaless | Flat `{ error, metadata, msg, parser_name }` | top-level `msg` |
| Custom | `{ error, rawevent: { msg, ... } }` | `rawevent.msg` |
| OutOfBox old | `{ error, rawevent: { msg, ... } }` | `rawevent.msg` |
| OutOfBox new | `{ error, rawevent: { rawevent, ... } }` | `rawevent.rawevent` |

### Extraction order (`ExtractUnparsedRawLog`)

1. If top-level **`rawevent` is absent or null** → use top-level **`msg`** (Schemaless).
2. If top-level **`rawevent` is an object**:
   - Try **`rawevent.msg`** first (Custom, old OutOfBox).
   - Else try **`rawevent.rawevent`** (new OutOfBox).
   - Fail if neither is present.

`msg` is checked before nested `rawevent` so old OutOfBox and Custom records keep working after the OutOfBox structure change.

> **Not supported on UNPARSED:** top-level `rawevent` as a plain string (`{"rawevent":"log line"}`). That shape is produced by **CUSTOM parsed** backups and is handled by `getRawDataFromDataBahnParsedObject`, not `ExtractUnparsedRawLog`.

```go
// Schemaless: flat document (RawEvent == nil)
if stored.RawEvent == nil {
    return stored.Message, nil
}
// Nested rawevent object: Msg first, then RawEvent
```

---

## Samples by normalization type

### 1. Schemaless — flat structure

**Kafka body** = **stored file line** (after dispenser fix on S3):

```json
{
  "error": "Failed to parse data: ReadMapCB: expect { or n, but found J",
  "metadata": {
    "db_device_type": "test-unparsed",
    "db_device_vendor": "test-unparsed",
    "db_event_source_id": "c55d431a-dd9b-4137-9eab-008d34144bee",
    "db_log_type": "json"
  },
  "msg": "Jun 09 12:15:01 prod-web-01 docker[1234]: Authorization: Bearer ghp_...",
  "parser_name": "schemaless"
}
```

**Replay publishes:**

```
Jun 09 12:15:01 prod-web-01 docker[1234]: Authorization: Bearer ghp_...
```

---

### 2. Custom — nested `rawevent.msg`

**Kafka** (Vector wrap) → **stored** (inner `message` only):

```json
{
  "error": "unable to parse",
  "rawevent": {
    "error": "unable to parse",
    "metadata": {
      "db_device_type": "custom_test-unparsed",
      "db_device_vendor": "test-unparsed",
      "db_event_source_id": "aa3d0543-97f0-422d-8106-eabdc63244ea",
      "db_log_type": "json"
    },
    "msg": "hello world this is not syslog format at all",
    "parser_name": "on_error_decision"
  }
}
```

**Replay publishes:**

```
hello world this is not syslog format at all
```

---

### 3. OutOfBox — old structure (`rawevent.msg`)

```json
{
  "error": "function call error for \"parse_json\" at (15:35): unable to parse json: expected value at line 1 column 1",
  "rawevent": {
    "error": "function call error for \"parse_json\" at (15:35): unable to parse json: expected value at line 1 column 1",
    "metadata": {
      "db_device_type": "entra_id",
      "db_device_vendor": "microsoft",
      "db_event_source_id": "c2e033f1-75d1-4caa-96c1-75b86efd0e85",
      "db_log_type": "json"
    },
    "msg": "Jun 03 21:12:45 server01 kernel: device eth0 entered promiscuous mode",
    "parser_name": "on_error_decision"
  }
}
```

**Replay publishes:**

```
Jun 03 21:12:45 server01 kernel: device eth0 entered promiscuous mode
```

---

### 4. OutOfBox — new structure (`rawevent.rawevent`)

```json
{
  "error": "function call error for \"parse_json\"",
  "rawevent": {
    "error": "function call error for \"parse_json\"",
    "metadata": { "db_log_type": "json" },
    "parser_name": "on_error_decision",
    "rawevent": "Jun 09 12:15:01 prod-web-01 sudo[8921]: session opened for user deploy(uid=1001)"
  }
}
```

**Replay publishes:**

```
Jun 09 12:15:01 prod-web-01 sudo[8921]: session opened for user deploy(uid=1001)
```

---

### OutOfBox old vs new — side by side

| Aspect | Old OutOfBox | New OutOfBox |
|--------|--------------|--------------|
| Inner field for raw log | `rawevent.msg` | `rawevent.rawevent` |
| `parser_name` | `on_error_decision` | `on_error_decision` |
| Top-level `error` | Present (duplicate of inner) | Present (duplicate of inner) |
| Replay extraction | `getRawDataFromUnparsedObject` → nested `msg` | Same → nested `rawevent` |

---

### UNPARSED — quick identifier cheatsheet

| If you see in a stored file… | Likely source | Replay extracts |
|-----------------------------|---------------|-----------------|
| `"parser_name": "schemaless"`, no top-level `rawevent` | Schemaless | `msg` |
| `"parser_name": "on_error_decision"`, `rawevent.msg` | Custom or old OutOfBox | `rawevent.msg` |
| `"parser_name": "on_error_decision"`, `rawevent.rawevent` (string) | New OutOfBox | `rawevent.rawevent` |
| Line is `%{[event][message]}` | Bad S3 schemaless file (pre-fix) | **Not replayable** — re-send events |

> Top-level `rawevent` as a plain string (`{"rawevent":"..."}`) is a **CUSTOM parsed** backup shape, not global unparsed. Use `replay_type=CUSTOM` with `forward_data_type=parsed`.

---

## CUSTOM replay

Used for general data replay (parsed backups, raw archives, Parquet exports).

| Condition | Behavior |
|-----------|----------|
| Parquet file (`.parquet`) | Publish row as-is |
| `forward_data_type` ≠ `parsed` | Publish line as-is |
| `forward_data_type = parsed` | Extract via `getRawDataFromDataBahnParsedObject` |

### Parsed backup — string `rawevent`

Used with **`replay_type=CUSTOM`** and **`forward_data_type=parsed`** — not UNPARSED replay.

```json
{ "rawevent": "May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu" }
```

### Parsed backup — object `rawevent` with `msg`

```json
{
  "error": "function call error",
  "rawevent": {
    "metadata": { "db_log_type": "json" },
    "msg": "May 31 10:00:05 web01 sshd[2145]: Accepted publickey for ubuntu",
    "parser_name": "on_error_decision"
  }
}
```

> **Note:** CUSTOM parsed extraction does **not** read nested `rawevent.rawevent`. That path is UNPARSED-only (new OutOfBox global-destination files).

---

## UNDELIVERED replay

No extraction. The full stored event JSON is published unchanged.

```json
{
  "eventtime": 123,
  "headers": { "db_edge_ts": "123" },
  "body": "stored event payload"
}
```

---

## Error cases (UNPARSED replay)

| Input | Error |
|-------|-------|
| Invalid JSON | `failed to unmarshal Unparsed event to extract rawevent: ...` |
| No `rawevent` and no `msg` | `rawevent or msg is missing or empty` |
| Nested object missing both `msg` and `rawevent` | `nested msg or rawevent is missing or empty` |
| Top-level `rawevent` is a string (wrong type for UNPARSED) | `failed to unmarshal Unparsed event to extract rawevent: ...` |
| Line is exactly `%{[event][message]}` | `invalid stored line from S3 dispenser (logstash field literal)` |

A failed extraction fails the file (`StatusFailed`); the line is not published.

---

## Related code

| Component | Path |
|-----------|------|
| Replay routing | `databahn-jobs/internal/replay/processor/reader.go` — `prepareReplayLine` |
| UNPARSED extraction model | `databahn-jobs/internal/replay/model/unparsed_event.go` — `ExtractUnparsedRawLog` |
| UNPARSED processor delegate | `databahn-jobs/internal/replay/processor/reader.go` — `getRawDataFromUnparsedObject` |
| UNPARSED model tests | `databahn-jobs/internal/replay/model/unparsed_event_test.go` |
| CUSTOM parsed extraction | `databahn-jobs/internal/replay/processor/reader.go` — `getRawDataFromDataBahnParsedObject` |
| Replay unit tests | `databahn-jobs/internal/replay/processor/reader_test.go` |
| Schemaless producer | `schemaless-normalization-service/pkg/processor/processor.go` — `handleFailedEvent` |
| Custom Vector unparsed | `custom-normaliztion-service/configs/output/1.error-handler.yaml` — `form_db_glob_dest` |
| S3 dispenser (Logstash) | `globaldestination-s3-dispenser/manifests/destination.tpl` |
| Azure dispenser (Vector) | `globaldestination-azure-blob-dispenser/manifests/vector.tpl` |

---

## Running tests

```bash
cd databahn-jobs
go test -vet=off ./internal/replay/model/... -run 'ExtractUnparsed'
go test -vet=off ./internal/replay/processor/... -run 'Unparsed|PrepareReplayLine'
```

---

## Change impact analysis

Today the mismatch is **inconsistent Kafka publish shapes** (Vector wrap vs Schemaless flat JSON) combined with **dispensers that were originally written for the Vector shape only**. There are two main ways to fix alignment: change **Schemaless** (producer) or change **dispenser templates** (consumer). Both can also be combined.

### Current state (baseline)

| Component | Custom / OutOfBox | Schemaless |
|-----------|-------------------|------------|
| Kafka publish | Vector `{ headers, message, topic }` | Flat `{ error, metadata, msg, parser_name }` |
| Azure file | Inner envelope `{ error, rawevent }` | Flat JSON (same as Kafka body) |
| S3 file (after tpl fix) | Inner envelope | Flat JSON |
| UNPARSED replay | `rawevent.msg` / `rawevent.rawevent` | top-level `msg` |

---

### Option A — Change Schemaless publish (`schemaless-normalization-service`)

Align Schemaless with the **Vector global-destination Kafka contract** used by Custom / OutOfBox.

**Typical change:** In `handleFailedEvent`, publish a Vector-style Kafka value instead of flat JSON, e.g.:

```
{
  "headers": { db_glob_path, db_tenant_id, db_source_name, ... },
  "message": {
    "error": "...",
    "rawevent": {
      "msg": "<raw log>",
      "error": "...",
      "metadata": { ... },
      "parser_name": "schemaless"
    }
  },
  "topic": "db.global.destination.unparsed.{s3|azureblob}.N"
}
```

(Exact inner shape can mirror Custom — `rawevent.msg` — or new OutOfBox — `rawevent.rawevent`.)

#### Impact

| Area | Impact | Severity |
|------|--------|----------|
| **S3 / Azure dispensers** | Both tpl paths that read `[event][message]` / `messagejson.message` work without Schemaless-specific branches | Low — simplifies consumers |
| **Stored file shape (new events)** | Schemaless files become **inner envelope** `{ error, rawevent }`, same as Custom — **no more flat JSON on disk** | Medium — format change on storage |
| **Existing files on S3/Azure** | Old flat Schemaless lines remain until aged out; replay must still read them | Medium — needs replay backward compat (already implemented) |
| **UNPARSED replay (`databahn-jobs`)** | New files use `rawevent.msg` path instead of top-level `msg`; existing `getRawDataFromUnparsedObject` already supports both if envelope matches Custom | Low if envelope uses `rawevent.msg` |
| **Other Kafka consumers** | Anything reading `db.global.destination.unparsed.*` and assuming flat Schemaless JSON **breaks** unless updated | High — audit all consumers |
| **Metrics / alerts** | Unparsed metrics keyed off Kafka may see different payload shape | Low–Medium — verify dashboards |
| **Deployment** | Requires Schemaless service rollout; dispensers and replay can stay as-is (or simplify tpl later) | Coordinated release |
| **Snowflake global dest** | Same topic pattern — same publish change applies if Schemaless routes there | Medium |

#### Pros

- Single Kafka contract across all three normalization types.
- Dispensers can use one code path (Vector unwrap only).
- Stored Schemaless files match Custom / OutOfBox — easier ops and debugging.

#### Cons

- Producer change + rollout in `schemaless-normalization-service`.
- **Does not fix** bad S3 files already written as `%{[event][message]}` — those need re-send or ignore.
- **Does not migrate** existing flat Schemaless blobs — replay must keep dual-path extraction indefinitely (or run migration).
- Any tool that parsed flat Schemaless Kafka messages directly must change.

---

### Option B — Change dispenser templates only (`destination.tpl` / `vector.tpl`)

Keep Schemaless Kafka publish as-is; make consumers explicitly handle **both** Vector wrap and flat JSON.

**S3 (done):** `destination.tpl` branches on `[event][message]` vs `[event][original]`.

**Azure (optional hardening):** Make implicit behavior explicit in `vector.tpl`:

```vrl
if err == null && exists(.messagejson.message) {
    .message = .messagejson.message
}
# else: keep .message as flat JSON (Schemaless)
```

#### Impact

| Area | Impact | Severity |
|------|--------|----------|
| **Schemaless service** | No change | None |
| **Custom / OutOfBox** | No change to Kafka or stored format | None |
| **S3 new events** | Flat Schemaless stored correctly (post-fix) | Low — already deployed path |
| **Azure** | Behavior unchanged for Schemaless; optional `exists()` only documents intent | Low |
| **Stored file shape** | **Remains different**: Custom = envelope, Schemaless = flat JSON | Medium — ongoing dual format on disk |
| **UNPARSED replay** | Must keep supporting **both** stored shapes (`msg` vs `rawevent.msg`) | Low — already implemented |
| **Existing bad S3 files** | tpl fix does **not** repair lines already written as `%{[event][message]}` | High for historical data — re-send or skip |
| **Deployment** | Redeploy `globaldestination-s3-dispenser` (and optionally Azure); no Schemaless release | Lower coordination |
| **Template regeneration** | Go control plane re-renders per-tenant `.conf` / Vector yaml on pod restart or config upsert | Low |

#### Pros

- Faster to ship; isolated to dispenser repos.
- No risk to other Schemaless Kafka subscribers (they still get flat JSON).
- Azure already works; S3 fix closes the production gap.

#### Cons

- **Two stored formats forever** unless Schemaless is changed later.
- Replay and documentation must permanently document both shapes.
- Every new global-destination consumer must implement dual-path logic.

---

### Option C — Change both (recommended long-term pattern)

1. **Short term:** Ship tpl fix (Option B) so S3 Schemaless delivery works immediately.
2. **Long term:** Align Schemaless publish to Vector wrap (Option A) for one Kafka contract.
3. **Always:** Keep replay backward compat for old flat Schemaless files and bad `%{[event][message]}` lines.

---

### Comparison matrix

| Criteria | Change Schemaless (producer) | Change tpl (consumer) | Change both |
|----------|------------------------------|----------------------|-------------|
| Fixes S3 Schemaless delivery | Yes (if envelope has `message`) | Yes (already with branch) | Yes |
| Unified Kafka shape | Yes | No | Yes |
| Unified file shape on disk | Yes (new writes) | No | Yes (new writes) |
| Replay code change required | Optional / minimal | None | Minimal |
| Old flat files on storage | Replay must still support | Replay must still support | Replay must still support |
| Bad `%{[event][message]}` files | Not fixed | Not fixed | Not fixed |
| Services to deploy | Schemaless | S3 (± Azure) dispenser | All three |
| Risk to other Kafka consumers | Higher | Lower | Higher (producer phase) |

---

### What does **not** change regardless of option

| Item | Why |
|------|-----|
| **Bad historical S3 lines** (`%{[event][message]}`) | Already written; neither producer nor tpl retroactively fixes object storage |
| **OutOfBox old vs new** (`rawevent.msg` vs `rawevent.rawevent`) | Separate evolution; replay already handles both |
| **UNPARSED replay dual extraction** | Needed until old Schemaless flat files age out or are migrated |
| **Raw log not stored on disk** | By design — storage holds JSON envelope; replay strips it |

---

### Decision guide

```
  Need S3 Schemaless working now?
       │
       ├─► Yes ──► Deploy tpl fix (Option B) — minimum change
       │
       └─► Want one format everywhere long-term?
                │
                ├─► Change Schemaless publish (Option A) after tpl fix
                │
                └─► Keep replay backward compat for old files in all cases
```

---

## Questions for manager / product clarification

Use this checklist when aligning on direction. Each item maps to a decision in [Change impact analysis](#change-impact-analysis).

### 1. Target architecture — unify formats or tolerate dual shapes?

1. Should we **standardize unparsed Kafka publish** so Schemaless matches Custom / OutOfBox (Vector `{ headers, message, topic }` wrap), or is it acceptable to keep **two Kafka contracts** indefinitely with dispenser + replay handling both?

2. Should **stored files on S3/Azure** use one canonical on-disk shape (inner envelope `{ error, rawevent }` for all sources), or is it acceptable that Schemaless remains **flat JSON** on disk while Custom / OutOfBox use the **wrapped envelope**?

3. If we standardize, should the inner raw-log field follow **Custom** (`rawevent.msg`), **new OutOfBox** (`rawevent.rawevent`), or a **new shared schema** (e.g. version field + explicit format indicator)?

### 2. Scope of work — what do we change first?

4. Is the approved **immediate fix** deploying the **S3 `destination.tpl` branch** (`[event][message]` vs `[event][original]`) only, without waiting for Schemaless changes?

5. Do we also want **Azure `vector.tpl` hardening** (`exists(.messagejson.message)`) for explicit Schemaless handling, even though Azure already works today?

6. Is a **Schemaless producer change** in scope for this initiative, or a **follow-up** after the dispenser fix is verified in production?

### 3. Backward compatibility and historical data

7. For **existing flat Schemaless files** already on global destination: must UNPARSED replay support them **forever**, or only until a **retention / cutover date**?

8. For **bad S3 lines** already written as `%{[event][message]}`: should we **re-send** affected events, **exclude** those files from replay, or **accept data loss** for that window?

9. For **OutOfBox old** files (`rawevent.msg`) vs **new** (`rawevent.rawevent`): confirm we continue supporting **both** in replay with no migration — is that the long-term expectation?

10. If we change Schemaless publish format, is a **formal backward-compat period** required (dual read in replay + dispensers for N months), or is a **hard cutover** acceptable?

### 4. Consumers and blast radius

11. Are there **other services** besides S3/Azure dispensers and `databahn-jobs` replay that read `db.global.destination.unparsed.*` topics or unparsed blobs directly (Snowflake dispenser, reports, analytics, customer exports)? If yes, who owns updating them?

12. Do any **customer-facing integrations** depend on the current Schemaless **flat JSON** shape on Kafka or in object storage?

13. Should we run a **repo / topic consumer audit** before changing Schemaless publish format?

### 5. Deployment and ownership

14. Who approves and coordinates rollout order: **S3 dispenser → replay (already done?) → Schemaless**?

15. Is a **single coordinated release** across services required, or can dispensers ship independently of Schemaless?

16. Which environments must be validated before production (dev / staging / customer sandbox with Schemaless + S3 global dest)?

### 6. Operations and data quality

17. Do we need an **unparsed data quality check** (alert or job) that detects invalid file lines like `%{[event][message]}` on global destination?

18. What is the **retention policy** for unparsed files — does it affect how long we must keep replay backward-compat paths?

19. Should **unparsed metrics / CP alerts** treat Schemaless and Custom counts as comparable today, given different on-disk shapes?

### 7. Documentation and communication

20. Should this document become the **team canonical spec** for unparsed format, with Custom / OutOfBox / Schemaless owners sign-off?

21. Do we need to communicate a **format change** to customers or internal teams if Schemaless storage shape changes from flat JSON to envelope?

### Suggested default recommendation (for discussion)

| Question area | Proposed default unless manager says otherwise |
|---------------|-----------------------------------------------|
| Immediate fix | Deploy S3 tpl fix (Option B) |
| Long-term | Align Schemaless to Vector Kafka wrap (Option A) in a follow-up |
| Backward compat | Keep replay dual-path for old flat Schemaless + OutOfBox old/new |
| Bad historical S3 lines | Identify affected prefix/date range; re-send or exclude from replay — **needs manager call** |
| On-disk format | Tolerate dual shapes short-term; unify on envelope long-term |

---

## Design summary

1. **Producers may differ** on Kafka wire format (Vector wrap vs flat Go JSON).
2. **Dispensers normalize to a stored envelope** — unwrap Vector `message` for OutOfBox/Custom; keep full body for Schemaless.
3. **Storage is always JSON lines**, not raw syslog.
4. **Replay** (`UNPARSED`) is the only stage that strips the envelope back to the raw log.
5. **Backward compatibility** in replay handles OutOfBox old (`rawevent.msg`) and new (`rawevent.rawevent`) in the same `getRawDataFromUnparsedObject` function.
6. **Aligning the system** is a choice between changing the Schemaless producer (unify Kafka), changing dispensers (handle both shapes), or both — see [Change impact analysis](#change-impact-analysis).
7. **Open decisions** for leadership sign-off are listed in [Questions for manager / product clarification](#questions-for-manager--product-clarification).

---

## Appendix — Dispenser templates

### Azure: Kafka input (`globaldestination-azure-blob-dispenser/configs/input.yaml`)

```yaml
sources:
  main_pipeline_input:
    type: kafka
    bootstrap_servers: "${KAFKA_BROKERS}"
    group_id: "globaldestination-azure-blob-dispenser"
    topics:
      - "^db.global.destination.(undelivered|unparsed).azureblob.[0-9]+$"

transforms:
  main_pipeline_transform:
    type: remap
    inputs:
      - main_pipeline_input
    source: |
      .event_type = ""
      if string!(.topic) != null {
        topic_parts = split(string!(.topic), ".")
        if length(topic_parts) >= 4 {
          .event_type = topic_parts[3]
        }
      }
      del(.offset)
      del(.partition)
      del(.message_key)
      del(.timestamp)
      del(.source_type)
      del(.topic)
```

### Azure: per-tenant template (`globaldestination-azure-blob-dispenser/manifests/vector.tpl`)

Rendered once per tenant UUID. **Unparsed write path is lines 22–26**; blob sink uses `raw_message` on `.message`.

```yaml
transforms:
  route_to_destination_{{ .TenantUUID }}:
    type: route
    inputs:
      - main_pipeline_transform
    route:
      delivery: .headers.db_tenant_id == "{{.TenantUUID}}"

  transform_extract_raw_event_{{.TenantUUID}}:
    type: remap
    inputs:
      - route_to_destination_{{ .TenantUUID }}.delivery
    source: |
           if  .headers.db_source_name == null {
             .headers.db_source_name = "unknown"
           }
           if  .headers.db_glob_path == null {
             .headers.db_glob_path = "unknown"
           }

            .path = ""
            if .headers.db_glob_path == "unparsed"{
              .messagejson, err = parse_json(.message)
              if err == null {
                  .message = .messagejson.message
              }
            }else{
            .path = to_string!(.headers.destination_id) + "/"
              {{- if eq .ForwardDataType 1 }}
              .messagejson, err = parse_json(.message)
              if err == null && .messagejson.rawevent != null {
                  .message = .messagejson.rawevent
              }
              {{- end }}
            }

  stats_timeframe_calculator_{{ .TenantUUID }}:
    inputs:
      - transform_extract_raw_event_{{.TenantUUID}}
    type: remap
    source: |
            # ... metrics fields ...
            if event_type == "unparsed" {
                .namespace = "global-destination-unparsed"
            }

sinks:
  azureblob_dispenser_default_{{.TenantUUID}}:
    type: azure_blob
    inputs:
      - transform_extract_raw_event_{{.TenantUUID}}
    compression: gzip
    blob_prefix: "{{ `{{ .headers.db_glob_path }}` }}/{{ `{{ .headers.db_source_name }}` }}/{{ `{{ .path }}` }}year=%Y/month=%m/day=%d/hour=%H/"
    connection_string: "{{.AzureBlobStorageAccountConnectionString}}"
    container_name: "{{.AzureBlobContainer}}"
    endpoint : "{{.AzureBlobStorageEndpoint}}"
    encoding:
      codec: raw_message
    batch:
      max_events: 100000
    acknowledgements:
      enabled: true
```

### S3: Kafka input (`globaldestination-s3-dispenser/pipeline/logstash.conf`)

```ruby
input {
    kafka {
        id => "kafka-input"
        bootstrap_servers => "${KAFKA_BROKERS}"
        topics_pattern => "^db.global.destination.(undelivered|unparsed).s3.[0-9]+$"
        group_id => "globaldestination-s3-dispenser"
        decorate_events => extended
    }
}
```

### S3: per-tenant template (`globaldestination-s3-dispenser/manifests/destination.tpl`)

Rendered once per destination. **Unparsed branch is the `else` of `db_glob_path == "undelivered"`** (lines 56–88).

```ruby
filter {
    if [@metadata][kafka][headers][db_tenant_id] == "{{.TenantUUID}}" {
            if [@metadata][kafka][headers][db_glob_path] == "undelivered" {
                # ... undelivered / forward_data_type handling ...
            } else {
                json {
                        source => "message"
                        target => "event"
                    }
                mutate {
                    id => "set_destination_id_default_{{.ID}}"
                    add_field => {
                        "destination_id" => ""
                    }
                }
                if ("_jsonparsefailure" in [tags]) {
                    mutate {
                        id => "handle_unparsed_events_parse_failure_{{.ID}}"
                        add_field => {
                            "output_message" => "%{[event][original]}"
                        }
                    }
                } else if [event][message] {
                    mutate {
                        id => "handle_unparsed_events_vector_{{.ID}}"
                        add_field => {
                            "output_message" => "%{[event][message]}"
                        }
                    }
                } else {
                    mutate {
                        id => "handle_unparsed_events_schemaless_{{.ID}}"
                        add_field => {
                            "output_message" => "%{[event][original]}"
                        }
                    }
                }
            }
        }
}

output {
    if [@metadata][kafka][headers][db_tenant_id] == "{{.TenantUUID}}" {
           s3 {
               id => "{{.ID}}"
               bucket => "{{ .BucketName }}"
               prefix => "{{ .Path }}%{[@metadata][kafka][headers][db_glob_path]}/%{[@metadata][kafka][headers][db_source_name]}/%{destination_id}year=%{+YYYY}/month=%{+MM}/date=%{+dd}/hour=%{+HH}"
               codec => line {format => "%{output_message}"}
           }
    }
}
```

### Custom producer: global-dest wrap (`custom-normaliztion-service/configs/output/1.error-handler.yaml`)

```vrl
# Inner payload (event_pipeline_done_handler_error)
message = {
  "msg": .metadata.rawevent,
  "error": .error,
  "parser_name": .parser_name,
  "metadata": { ... }
}
.message = message

# Global destination wrap (form_db_glob_dest)
.message.rawevent = del(.message)
.message.error = del(.error)
.headers.db_glob_path = "unparsed"
```

Published to Kafka via `glob_dest_kafka_output` with `encoding.codec: json` (serializes `{ headers, message, topic }`).

### Schemaless producer (`schemaless-normalization-service/pkg/processor/processor.go`)

```go
type UnparsedEventPayload struct {
    Error      string                `json:"error"`
    Metadata   UnparsedEventMetadata `json:"metadata"`
    Message    string                `json:"msg"`          // note: json tag is "msg", not "message"
    ParserName string                `json:"parser_name"`
}
// Kafka Message.Message = marshaled UnparsedEventPayload (flat JSON, no Vector wrap)
// Kafka headers include db_glob_path = "unparsed"
```

---

## Normalization × global destination — stored file structure

Reference matrix from tested captures. **Rows** = global destination type; **columns** = normalization type. Cells show the **stored file line structure** (one JSON object per line). **Bold** marks where the raw log string lives.

<table>
<thead>
<tr>
<th></th>
<th>OutOfBox (old)</th>
<th>OutOfBox (new)</th>
<th>Custom</th>
<th>Schemaless</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Azure</strong></td>
<td><pre>{
  error,
  rawevent: {
    error,
    metadata,
    <strong>msg</strong>,
    parser_name
  }
}</pre></td>
<td><pre>{
  error,
  rawevent: {
    error,
    metadata,
    parser_name,
    <strong>rawevent</strong>
  }
}</pre></td>
<td><pre>{
  error,
  rawevent: {
    error,
    metadata,
    <strong>msg</strong>,
    parser_name
  }
}</pre></td>
<td><pre>{
  error,
  metadata,
  <strong>msg</strong>,
  parser_name
}</pre></td>
</tr>
<tr>
<td><strong>S3</strong></td>
<td><pre>{
  error,
  rawevent: {
    error,
    metadata,
    <strong>msg</strong>,
    parser_name
  }
}</pre></td>
<td><pre>{
  error,
  rawevent: {
    error,
    metadata,
    parser_name,
    <strong>rawevent</strong>
  }
}</pre></td>
<td><pre>{
  error,
  rawevent: {
    error,
    metadata,
    <strong>msg</strong>,
    parser_name
  }
}</pre></td>
<td><pre>{
  error,
  metadata,
  <strong>msg</strong>,
  parser_name
}</pre><br><em>Requires <a href="#s3-dispenser-fix-applied">S3 dispenser fix</a> — pre-fix wrote literal <code>%{[event][message]}</code></em></td>
</tr>
</tbody>
</table>

After the S3 dispenser fix, **S3 Schemaless** matches the Azure Schemaless cell.

### Kafka message published (before dispenser writes to file)

**Rows** = global destination topic; **columns** = normalization type. Cells show the **Kafka message value** structure. Vector-based types (OutOfBox, Custom) serialize the whole Vector event; Schemaless publishes a flat body with routing fields in **Kafka headers** (shown below each Schemaless cell).

<table>
<thead>
<tr>
<th></th>
<th>OutOfBox (old)</th>
<th>OutOfBox (new)</th>
<th>Custom</th>
<th>Schemaless</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Azure</strong><br></td>
<td><pre>{
  headers: {
    db_glob_path: "unparsed",
    db_tenant_id,
    db_source_name,
    destination_id,
    ...
  },
  message: {
    error,
    rawevent: {
      error,
      metadata,
      <strong>msg</strong>,
      parser_name
    }
  }
}</pre></td>
<td><pre>{
  headers: {
    db_glob_path: "unparsed",
    db_tenant_id,
    db_source_name,
    destination_id,
    ...
  },
  message: {
    error,
    rawevent: {
      error,
      metadata,
      parser_name,
      <strong>rawevent</strong>
    }
  }
}</pre></td>
<td><pre>{
  headers: {
    db_glob_path: "unparsed",
    db_tenant_id,
    db_source_name,
    destination_id,
    ...
  },
  message: {
    error,
    rawevent: {
      error,
      metadata,
      <strong>msg</strong>,
      parser_name
    }
  }
}</pre></td>
<td><pre>Kafka body:
{
  error,
  metadata,
  <strong>msg</strong>,
  parser_name
}

Kafka headers (separate):
{
  db_glob_path: "unparsed",
  db_tenant_id,
  db_source_name,
  db_event_source_id,
  ...
}</pre></td>
</tr>
<tr>
<td><strong>S3</strong><br></td>
<td><pre>{
  headers: {
    db_glob_path: "unparsed",
    db_tenant_id,
    db_source_name,
    destination_id,
    ...
  },
  message: {
    error,
    rawevent: {
      error,
      metadata,
      <strong>msg</strong>,
      parser_name
    }
  }
}</pre></td>
<td><pre>{
  headers: {
    db_glob_path: "unparsed",
    db_tenant_id,
    db_source_name,
    destination_id,
    ...
  },
  message: {
    error,
    rawevent: {
      error,
      metadata,
      parser_name,
      <strong>rawevent</strong>
    }
  }
}</pre></td>
<td><pre>{
  headers: {
    db_glob_path: "unparsed",
    db_tenant_id,
    db_source_name,
    destination_id,
    ...
  },
  message: {
    error,
    rawevent: {
      error,
      metadata,
      <strong>msg</strong>,
      parser_name
    }
  }
}</pre></td>
<td><pre>Kafka body:
{
  error,
  metadata,
  <strong>msg</strong>,
  parser_name
}

Kafka headers (separate):
{
  db_glob_path: "unparsed",
  db_tenant_id,
  db_source_name,
  db_event_source_id,
  ...
}</pre></td>
</tr>
</tbody>
</table>

OutOfBox / Custom: entire object above is the Kafka **message value** (`encoding.codec: json` on the Vector event). Schemaless: only the flat **Kafka body** block is the message value; headers are attached separately by the Go producer.

### S3 dispenser fix (applied) {#s3-dispenser-fix-applied}

**Repository:** `globaldestination-s3-dispenser`

| Item | Location |
|------|----------|
| Fix | `manifests/destination.tpl` — unparsed `else` branch (`db_glob_path != "undelivered"`) |
| Prerequisite | `pipeline/logstash.conf` — `decorate_events => extended` (provides `[event][original]`) |

**Root cause:** The old unparsed branch always used `output_message => "%{[event][message]}"`. Schemaless Kafka bodies have `msg`, not `message`, so Logstash wrote the literal string `%{[event][message]}` to S3.

**Fix:** After `json { source => "message" target => "event" }`, branch on parsed shape:

<pre>if ("_jsonparsefailure" in [tags]) {
    output_message = [event][original]          # parse failure fallback
} else if [event][message] {
    output_message = [event][message]           # Vector: OutOfBox / Custom
} else {
    output_message = [event][original]          # Schemaless flat JSON
}</pre>

Full template (`manifests/destination.tpl`):

<pre># UNPARSED (db_glob_path != "undelivered"):
#   Vector (Custom/OutOfBox) Kafka body has top-level "message" -> use [event][message]
#   Schemaless Kafka body is flat JSON (field "msg", not "message") -> use [event][original]
json {
    source => "message"
    target => "event"
}
if ("_jsonparsefailure" in [tags]) {
    mutate { id => "handle_unparsed_events_parse_failure_{{.ID}}"
             add_field => { "output_message" => "%{[event][original]}" } }
} else if [event][message] {
    mutate { id => "handle_unparsed_events_vector_{{.ID}}"
             add_field => { "output_message" => "%{[event][message]}" } }
} else {
    mutate { id => "handle_unparsed_events_schemaless_{{.ID}}"
             add_field => { "output_message" => "%{[event][original]}" } }
}</pre>

**Deploy / verify:**

1. Merge / redeploy `globaldestination-s3-dispenser`.
2. Confirm rendered per-tenant `.conf` contains `handle_unparsed_events_schemaless_` mutate block.
3. Send Schemaless unparsed test event; S3 line should be full flat JSON (not `%{[event][message]}`).

> **Does not fix** lines already on S3 written as `%{[event][message]}` — UNPARSED replay rejects those (`unparsed_event.go`).

### Dispenser transform (Kafka → stored file)

How each destination maps Kafka input to the stored file line. Azure uses Vector unwrap; S3 uses Logstash branching above.

<table>
<thead>
<tr>
<th>Global destination ↓ / Normalization →</th>
<th>OutOfBox (old / new)</th>
<th>Custom</th>
<th>Schemaless</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Azure</strong></td>
<td colspan="2"><pre>Kafka in:  { headers, message: { error, rawevent }, topic }
Transform: parse JSON → .message = messagejson.message
Stored:    { error, rawevent: { … } }</pre></td>
<td><pre>Kafka in:  { error, metadata, msg, parser_name }
Transform: no unwrap (no top-level "message" key)
Stored:    { error, metadata, msg, parser_name }</pre></td>
</tr>
<tr>
<td><strong>S3</strong><br><em>(with fix)</em></td>
<td colspan="2"><pre>Kafka in:  { headers, message: { error, rawevent }, topic }
Transform: json filter → [event][message] present
           output_message = [event][message]
Stored:    { error, rawevent: { … } }</pre></td>
<td><pre>Kafka in:  { error, metadata, msg, parser_name }
Transform: json filter → [event][message] absent
           output_message = [event][original]
Stored:    { error, metadata, msg, parser_name }

Pre-fix:   literal "%{[event][message]}" (broken)</pre></td>
</tr>
</tbody>
</table>

### Replay extraction by normalization type

| Normalization | Stored shape | Field replay extracts |
|---|---|---|
| OutOfBox (old) | nested envelope | `rawevent.msg` |
| OutOfBox (new) | nested envelope | `rawevent.rawevent` |
| Custom | nested envelope | `rawevent.msg` |
| Schemaless | flat JSON | top-level `msg` |
