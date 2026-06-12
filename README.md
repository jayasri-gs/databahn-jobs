# Databahn Jobs
Home of the background Jobs.

## Replay job

Replay reads files from object storage and publishes lines to Kafka. How each line is transformed depends on `replay_type` (UNPARSED, CUSTOM, UNDELIVERED) and normalization source format.

See **[docs/replay-event-extraction.md](docs/replay-event-extraction.md)** for format comparison, sample payloads (Schemaless, Custom, OutOfBox old/new), and extraction rules.

## Insights Aggregation Job 
The Insights Aggregation Job is responsible for aggregating the staging insights data from temporary staging store to final insights store. 
