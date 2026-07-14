# File Parsing Pipeline — Node.js/TypeScript Port

This is a Node.js/TypeScript port of the Python file-parsing pipeline.

## Services

- `job_service` — REST API (Express) and job-event consumer.
- `ingest` — source normalization, URL fetch, archive extraction, SSRF guard.
- `detect_bootstrap` — adaptive probing, encoding detection, seed template generation.
- `stream_parser` — streaming line classifier, Parquet writer, rubbish/DLQ writers.
- `ai_classifier` — Bedrock Claude integration with a mock fallback.
- `load` — bulk-loads Parquet parts into Postgres.
- `report` — per-file and batch rollup reports.
- `retry` — DLQ recovery for failed lines.

## Shared modules

- `shared/config.ts` — environment settings.
- `shared/db.ts` — Postgres pool and schema bootstrap.
- `shared/queueUtils.ts` — SQS send/receive/delete helpers.
- `shared/s3Utils.ts` — S3 client, range reads, streaming, presigned URLs.
- `shared/models/*` — TypeScript equivalents of Pydantic models.

## Scripts

- `npm run init:db` — create Postgres tables.
- `npm run setup:localstack` — create LocalStack buckets, SQS queues, DynamoDB table.
- `npm run test:upload` — upload a sample file to S3.

## Development

```bash
npm install
npm run build
npm run init:db
npm run setup:localstack
```

Run services in separate terminals:

```bash
npm run dev:job
npm run dev:ingest
npm run dev:detect
npm run dev:parse
npm run dev:ai
npm run dev:load
npm run dev:report
npm run dev:retry
```

## Notes

- Uses AWS SDK v3 with `AWS_ENDPOINT_URL` pointing at LocalStack for local development.
- Parquet I/O uses `@dsnp/parquetjs`.
- Archive extraction uses `node-stream-zip`, `tar`, `node-7z`, and `unrar-async`.
