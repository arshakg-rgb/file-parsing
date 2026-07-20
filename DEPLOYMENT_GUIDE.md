# Large File Extraction - Deployment Guide

This guide covers the manual deployment steps for the async archive entry extraction feature.

## Prerequisites

- GCP project: `data-etl-499916`
- Region: `us-central1`
- Service account: `datalead-osint@data-etl-499916.iam.gserviceaccount.com`

## Step 1: Create Pub/Sub Topic

Create the Pub/Sub topic for the archive entry queue:

```bash
gcloud pubsub topics create fpp-archive-entry \
  --project=data-etl-499916
```

Create a subscription for the consumer:

```bash
gcloud pubsub subscriptions create archive-entry-consumer-sub \
  --topic=fpp-archive-entry \
  --project=data-etl-499916 \
  --ack-deadline=7200s  # 2 hours to match Cloud Run Job timeout
```

Grant the service account Pub/Sub Subscriber role:

```bash
gcloud pubsub subscriptions add-iam-policy-binding archive-entry-consumer-sub \
  --member=serviceAccount:datalead-osint@data-etl-499916.iam.gserviceaccount.com \
  --role=roles/pubsub.subscriber \
  --project=data-etl-499916
```

## Step 2: Create Secret for Queue URL

Create the secret for the archive entry queue URL:

```bash
gcloud secrets create FILE_ARCHIVE_ENTRY_QUEUE_URL \
  --project=data-etl-499916
```

Add the secret value:

```bash
echo "pubsub://projects/data-etl-499916/topics/fpp-archive-entry" | \
  gcloud secrets versions add FILE_ARCHIVE_ENTRY_QUEUE_URL \
  --data-file=- \
  --project=data-etl-499916
```

## Step 3: Run Database Migration

Run the migration to create the `pending_archive_entries` table:

```bash
# From the project root
npm run migrate
```

Or manually run the migration script:

```bash
npx tsx src/scripts/migrate.ts
```

This will create the table with the following schema:
- `id` (VARCHAR(36) PRIMARY KEY)
- `job_id` (VARCHAR(36) NOT NULL)
- `entry_name` (TEXT NOT NULL)
- `entry_size` (BIGINT NOT NULL)
- `status` (VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')))
- `error` (TEXT)
- `created_at` (TIMESTAMPTZ NOT NULL DEFAULT NOW())
- `updated_at` (TIMESTAMPTZ NOT NULL DEFAULT NOW())

## Step 4: Deploy via Cloud Build

The cloudbuild.yaml has been updated with:
- Dockerfile.archive-entry build step
- Cloud Run Job deployment for archive-entry-consumer
- Config vars: ARCHIVE_ENTRY_QUEUE_URL, LARGE_FILE_THRESHOLD_BYTES (500MB)

Run the Cloud Build:

```bash
gcloud builds submit . \
  --config=cloudbuild.yaml \
  --project=data-etl-499916
```

This will:
1. Build and push the archive-entry-consumer Docker image
2. Deploy it as a Cloud Run Job (not Service) with 7200s timeout
3. Update the ingest service with the new config vars

## Step 5: Schedule Reconciler via Cloud Scheduler

Create a Cloud Scheduler job to run the reconciler every 30 minutes:

```bash
gcloud scheduler jobs create reconciler-sweep \
  --schedule="*/30 * * * *" \
  --time-zone="UTC" \
  --description="Sweep stuck jobs and stale archive entries" \
  --http-uri="https://ingest-<hash>-uc.a.run.app/reconcile" \
  --http-method=POST \
  --project=data-etl-499916 \
  --region=us-central1
```

Note: You'll need to add a reconciler endpoint to the ingest service or deploy the reconciler as a separate Cloud Run Job/Function. The current reconciler.ts is a standalone script that can be run via Cloud Scheduler using Cloud Run Jobs.

Alternative: Deploy reconciler as a Cloud Run Job:

```bash
gcloud run jobs deploy reconciler \
  --image us-central1-docker.pkg.dev/data-etl-499916/file-parsing/job-service:latest \
  --region us-central1 \
  --platform managed \
  --service-account=datalead-osint@data-etl-499916.iam.gserviceaccount.com \
  --add-cloudsql-instances=data-etl-499916:us-central1:datalead-osint \
  --memory 512Mi \
  --cpu 1 \
  --timeout 1800s \
  --command="node dist/scripts/reconciler.js" \
  --set-env-vars QUEUE_BACKEND=pubsub,GCP_PROJECT_ID=data-etl-499916 \
  --update-secrets=FILE_DATABASE_URL=FILE_DATABASE_URL:latest
```

Then schedule it:

```bash
gcloud scheduler jobs create reconciler-job \
  --schedule="*/30 * * * *" \
  --time-zone="UTC" \
  --description="Reconciler sweep for stuck jobs" \
  --run-job="reconciler" \
  --project=data-etl-499916 \
  --region=us-central1
```

## Step 6: Verify Deployment

1. Check Cloud Run Job is deployed:
```bash
gcloud run jobs describe archive-entry-consumer \
  --region=us-central1 \
  --project=data-etl-499916
```

2. Check Pub/Sub topic exists:
```bash
gcloud pubsub topics describe fpp-archive-entry \
  --project=data-etl-499916
```

3. Check database table exists:
```bash
# Connect to your database and verify
\d pending_archive_entries
```

## Testing

Test with a 3.2GB RAR file to verify:
- File routes to async queue
- Consumer picks up the message
- Extraction completes without OOM
- Job transitions INGESTING → DONE

Test with an archive containing two files >500MB to verify the concurrency path.
