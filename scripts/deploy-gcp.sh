#!/bin/bash

# GCP Deployment Script for File Parsing Pipeline

set -e

PROJECT_ID="data-etl-499916"
REGION="us-central1"
DATA_BUCKET="datalead-osint"
DEPLOYMENT_BUCKET="datalead-osint"

echo "=== Deploying File Parsing Pipeline to GCP ==="

# Enable required APIs
echo "Enabling GCP APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  pubsub.googleapis.com \
  firestore.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  --project=${PROJECT_ID}

# Create Pub/Sub topics and subscriptions
echo "Creating Pub/Sub topics and subscriptions..."
for topic in ingest classify parse dlq load report job-events; do
  gcloud pubsub topics create ${topic} --project=${PROJECT_ID} || true
  gcloud pubsub subscriptions create ${topic}-sub \
    --topic=${topic} \
    --project=${PROJECT_ID} || true
done

# Build and deploy services
echo "Building and deploying services..."
for service in ingest detect_bootstrap stream_parser load report retry; do
  echo "Deploying ${service}..."
  
  gcloud builds submit \
    --tag gcr.io/${PROJECT_ID}/${service}:latest \
    --project=${PROJECT_ID} \
    --timeout=600s
  
  gcloud run deploy ${service} \
    --image=gcr.io/${PROJECT_ID}/${service}:latest \
    --platform=managed \
    --region=${REGION} \
    --allow-unauthenticated \
    --memory=512Mi \
    --cpu=1 \
    --timeout=3600s \
    --set-env-vars=DATA_BUCKET=${DATA_BUCKET} \
    --set-env-vars=QUEUE_BACKEND=pubsub \
    --set-env-vars=GCP_PROJECT_ID=${PROJECT_ID} \
    --set-env-vars=FIRESTORE_DATABASE_ID=file-parsing-db \
    --set-env-vars=TEMPLATE_COLLECTION=file-parsing-templates \
    --set-env-vars=BEDROCK_MODEL_ID=anthropic.claude-3-5-sonnet-20241022 \
    --set-env-vars=HEALTH_CHECK_PORT=8080 \
    --project=${PROJECT_ID}
done

echo "=== Deployment Complete ==="
echo "Services deployed:"
gcloud run services list --project=${PROJECT_ID} --region=${REGION}
