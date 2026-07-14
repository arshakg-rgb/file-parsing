#!/bin/bash

# Server-side test script for deployed file parsing pipeline
# This script tests the deployed Cloud Run services

PROJECT_ID="data-etl-499916"
REGION="us-central1"

echo "=== Server-Side File Parsing Pipeline Test ==="
echo ""

# Get service URLs
INGEST_URL=$(gcloud run services describe ingest \
  --project=$PROJECT_ID \
  --region=$REGION \
  --format='value(status.url)')

echo "Testing deployed services..."
echo ""

# Test 1: Health Checks
echo "Test 1: Health Checks"
echo "---"
for service in ingest detect_bootstrap stream_parser load report retry; do
  URL=$(gcloud run services describe $service \
    --project=$PROJECT_ID \
    --region=$REGION \
    --format='value(status.url)')
  
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" $URL/health 2>/dev/null)
  if [ "$STATUS" = "200" ]; then
    echo "✓ $service: Health OK"
  else
    echo "✗ $service: Health FAILED (HTTP $STATUS)"
  fi
done
echo ""

# Test 2: Pub/Sub Topics Exist
echo "Test 2: Pub/Sub Topics"
echo "---"
for topic in ingest classify parse dlq load report job-events; do
  EXISTS=$(gcloud pubsub topics describe $topic \
    --project=$PROJECT_ID \
    --format='value(name)' 2>/dev/null)
  if [ -n "$EXISTS" ]; then
    echo "✓ Topic $topic exists"
  else
    echo "✗ Topic $topic missing"
  fi
done
echo ""

# Test 3: Firestore Database
echo "Test 3: Firestore Database"
echo "---"
FIRESTORE_DB=$(gcloud firestore databases list \
  --project=$PROJECT_ID \
  --format='value(name)' 2>/dev/null)
if [ -n "$FIRESTORE_DB" ]; then
  echo "✓ Firestore database exists: $FIRESTORE_DB"
else
  echo "✗ Firestore database not found"
fi
echo ""

# Test 4: Send Test Message to Ingest Queue
echo "Test 4: Ingest Queue Test"
echo "---"
# Create a simple test message
TEST_MESSAGE=$(cat <<EOF
{
  "job_id": "test-job-$(date +%s)",
  "s3_url": "gs://datalead-osint/test/sample.csv",
  "size": 1024,
  "field_spec": {
    "fields": [
      {"name": "id", "type": "string"},
      {"name": "name", "type": "string"}
    ]
  },
  "seed_template_ids": []
}
EOF
)

echo "Sending test message to ingest topic..."
gcloud pubsub topics publish ingest \
  --project=$PROJECT_ID \
  --message="$TEST_MESSAGE" 2>/dev/null

if [ $? -eq 0 ]; then
  echo "✓ Test message sent successfully"
  echo "  Job ID: test-job-$(date +%s)"
  echo "  Check logs to verify processing"
else
  echo "✗ Failed to send test message"
fi
echo ""

# Test 5: Check Recent Logs
echo "Test 5: Recent Logs (Last 5 entries from ingest service)"
echo "---"
gcloud run logs read ingest \
  --project=$PROJECT_ID \
  --region=$REGION \
  --limit=5 \
  --format='value(timestamp,severity,textMessage)' 2>/dev/null || echo "No logs available"
echo ""

echo "=== Test Complete ==="
echo ""
echo "To view logs in Grafana/Loki:"
echo "1. Go to https://logs-prod-021.grafana.net"
echo "2. Query: {service=\"ingest\"}"
echo ""
echo "To manually trigger a file parse:"
echo "1. Upload a file to gs://datalead-osint/"
echo "2. Send a message to the ingest topic with the file URL"
