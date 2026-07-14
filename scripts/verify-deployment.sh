#!/bin/bash

# Script to verify Cloud Run service revisions and deployment status

PROJECT_ID="data-etl-499916"
REGION="us-central1"

SERVICES=("ingest" "detect_bootstrap" "stream_parser" "load" "report" "retry")

echo "=== Cloud Run Service Deployment Status ==="
echo ""

for service in "${SERVICES[@]}"; do
  echo "Service: $service"
  echo "---"
  
  # Get service URL
  URL=$(gcloud run services describe $service \
    --project=$PROJECT_ID \
    --region=$REGION \
    --format='value(status.url)' 2>/dev/null)
  
  if [ -n "$URL" ]; then
    echo "URL: $URL"
    
    # Get latest revision
    LATEST_REVISION=$(gcloud run revisions list \
      --service=$service \
      --project=$PROJECT_ID \
      --region=$REGION \
      --limit=1 \
      --format='value(name)' 2>/dev/null)
    
    if [ -n "$LATEST_REVISION" ]; then
      echo "Latest Revision: $LATEST_REVISION"
      
      # Get revision creation time
      CREATED=$(gcloud run revisions describe $LATEST_REVISION \
        --project=$PROJECT_ID \
        --region=$REGION \
        --format='value(startTime)' 2>/dev/null)
      echo "Created: $CREATED"
      
      # Get revision status
      STATUS=$(gcloud run revisions describe $LATEST_REVISION \
        --project=$PROJECT_ID \
        --region=$REGION \
        --format='value(status)' 2>/dev/null)
      echo "Status: $STATUS"
    else
      echo "No revisions found"
    fi
    
    # Test health endpoint
    echo "Testing health endpoint..."
    HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" $URL/health 2>/dev/null)
    if [ "$HEALTH_STATUS" = "200" ]; then
      echo "Health Check: ✓ OK"
    else
      echo "Health Check: ✗ FAILED (HTTP $HEALTH_STATUS)"
    fi
  else
    echo "Service not deployed"
  fi
  
  echo ""
done

echo "=== Cloud Build History (Last 5) ==="
gcloud builds list \
  --project=$PROJECT_ID \
  --limit=5 \
  --format="table(id,startTime,status,duration,source.repoSource.repoName)"
