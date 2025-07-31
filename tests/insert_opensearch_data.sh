#!/bin/bash

# Configuration
OPENSEARCH_URL="https://localhost:9200"
INDEX_NAME="db_insights_sights_sourcehostname_f5e31bb8-af80-40d8-a0e4-16f12187e4e4"
DATA_FILE="opensearch_sample_data.json"

# Authentication required for HTTPS
OPENSEARCH_USER="admin"
OPENSEARCH_PASSWORD="!QAZ2wsx3edc4rfv5tgb6yhn7ujm8ik"
AUTH_PARAMS="-k -u $OPENSEARCH_USER:$OPENSEARCH_PASSWORD"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up OpenSearch sample data for device inventory...${NC}"

# Check if OpenSearch is running
echo "Checking OpenSearch connection..."
if ! curl -s ${AUTH_PARAMS} "$OPENSEARCH_URL" > /dev/null; then
    echo -e "${RED}Error: Cannot connect to OpenSearch at $OPENSEARCH_URL${NC}"
    echo "Please make sure your Docker OpenSearch container is running."
    echo "If using docker-compose, try: docker-compose up -d opensearch"
    exit 1
fi

echo -e "${GREEN}✓ OpenSearch is accessible${NC}"

# Create index with proper mapping if it doesn't exist
echo "Creating index with mapping..."
curl -X PUT "$OPENSEARCH_URL/$INDEX_NAME" ${AUTH_PARAMS} \
  -H 'Content-Type: application/json' \
  -d '{
    "mappings": {
      "properties": {
        "id": { "type": "keyword" },
        "key1": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
        "key2": { "type": "text" },
        "tenant_id": { "type": "keyword" },
        "min_time": { "type": "long" },
        "max_time": { "type": "long" },
        "source_id": { "type": "keyword" },
        "data_plane_id": { "type": "keyword" },
        "timestamp": { "type": "long" },
        "updated_at": { "type": "long" },
        "reputation": { "type": "keyword" },
        "hostname": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }
      }
    }
  }' 2>/dev/null

echo -e "\n${GREEN}✓ Index created/updated${NC}"

# Insert sample data using bulk API
echo "Inserting sample data..."
if [ ! -f "$DATA_FILE" ]; then
    echo -e "${RED}Error: Data file $DATA_FILE not found${NC}"
    exit 1
fi

RESPONSE=$(curl -s -X POST "$OPENSEARCH_URL/_bulk" ${AUTH_PARAMS} \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @"$DATA_FILE")

# Check for errors in the response
if echo "$RESPONSE" | grep -q '"errors":true'; then
    echo -e "${RED}Error occurred during bulk insert:${NC}"
    echo "$RESPONSE" | jq '.'
    exit 1
fi

echo -e "${GREEN}✓ Sample data inserted successfully${NC}"

# Verify the data
echo "Verifying inserted data..."
sleep 2  # Wait for indexing

COUNT=$(curl -s "$OPENSEARCH_URL/$INDEX_NAME/_count" ${AUTH_PARAMS} | jq '.count')
echo -e "${GREEN}✓ Total documents in index: $COUNT${NC}"

# Show sample search
echo -e "\n${YELLOW}Sample search for devices with 'normal' reputation:${NC}"
curl -s -X GET "$OPENSEARCH_URL/$INDEX_NAME/_search" ${AUTH_PARAMS} \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "term": {
        "reputation": "normal"
      }
    },
    "size": 3
  }' | jq '.hits.hits[] | {hostname: ._source.hostname, reputation: ._source.reputation, source_id: ._source.source_id}'

echo -e "\n${GREEN}✓ Setup complete!${NC}"
echo -e "${YELLOW}Index name: $INDEX_NAME${NC}"
echo -e "${YELLOW}Total documents: $COUNT${NC}"
echo -e "${YELLOW}OpenSearch URL: $OPENSEARCH_URL${NC}"

echo -e "\n${YELLOW}You can now test your Go application's device inventory alerts with this data.${NC}" 