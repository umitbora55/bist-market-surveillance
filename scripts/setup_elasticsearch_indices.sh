#!/bin/bash

echo "Creating Elasticsearch indices for BIST Market Surveillance..."

ES_HOST="localhost:9200"

# Create trades index with proper mapping
curl -X PUT "$ES_HOST/bist-trades" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "symbol": { "type": "keyword" },
      "symbol_full": { "type": "keyword" },
      "name": { "type": "text" },
      "price": { "type": "float" },
      "volume": { "type": "integer" },
      "price_change_pct": { "type": "float" },
      "timestamp": { "type": "date" },
      "trade_id": { "type": "keyword" }
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0
  }
}'

echo -e "\n\n"

# Create alerts index
curl -X PUT "$ES_HOST/bist-alerts" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "alert_id": { "type": "keyword" },
      "symbol": { "type": "keyword" },
      "alert_type": { "type": "keyword" },
      "severity": { "type": "keyword" },
      "description": { "type": "text" },
      "price": { "type": "float" },
      "volume": { "type": "integer" },
      "threshold_value": { "type": "float" },
      "detected_at": { "type": "date" },
      "metadata": { "type": "object", "enabled": false }
    }
  },
  "settings": {
    "number_of_shards": 2,
    "number_of_replicas": 0
  }
}'

echo -e "\n\n"

# List all indices
echo "Listing all indices:"
curl -X GET "$ES_HOST/_cat/indices?v"

echo -e "\n\nElasticsearch setup complete!"
