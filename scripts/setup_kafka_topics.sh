#!/bin/bash

echo "Creating Kafka topics for BIST Market Surveillance..."

# Topic configurations
BOOTSTRAP_SERVER="localhost:9092"
PARTITIONS=6
REPLICATION_FACTOR=1

# Create topics
docker exec kafka kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic bist.trades.raw \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    --if-not-exists

docker exec kafka kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic bist.trades.enriched \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    --if-not-exists

docker exec kafka kafka-topics --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic bist.alerts \
    --partitions 3 \
    --replication-factor $REPLICATION_FACTOR \
    --if-not-exists

echo ""
echo "Listing all topics:"
docker exec kafka kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "Topic details:"
docker exec kafka kafka-topics --describe --bootstrap-server $BOOTSTRAP_SERVER
