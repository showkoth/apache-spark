#!/bin/bash
set -e

# Format NameNode if necessary
if [ ! -d "/hadoop/dfs/name/current" ]; then
  echo "Formatting HDFS NameNode..."
  hdfs namenode -format -force
fi

# Start HDFS
echo "Starting HDFS..."
start-dfs.sh

# Wait a few seconds to ensure HDFS is up
sleep 5

# Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /data/imdb
hdfs dfs -mkdir -p /data/warehouse

# Put data.tsv into HDFS at /data/imdb
echo "Adding data.tsv to HDFS..."
hdfs dfs -put -f /hadoop/dfs/name/data.tsv /data/imdb/data.tsv

echo "HDFS setup complete."

# Keep the container running
tail -f /dev/null
