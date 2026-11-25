#!/bin/bash
# Bootstrap TravelRadar directory tree on HDFS.
# Usage (from repo root, with warehouse profile up):
#   docker compose exec namenode bash /init-dirs.sh
# Or via the one-shot hdfs-init service.
set -euo pipefail

HDFS_URI="${HDFS_URI:-hdfs://namenode:9000}"

echo "Waiting for NameNode at ${HDFS_URI}..."
for i in $(seq 1 60); do
  if hdfs dfs -test -d / 2>/dev/null || hdfs dfsadmin -safemode get 2>/dev/null | grep -qi 'OFF\|ON'; then
    break
  fi
  sleep 2
done

# Leave safemode if stuck (fresh single-node cluster)
hdfs dfsadmin -safemode leave 2>/dev/null || true

DIRS=(
  /travelradar
  /travelradar/raw
  /travelradar/cleaned
  /travelradar/cleaned/flights
  /travelradar/cleaned/ships
  /travelradar/cleaned/vehicles
  /travelradar/cleaned/weather
  /travelradar/batch
  /travelradar/models
  /travelradar/checkpoints
  /travelradar/checkpoints/opensky
  /travelradar/checkpoints/aisstream
)

for d in "${DIRS[@]}"; do
  hdfs dfs -mkdir -p "$d"
done

hdfs dfs -chmod -R 777 /travelradar
echo "HDFS tree ready:"
hdfs dfs -ls -R /travelradar | head -50
