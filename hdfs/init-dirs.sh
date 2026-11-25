#!/bin/bash
# Creates the TravelRadar directory tree on HDFS. Idempotent; run by the one-shot `hdfs-init` service:
#   docker compose run --rm hdfs-init
set -euo pipefail

echo "Waiting for HDFS (namenode out of safe mode, one live datanode)..."
for _ in $(seq 1 90); do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF \
     && [ "$(hdfs dfsadmin -report -live 2>/dev/null | grep -c '^Name:')" -ge 1 ]; then
    break
  fi
  sleep 2
done

DIRS=(
  /travelradar/raw            # bronze: Kafka records exactly as received (master dataset)
  /travelradar/cleaned        # silver: parsed, typed, validated events
  /travelradar/batch          # batch acquisitions (history backfills)
  /travelradar/views          # gold: batch views computed from raw + batch
  /travelradar/models         # MS3+: model artefacts
  /travelradar/checkpoints    # Spark streaming checkpoints
  /travelradar/warehouse      # Hive managed tables (unused: all our tables are external)
)
hdfs dfs -mkdir -p "${DIRS[@]}"
hdfs dfs -chmod -R 777 /travelradar
echo "HDFS tree ready:"
hdfs dfs -ls /travelradar
