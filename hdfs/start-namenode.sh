#!/bin/bash
# Formats the namenode on first start (empty volume), then runs it in the foreground.
set -euo pipefail

if [ ! -f /data/dfs/name/current/VERSION ]; then
  echo "Formatting new HDFS namenode"
  hdfs namenode -format -force -nonInteractive -clusterid travelradar
fi
exec hdfs namenode
