#!/bin/bash
# Starts HiveServer2, (re)creates the external tables from tables.sql, then registers new
# partitions written by Spark every HIVE_SYNC_INTERVAL seconds (MSCK REPAIR ... SYNC PARTITIONS).
#
# The metastore is embedded (Derby) and holds no state worth keeping: every table is EXTERNAL and is
# recreated from tables.sql on start, the data itself lives on HDFS.
set -u

BEELINE="beeline -u jdbc:hive2://localhost:10000/ -n hive --silent=true --showWarnings=false"
INTERVAL="${HIVE_SYNC_INTERVAL:-300}"

/entrypoint.sh &
HS2=$!

echo "start-hive: waiting for HiveServer2"
until HADOOP_CLIENT_OPTS="-Xmx256m" $BEELINE -e "SELECT 1;" >/dev/null 2>&1; do
  kill -0 "$HS2" 2>/dev/null || { echo "start-hive: HiveServer2 exited"; exit 1; }
  sleep 5
done

echo "start-hive: creating tables"
HADOOP_CLIENT_OPTS="-Xmx256m" $BEELINE -f /hive/tables.sql

(
  while kill -0 "$HS2" 2>/dev/null; do
    # --force: a failing table (e.g. a source without data yet) must not block the others
    HADOOP_CLIENT_OPTS="-Xmx256m" $BEELINE --force=true -f /hive/repair.sql >/dev/null 2>&1 \
      && echo "start-hive: partitions synced $(date -u +%FT%TZ)" \
      || echo "start-hive: partition sync failed $(date -u +%FT%TZ)"
    sleep "$INTERVAL"
  done
) &

wait "$HS2"
