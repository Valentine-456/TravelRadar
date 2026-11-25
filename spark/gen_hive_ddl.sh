#!/bin/sh
# Regenerates hive/tables.sql and hive/repair.sql from the Spark parsers (no running stack needed).
set -e
cd "$(dirname "$0")/.."
docker compose build -q spark-master
docker compose run --rm --no-deps -T --entrypoint "" -v "$(pwd)/hive:/opt/spark/hive" spark-master \
  python3 -m travelradar.hive_ddl /opt/spark/hive
