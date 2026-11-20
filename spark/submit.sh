#!/bin/sh
# Usage: ./submit.sh <job.py>  (from spark/jobs/) [args...]
set -e
job="$1"; shift
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  "/opt/spark/apps/jobs/$job" "$@"
