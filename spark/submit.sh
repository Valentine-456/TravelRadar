#!/bin/sh
# ./submit.sh <job.py>      run in the foreground (Ctrl+C stops it)
# ./submit.sh -d <job.py>   run in the background, logs in spark/logs/<job>.log
#                           stop it at http://localhost:8080 -> Running Applications -> (kill)
set -e

exec_flags=""
if [ "$1" = "-d" ]; then exec_flags="-d"; shift; fi
job="$1"

docker compose exec $exec_flags spark-master sh -c "mkdir -p /opt/spark/apps/logs && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 512m \
    --executor-memory 768m \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /opt/spark/apps/jobs/$job ${exec_flags:+> /opt/spark/apps/logs/${job%.py}.log 2>&1}"
