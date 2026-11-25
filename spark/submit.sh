#!/bin/sh
# ./submit.sh <job.py>      run in the foreground (Ctrl+C stops it)
# ./submit.sh -d <job.py>   run in the background, logs in spark/logs/<job>.log
#                           stop it at http://localhost:8080 -> Running Applications -> (kill)
set -e

exec_flags=""
if [ "$1" = "-d" ]; then exec_flags="-d"; shift; fi
job="$1"

packages_arg=""
case "$job" in
  opensky_job.py|aisstream_job.py|example_job.py)
    packages_arg="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ;;
esac

log_redirect=""
if [ -n "$exec_flags" ]; then
  log_redirect="> /opt/spark/apps/logs/${job%.py}.log 2>&1"
fi

docker compose exec $exec_flags spark-master sh -c "mkdir -p /opt/spark/apps/logs && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 512m \
    --executor-memory 512m \
    --conf spark.cores.max=1 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    $packages_arg \
    /opt/spark/apps/jobs/$job $log_redirect"
