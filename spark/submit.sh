#!/bin/sh
# ./submit.sh <job.py> [args]      run in the foreground (Ctrl+C stops it)
# ./submit.sh -d <job.py> [args]   run in the background, logs in spark/logs/<job>.log
#                                  stop it at http://localhost:8080 -> Running Applications -> (kill)
#
# Environment overrides: DRIVER_MEMORY (default 768m), EXECUTOR_MEMORY (1g), CORES (2).
set -e

exec_flags=""
if [ "$1" = "-d" ]; then exec_flags="-d"; shift; fi
job="$1"; shift
args="$*"

log_redirect=""
if [ -n "$exec_flags" ]; then
  log_redirect="> /opt/spark/apps/logs/${job%.py}.log 2>&1"
fi

# Forward pipeline settings (see jobs/travelradar/config.py) to the driver and the executors.
env_flags=""
conf_flags=""
for var in TRAVELRADAR_ROOT TOPIC_PREFIX REDIS_PREFIX ARCHIVE_TRIGGER HOT_TRIGGER; do
  val=$(printenv "$var" || true)
  if [ -n "$val" ]; then
    env_flags="$env_flags -e $var=$val"
    conf_flags="$conf_flags --conf spark.executorEnv.$var=$val"
  fi
done

docker compose exec $exec_flags $env_flags spark-master sh -c "mkdir -p /opt/spark/apps/logs && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory ${DRIVER_MEMORY:-768m} \
    --executor-memory ${EXECUTOR_MEMORY:-1g} \
    --conf spark.cores.max=${CORES:-2} \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/apps/jobs $conf_flags \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    /opt/spark/apps/jobs/$job $args $log_redirect"
