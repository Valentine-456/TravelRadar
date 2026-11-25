#!/bin/sh
# Spark unit tests, run in the cluster's Spark image (no running stack needed):
#   ./test.sh                 all tests
#   ./test.sh -k opensky      pytest arguments are passed through
# JUnit report: spark/tests/reports/spark.xml (input for tests/make_report.py)
set -e
cd "$(dirname "$0")/.."
docker compose build -q spark-master
docker compose run --rm --no-deps -T --entrypoint "" -v "$(pwd)/hive:/opt/spark/hive:ro" spark-master \
  python3 -m pytest /opt/spark/apps/tests -p no:cacheprovider -q \
  --junitxml=/opt/spark/apps/tests/reports/spark.xml -o junit_family=xunit1 "$@"
