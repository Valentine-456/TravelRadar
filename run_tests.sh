#!/bin/sh
# Runs every test suite and builds the test-case table for the test documentation.
#   ./run_tests.sh                 unit + static tests (no running stack needed)
#   ./run_tests.sh --integration   also the integration tests (stack up, flows deployed, pipeline running)
# Result: tests/reports/test_cases.md / .csv  (+ JUnit XML per suite)
set -u
cd "$(dirname "$0")"
status=0
echo "== Spark unit tests (Spark image)";      spark/test.sh || status=1
echo "== Backend API tests";                   (cd backend && uv run pytest) || status=1
echo "== NiFi flow definition tests";          (cd tests && uv run pytest) || status=1
if [ "${1:-}" = "--integration" ]; then
  echo "== Integration tests (running stack)"
  (cd tests && uv run pytest -m integration --junitxml=reports/integration.xml) || status=1
fi
python3 tests/make_report.py
exit $status
