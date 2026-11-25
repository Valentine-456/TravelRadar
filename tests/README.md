# Tests

```bash
./run_tests.sh                 # from the repo root: all suites except integration
./run_tests.sh --integration   # + integration tests (stack up, flows deployed, pipeline running)
```

| Suite | Where | Needs | What |
| --- | --- | --- | --- |
| Spark unit | `spark/tests/` (`spark/test.sh`) | Docker only | parsers of all 6 sources, Redis hot path, HDFS archive (exactly-once restart), quality engine, backfills, batch views, Hive DDL |
| Backend API | `backend/tests/` (`uv run pytest`) | nothing | REST endpoints against an in-memory Redis |
| NiFi flows | `tests/test_nifi_flows.py` | nothing | topic wiring, retry on failure, API limits, bounding boxes, secrets, self-contained flows |
| Integration | `tests/test_integration.py` (`-m integration`) | running stack | Kafka, HDFS, Hive, NiFi, live ingestion, pipeline, Redis, isolated end-to-end replay |

The end-to-end test (IT-08) replays the captured samples through the real pipeline in an isolated
namespace (`it.*` topics, `it:*` Redis keys, `/travelradar-it` on HDFS) and removes it afterwards.

## Test documentation

Each test's docstring is `<ID>: <title>` followed by `Objective:`, `Steps:` and `Expected:`; the test
records what it observed with `actual(...)`. `make_report.py` turns the JUnit reports of all suites
into `tests/reports/test_cases.md` and `.csv` (one row per test case, with the actual result and
PASS/FAIL/SKIPPED), the input for the test document.

IDs: `FT-` functional, `QT-` quality, `IT-` integration; then the area (`OS` OpenSky, `AIS`, `TR`/`TL`
trains, `EN` Entur, `WE` weather, `RS` Redis, `AR` archive, `BF` backfill, `BV` batch views,
`QC` quality engine, `HV` Hive, `API`, `NF` NiFi).
