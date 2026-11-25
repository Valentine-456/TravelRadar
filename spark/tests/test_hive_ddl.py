from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

import backfill_job
import batch_views_job
from conftest import fixture_json
from test_batch import _archive_opensky
from travelradar import hive_ddl

HIVE_DIR = Path("/opt/spark/hive")   # ../hive, mounted by test.sh


@pytest.mark.skipif(not HIVE_DIR.exists(), reason="hive/ not mounted")
def test_committed_ddl_is_up_to_date(spark, actual):
    """FT-HV-01: Hive table definitions match the Parquet written by Spark
    Objective: Hive reads Parquet columns by name; a column renamed in a parser without updating the DDL would silently read as NULL.
    Steps: Regenerate the DDL from the current parsers (hive_ddl.tables_sql / repair_sql) and compare with the committed hive/tables.sql and hive/repair.sql.
    Expected: identical files (otherwise run spark/gen_hive_ddl.sh)."""
    assert (HIVE_DIR / "tables.sql").read_text() == hive_ddl.tables_sql(spark), \
        "hive/tables.sql is outdated: run spark/gen_hive_ddl.sh"
    assert (HIVE_DIR / "repair.sql").read_text() == hive_ddl.repair_sql(spark)
    actual(f"{len(hive_ddl.tables(spark))} tables up to date")


def test_view_columns_match_view_output(spark, hdfs_root, tmp_path, actual):
    """FT-HV-02: Hive view tables match the batch views' output
    Objective: The view DDL is written by hand; check it against the columns the batch views really produce.
    Steps: Produce raw, cleaned and rail history data, compute every batch view and compare its columns and types (without dt) with hive_ddl.VIEW_COLUMNS.
    Expected: identical (name, type) lists for all 4 views."""
    _archive_opensky(spark, tmp_path)
    trains = fixture_json("dt_train_1.json")
    f = backfill_job.rail_day_frame(spark, trains)
    backfill_job.write_day(f, backfill_job.rail_cleaned(f), "digitraffic_trains_archive",
                           "train_stop_events_history", date(2026, 11, 24))
    checked = []
    for name, fn in batch_views_job.VIEWS.items():
        df = fn(spark, None)
        got = sorted((c.name, c.dataType.simpleString()) for c in df.schema if c.name != "dt")
        assert got == sorted(hive_ddl.VIEW_COLUMNS[name]), name
        checked.append(name)
    actual(f"checked {checked}")
