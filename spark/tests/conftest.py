"""Unit tests for the Spark code. Run inside the Spark image (same Spark/Java/Python as the cluster):

    ./test.sh            # from spark/

Test documentation: every test's docstring has the form

    <ID>: <title>
    Objective: ...
    Steps: ...
    Expected: ...

and tests call `actual(...)` with what they observed. Both are stored as JUnit properties, from
which tests/make_report.py builds the test-case table of the test documentation.
"""
from __future__ import annotations

import inspect
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

# Point every path at a throw-away local folder *before* travelradar.config is imported.
os.environ.setdefault("TRAVELRADAR_ROOT", "file://" + tempfile.mkdtemp(prefix="travelradar-test-"))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,  # noqa: E402
                               StructType, TimestampType)

from travelradar import config  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"

KAFKA_SCHEMA = StructType([
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("timestamp", TimestampType()),
])


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    s = (SparkSession.builder.master("local[2]").appName("travelradar-tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         # the container mounts core-site.xml (fs.defaultFS = hdfs://namenode): tests stay local
         .config("spark.hadoop.fs.defaultFS", "file:///")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .config("spark.driver.memory", "1g")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def _document(request, record_property):
    """Store the docstring (objective / steps / expected) with the result."""
    record_property("doc", inspect.getdoc(request.function) or "")


@pytest.fixture
def actual(record_property):
    """actual("...") records the observed result for the test report (and prints it)."""
    def record(text) -> None:
        if isinstance(text, dict):
            text = {str(k): v for k, v in text.items()}
        text = text if isinstance(text, str) else json.dumps(text, default=str)
        print(text)
        record_property("actual", text)
    return record


@pytest.fixture
def hdfs_root(tmp_path, monkeypatch) -> Path:
    """A fresh local 'HDFS' root per test (raw, cleaned, batch, views, checkpoints)."""
    root = f"file://{tmp_path}"
    monkeypatch.setattr(config, "HDFS_ROOT", root)
    monkeypatch.setattr(config, "RAW_PATH", f"{root}/raw/events")
    monkeypatch.setattr(config, "CLEANED_ROOT", f"{root}/cleaned")
    monkeypatch.setattr(config, "BATCH_ROOT", f"{root}/batch")
    monkeypatch.setattr(config, "VIEWS_ROOT", f"{root}/views")
    monkeypatch.setattr(config, "CHECKPOINT_ROOT", f"{root}/checkpoints")
    return tmp_path


def fixture_lines(name: str) -> list[str]:
    return [l for l in (FIXTURES / name).read_text().splitlines() if l.strip()]


def fixture_json(name: str):
    return json.loads((FIXTURES / name).read_text())


def frame(spark: SparkSession, values: list[str],
          ingest_time: datetime = datetime(2026, 11, 25, 12, 0, 0)) -> DataFrame:
    """A Kafka frame (value, ingest_time) as the parse functions receive it."""
    schema = StructType([StructField("value", StringType()), StructField("ingest_time", TimestampType())])
    return spark.createDataFrame([(v, ingest_time) for v in values], schema)


def write_kafka_batch(directory: Path, topic: str, values: list[str], ingest_time: datetime,
                      first_offset: int = 0) -> None:
    """Append records shaped like Kafka source rows to a folder read by a JSON file stream."""
    directory.mkdir(parents=True, exist_ok=True)
    name = directory / f"batch-{first_offset:08d}.json"
    with open(name, "w") as f:
        for i, v in enumerate(values):
            f.write(json.dumps({"topic": topic, "partition": 0, "offset": first_offset + i,
                                "key": None, "value": v,
                                "timestamp": ingest_time.strftime("%Y-%m-%dT%H:%M:%S")}) + "\n")


def kafka_stream(spark: SparkSession, directory: Path) -> DataFrame:
    """A streaming DataFrame with the Kafka source's columns, fed from files instead of Kafka."""
    return spark.readStream.schema(KAFKA_SCHEMA).json(str(directory))


def rows(df: DataFrame) -> list[dict]:
    return [r.asDict(recursive=True) for r in df.collect()]
