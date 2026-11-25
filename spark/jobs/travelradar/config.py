"""Runtime settings, overridable through environment variables (tests point them at local paths)."""
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

HDFS_ROOT = os.environ.get("TRAVELRADAR_ROOT", "hdfs://namenode:9000/travelradar")
RAW_PATH = f"{HDFS_ROOT}/raw/events"            # bronze: every Kafka record, partitioned by topic and dt
CLEANED_ROOT = f"{HDFS_ROOT}/cleaned"           # silver: one folder per table, partitioned by dt
BATCH_ROOT = f"{HDFS_ROOT}/batch"               # history backfills (same schemas as raw / cleaned)
VIEWS_ROOT = f"{HDFS_ROOT}/views"               # gold: batch views
CHECKPOINT_ROOT = f"{HDFS_ROOT}/checkpoints"

# Namespacing for the end-to-end integration test: it reads `it.opensky` etc. and writes
# `it:planes:*` in Redis and a separate HDFS root, so it never touches production data.
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "")
REDIS_PREFIX = os.environ.get("REDIS_PREFIX", "")

# PM1 design decision 3: Parquet in 10-minute micro-batches avoids thousands of small files.
# "availableNow" processes everything currently in Kafka and stops (catch-up runs and tests).
def _interval(value: str) -> str:
    """Trigger interval: a Spark interval ("10 minutes"), "availableNow", or plain seconds ("120")."""
    return f"{value} seconds" if value.isdigit() else value


ARCHIVE_TRIGGER = _interval(os.environ.get("ARCHIVE_TRIGGER", "10 minutes"))
HOT_TRIGGER = _interval(os.environ.get("HOT_TRIGGER", "10 seconds"))
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "200000"))

HOT_RETENTION_SECONDS = 24 * 3600


def cleaned_path(table: str) -> str:
    return f"{CLEANED_ROOT}/{table}"


def checkpoint_path(name: str) -> str:
    return f"{CHECKPOINT_ROOT}/{name}"
