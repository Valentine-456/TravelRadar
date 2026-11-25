"""Shared HDFS locations for TravelRadar Spark jobs."""
import os

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
ROOT = f"{HDFS_URI}/travelradar"

CLEANED_FLIGHTS = f"{ROOT}/cleaned/flights"
CLEANED_SHIPS = f"{ROOT}/cleaned/ships"
SMOKE_PATH = f"{ROOT}/cleaned/_smoke"

# Checkpoints default to local disk so streaming jobs start without the warehouse
# profile. Set USE_HDFS_CHECKPOINTS=1 (with warehouse up) for durable checkpoints.
if os.environ.get("USE_HDFS_CHECKPOINTS", "").lower() in ("1", "true", "yes"):
    CHECKPOINT_OPENSKY = f"{ROOT}/checkpoints/opensky"
    CHECKPOINT_AISSTREAM = f"{ROOT}/checkpoints/aisstream"
else:
    CHECKPOINT_OPENSKY = "/tmp/checkpoints/opensky"
    CHECKPOINT_AISSTREAM = "/tmp/checkpoints/aisstream"
