import json
from functools import lru_cache
from typing import Annotated, Any

import redis
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from backend.settings import get_settings

router = APIRouter(prefix="/flights")

SNAPSHOTS_KEY = "planes:snaps"


@lru_cache
def get_redis() -> redis.Redis:
    settings = get_settings()
    return redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)


RedisDep = Annotated[redis.Redis, Depends(get_redis)]


class FlightTimestamps(BaseModel):
    timestamps: list[int]


def to_feature(plane: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [plane["longitude"], plane["latitude"]]},
        "properties": {
            "icao24": plane["icao24"],
            "callsign": plane.get("callsign") or plane["icao24"],
            "country": plane.get("origin_country"),
            "track": plane.get("true_track", 0),
            "altitude": round(plane.get("altitude") or 0),
            "velocity": round(plane.get("velocity") or 0),
        },
    }


def snapshot(r: redis.Redis, timestamp: int | None) -> dict[str, Any]:
    features = []
    if timestamp is not None:
        planes = (json.loads(raw) for raw in r.hvals(f"planes:snap:{timestamp}"))
        features = [to_feature(p) for p in planes]
    return {"type": "FeatureCollection", "time": timestamp, "features": features}


@router.get("/timestamps", response_model=FlightTimestamps)
def get_timestamps(r: RedisDep) -> FlightTimestamps:
    """All minutes (Unix seconds) that have a snapshot, oldest first."""
    return FlightTimestamps(timestamps=[int(t) for t in r.zrange(SNAPSHOTS_KEY, 0, -1)])


@router.get("/latest")
def get_latest(r: RedisDep) -> dict[str, Any]:
    """The newest snapshot. `time` is its timestamp."""
    found = r.zrange(SNAPSHOTS_KEY, -1, -1)
    return snapshot(r, int(found[0]) if found else None)


@router.get("/{timestamp}")
def get_at(timestamp: int, r: RedisDep) -> dict[str, Any]:
    """The snapshot at `timestamp`, or the closest earlier one."""
    found = r.zrevrangebyscore(SNAPSHOTS_KEY, timestamp, "-inf", start=0, num=1)
    return snapshot(r, int(found[0]) if found else None)
