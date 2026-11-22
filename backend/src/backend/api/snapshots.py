"""Shared reading of Redis snapshots written by the Spark jobs.

Layout per kind (e.g. "planes", "ships"):
  <kind>:snaps         sorted set of snapshot timestamps (Unix seconds)
  <kind>:snap:<ts>     hash, one JSON value per object
"""

import json
from collections.abc import Callable
from functools import lru_cache
from typing import Annotated, Any

import redis
from fastapi import Depends
from pydantic import BaseModel

from backend.settings import get_settings

ToFeature = Callable[[dict[str, Any]], dict[str, Any]]


@lru_cache
def get_redis() -> redis.Redis:
    settings = get_settings()
    return redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)


RedisDep = Annotated[redis.Redis, Depends(get_redis)]


class Timestamps(BaseModel):
    timestamps: list[int]


def point(lon: float, lat: float, properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": properties}


def timestamps(r: redis.Redis, kind: str) -> Timestamps:
    return Timestamps(timestamps=[int(t) for t in r.zrange(f"{kind}:snaps", 0, -1)])


def latest(r: redis.Redis, kind: str, to_feature: ToFeature) -> dict[str, Any]:
    found = r.zrange(f"{kind}:snaps", -1, -1)
    return _collection(r, kind, int(found[0]) if found else None, to_feature)


def at(r: redis.Redis, kind: str, timestamp: int, to_feature: ToFeature) -> dict[str, Any]:
    """The snapshot at `timestamp`, or the closest earlier one."""
    found = r.zrevrangebyscore(f"{kind}:snaps", timestamp, "-inf", start=0, num=1)
    return _collection(r, kind, int(found[0]) if found else None, to_feature)


def _collection(r: redis.Redis, kind: str, ts: int | None, to_feature: ToFeature) -> dict[str, Any]:
    features = []
    if ts is not None:
        features = [to_feature(json.loads(raw)) for raw in r.hvals(f"{kind}:snap:{ts}")]
    return {"type": "FeatureCollection", "time": ts, "features": features}
