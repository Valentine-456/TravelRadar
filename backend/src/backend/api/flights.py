from typing import Any

from fastapi import APIRouter

from backend.api import snapshots
from backend.api.snapshots import RedisDep, Timestamps

router = APIRouter(prefix="/flights")

KIND = "planes"


def to_feature(plane: dict[str, Any]) -> dict[str, Any]:
    return snapshots.point(plane["longitude"], plane["latitude"], {
        "icao24": plane["icao24"],
        "callsign": plane.get("callsign") or plane["icao24"],
        "country": plane.get("origin_country"),
        "track": plane.get("true_track", 0),
        "altitude": round(plane.get("altitude") or 0),
        "velocity": round(plane.get("velocity") or 0),
    })


@router.get("/timestamps", response_model=Timestamps)
def get_timestamps(r: RedisDep) -> Timestamps:
    return snapshots.timestamps(r, KIND)


@router.get("/latest")
def get_latest(r: RedisDep) -> dict[str, Any]:
    return snapshots.latest(r, KIND, to_feature)


@router.get("/{timestamp}")
def get_at(timestamp: int, r: RedisDep) -> dict[str, Any]:
    return snapshots.at(r, KIND, timestamp, to_feature)
