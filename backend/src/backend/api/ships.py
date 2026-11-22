from typing import Any

from fastapi import APIRouter

from backend.api import snapshots
from backend.api.snapshots import RedisDep, Timestamps

router = APIRouter(prefix="/ships")

KIND = "ships"


def to_feature(ship: dict[str, Any]) -> dict[str, Any]:
    return snapshots.point(ship["longitude"], ship["latitude"], {
        "mmsi": ship["mmsi"],
        "name": ship.get("ship_name") or str(ship["mmsi"]),
        "speed": ship.get("speed_knots", 0),
        "heading": ship.get("heading"),
        "navStatus": ship.get("nav_status"),
    })


@router.get("/timestamps", response_model=Timestamps)
def get_timestamps(r: RedisDep) -> Timestamps:
    return snapshots.timestamps(r, KIND)


@router.get("/latest")
def get_latest(r: RedisDep) -> dict[str, Any]:
    return snapshots.latest(r, KIND, to_feature)


@router.get("/{timestamp}")
def get_at(timestamp: int, r: RedisDep) -> dict[str, Any]:
    """Ships at `timestamp`, or the closest earlier 5-minute snapshot."""
    return snapshots.at(r, KIND, timestamp, to_feature)
