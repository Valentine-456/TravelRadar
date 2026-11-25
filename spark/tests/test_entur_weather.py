from __future__ import annotations

import json

from conftest import fixture_json, frame, rows
from travelradar.sources import entur, weather


def vehicles() -> list[dict]:
    return fixture_json("entur.json")["data"]["vehicles"]


def test_entur_vehicles(spark, actual):
    """FT-EN-01: Entur vehicles are mapped
    Objective: Check the Entur parser on a live GraphQL response (21 vehicles: ferries, buses, a tram).
    Steps: Parse every vehicle as one Kafka record.
    Expected: 21 rows; mode upper-case; speed converted from m/s to km/h; operator falls back to the codespace when operatorRef is empty."""
    vs = vehicles()
    out = rows(entur.parse(frame(spark, [json.dumps(v) for v in vs])))
    assert len(out) == len(vs) == 21
    ferry = next(r for r in out if r["vehicle_id"] == "259299000")
    assert ferry["mode"] == "FERRY" and ferry["operator"] == "ATB"   # operatorRef is ""
    with_speed = [(v, r) for v in vs for r in out if r["vehicle_id"] == v["vehicleId"] and v["speed"]]
    for v, r in with_speed:
        assert abs(r["speed_kmh"] - v["speed"] * 3.6) < 1e-9
    actual(f"{len(out)} vehicles, modes {sorted({r['mode'] for r in out})}, "
           f"{len(with_speed)} with speed")


def test_entur_delay_validity(spark, actual):
    """QT-EN-02: Unusable Entur delays are flagged
    Objective: PM1 found stale delays of completed trips and outliers of several hours; they must not be used as labels.
    Steps: Parse a vehicle with delay 120 s, one with status COMPLETED, one with delay 30,000 s, one without delay.
    Expected: delay_valid is true only for the first; the delay values themselves are kept."""
    base = vehicles()[1]
    cases = {"ok": dict(delay=120.0), "done": dict(delay=60.0, vehicleStatus="COMPLETED"),
             "outlier": dict(delay=30000.0), "none": dict(delay=None)}
    values = [json.dumps({**base, "vehicleId": k, **v}) for k, v in cases.items()]
    out = {r["vehicle_id"]: r for r in rows(entur.parse(frame(spark, values)))}
    assert {k: r["delay_valid"] for k, r in out.items()} == \
        {"ok": True, "done": False, "outlier": False, "none": False}
    assert out["outlier"]["delay_s"] == 30000.0
    actual({k: r["delay_valid"] for k, r in out.items()})


def test_entur_invalid_dropped(spark, actual):
    """QT-EN-03: Vehicles without time or position are rejected
    Objective: Rows that cannot be placed in space and time are removed.
    Steps: Parse a vehicle without lastUpdated and one without location.
    Expected: 0 rows."""
    base = vehicles()[0]
    values = [json.dumps({**base, "lastUpdated": None}), json.dumps({**base, "location": None})]
    assert rows(entur.parse(frame(spark, values))) == []
    actual("0 rows")


def test_weather_nearest_city(spark, actual):
    """FT-WE-01: Open-Meteo records are assigned to their city
    Objective: The response echoes the model grid point, not our city id; the parser must recover the city.
    Steps: Parse the live response for Helsinki and Oslo (2 records, grid coordinates 60.17/24.95 and 59.91/10.74).
    Expected: location_id helsinki (FI) and oslo (NO); event_time from current.time; temperature and wind present."""
    entries = fixture_json("openmeteo.json")
    out = {r["location_id"]: r for r in rows(weather.parse(frame(spark, [json.dumps(e) for e in entries])))}
    assert set(out) == {"helsinki", "oslo"}
    assert out["helsinki"]["country"] == "FI" and out["oslo"]["country"] == "NO"
    assert out["helsinki"]["temperature_c"] == entries[0]["current"]["temperature_2m"]
    assert out["helsinki"]["event_time"].strftime("%Y-%m-%dT%H:%M") == entries[0]["current"]["time"]
    actual({k: (str(r["event_time"]), r["temperature_c"]) for k, r in out.items()})
