from __future__ import annotations

import json
from datetime import datetime

from conftest import fixture_lines, frame, rows
from travelradar.sources import opensky


def vector(icao="4caf28", callsign="RYR2KK  ", tpos=1795281484, last=1795281484, lon=6.289,
           lat=55.6628, baro=11277.6, ground=False, vel=255.92, track=99.25, geo=11010.9):
    return json.dumps([icao, callsign, "Ireland", tpos, last, lon, lat, baro, ground, vel, track, 0,
                       None, geo, "7675", False, 0])


def test_maps_state_vector_fields(spark, actual):
    """FT-OS-01: OpenSky state vector is mapped to typed columns
    Objective: Check that the OpenSky parser maps every array position of a state vector to the right column and type.
    Steps: 1) Load the 11 state vectors captured from the live topic (nifi/responses/opensky.txt). 2) Run opensky.parse.
    Expected: All 11 vectors are kept; icao24 is lower-case hex; callsigns are trimmed; latitude, longitude and event_time are set; dt is the UTC day of event_time (2026-11-21)."""
    out = rows(opensky.parse(frame(spark, fixture_lines("opensky.jsonl"))))
    assert len(out) == 11
    first = next(r for r in out if r["icao24"] == "4caf28")
    assert first["callsign"] == "RYR2KK"
    assert first["origin_country"] == "Ireland"
    assert (first["longitude"], first["latitude"]) == (6.289, 55.6628)
    assert first["altitude"] == 11010.9          # geometric altitude preferred
    assert first["event_time"] == datetime(2026, 11, 21, 17, 18, 4)
    assert first["dt"] == "2026-11-21"
    assert all(r["callsign"] == r["callsign"].strip() for r in out if r["callsign"])
    actual(f"{len(out)} of 11 vectors parsed; sample: {first['icao24']} {first['callsign']} "
           f"alt={first['altitude']} event_time={first['event_time']}")


def test_altitude_and_time_fallbacks(spark, actual):
    """FT-OS-02: Fallbacks for missing altitude and position time
    Objective: Check the documented fallbacks: barometric altitude when geometric altitude is missing, last_contact when time_position is missing.
    Steps: Parse a vector with geo_altitude = null and time_position = null.
    Expected: altitude equals baro_altitude; event_time equals last_contact."""
    out = rows(opensky.parse(frame(spark, [vector(geo=None, tpos=None, last=1795281300)])))
    assert out[0]["altitude"] == 11277.6
    assert out[0]["event_time"] == datetime.utcfromtimestamp(1795281300)
    actual(f"altitude={out[0]['altitude']}, event_time={out[0]['event_time']}")


def test_invalid_records_dropped(spark, actual):
    """QT-OS-03: Invalid OpenSky records are rejected
    Objective: Check that records which cannot be placed on the map are removed during cleaning.
    Steps: Parse one valid vector plus: no position, latitude 95, malformed icao24, truncated array, non-JSON text, no timestamps.
    Expected: Only the valid vector remains (1 of 7)."""
    bad = [
        vector(lat=None, lon=None),
        vector(lat=95.0),
        vector(icao="XYZ"),
        json.dumps(["4caf28", "RYR", "Ireland", 1, 1]),
        "not json",
        vector(tpos=None, last=None),
    ]
    out = rows(opensky.parse(frame(spark, [vector()] + bad)))
    assert len(out) == 1
    actual(f"{len(out)} of {len(bad) + 1} records kept")


def test_on_ground_flag(spark, actual):
    """FT-OS-04: on_ground flag is kept as boolean
    Objective: Planes on the ground must be distinguishable for later analysis (taxiing vs flying).
    Steps: Parse one vector with on_ground = true and one with false.
    Expected: on_ground is True and False respectively."""
    out = rows(opensky.parse(frame(spark, [vector(icao="000001", ground=True),
                                           vector(icao="000002", ground=False)])))
    flags = {r["icao24"]: r["on_ground"] for r in out}
    assert flags == {"000001": True, "000002": False}
    actual(str(flags))
