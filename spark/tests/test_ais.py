from __future__ import annotations

import json
from datetime import datetime

from conftest import fixture_lines, frame, rows
from travelradar.sources import ais


def report(mmsi=265520430, name="VAXO   ", t="2026-11-22 01:03:43.792353774 +0000 UTC", lat=59.33,
           lon=18.08, sog=0.1, cog=171.6, heading=216, status=5, rot=0, msg_type="PositionReport"):
    return json.dumps({
        "MessageType": msg_type,
        "MetaData": {"MMSI": mmsi, "ShipName": name, "time_utc": t},
        "Message": {"PositionReport": {"Latitude": lat, "Longitude": lon, "Sog": sog, "Cog": cog,
                                       "TrueHeading": heading, "NavigationalStatus": status,
                                       "RateOfTurn": rot, "PositionAccuracy": True}},
    })


def test_maps_position_reports(spark, actual):
    """FT-AIS-01: AIS position reports are mapped to typed columns
    Objective: Check the AIS parser on messages captured from AISStream.
    Steps: 1) Load the 5 captured WebSocket messages (1 subscription confirmation, 4 position reports). 2) Run ais.parse.
    Expected: 4 rows; the confirmation is dropped; names are trimmed; event_time comes from MetaData.time_utc with sub-second precision."""
    out = rows(ais.parse(frame(spark, fixture_lines("ais.jsonl"))))
    assert len(out) == 4
    vaxo = next(r for r in out if r["mmsi"] == 265520430)
    assert vaxo["ship_name"] == "VAXO"
    assert vaxo["event_time"] == datetime(2026, 11, 22, 1, 3, 43, 792353)
    assert vaxo["heading"] == 216.0
    assert vaxo["dt"] == "2026-11-22"
    actual(f"{len(out)} rows; VAXO event_time={vaxo['event_time']} heading={vaxo['heading']}")


def test_sentinel_values_become_null(spark, actual):
    """QT-AIS-02: AIS 'not available' sentinels are converted to null
    Objective: AIS encodes missing values as magic numbers (ITU-R M.1371); they must not reach analytics as real values.
    Steps: Parse a report with SOG 102.3, COG 360, TrueHeading 511 and RateOfTurn -128; and one with only TrueHeading 511.
    Expected: speed, course, true_heading and rate_of_turn are null; in the second report heading falls back to course over ground."""
    out = rows(ais.parse(frame(spark, [
        report(mmsi=111111111, sog=102.3, cog=360.0, heading=511, rot=-128),
        report(mmsi=222222222, cog=171.6, heading=511),
    ])))
    a, b = sorted(out, key=lambda r: r["mmsi"])
    assert a["speed_knots"] is None and a["course"] is None
    assert a["true_heading"] is None and a["heading"] is None and a["rate_of_turn"] is None
    assert b["heading"] == 171.6
    actual(f"sentinels -> {a['speed_knots']}, {a['course']}, {a['true_heading']}, {a['rate_of_turn']}; "
           f"heading fallback -> {b['heading']}")


def test_invalid_reports_dropped(spark, actual):
    """QT-AIS-03: Invalid AIS messages are rejected
    Objective: Messages without a usable identity, position or time are removed.
    Steps: Parse one valid report plus: latitude 91 (not available), longitude 181, MMSI with 5 digits, other message type, unparsable time.
    Expected: Only the valid report remains (1 of 6)."""
    out = rows(ais.parse(frame(spark, [
        report(), report(lat=91.0), report(lon=181.0), report(mmsi=12345),
        report(msg_type="ShipStaticData"), report(t="yesterday"),
    ])))
    assert len(out) == 1
    actual(f"{len(out)} of 6 messages kept")


def test_time_without_fraction(spark, actual):
    """FT-AIS-04: Timestamps without fractional seconds are accepted
    Objective: AISStream does not always send sub-second digits; such reports must not be lost.
    Steps: Parse a report with time_utc '2026-11-22 01:03:43 +0000 UTC'.
    Expected: event_time = 2026-11-22 01:03:43."""
    out = rows(ais.parse(frame(spark, [report(t="2026-11-22 01:03:43 +0000 UTC")])))
    assert out[0]["event_time"] == datetime(2026, 11, 22, 1, 3, 43)
    actual(str(out[0]["event_time"]))
