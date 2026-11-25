from __future__ import annotations

import copy
import json
from datetime import datetime

from conftest import fixture_json, frame, rows
from travelradar.sources import digitraffic_locations, digitraffic_trains


def train_json(mutate=None) -> str:
    train = fixture_json("dt_train_1.json")[0]
    if mutate:
        train = copy.deepcopy(train)
        mutate(train)
    return json.dumps(train)


def test_train_exploded_into_commercial_stops(spark, actual):
    """FT-TR-01: A train message becomes one stop event per commercial stop
    Objective: Check that the full train object (MQTT / archive) is exploded into stop events and only passenger stops are kept.
    Steps: Parse train IC 1 of 2026-11-24 (Helsinki-Turku, 140 timetable rows of which 24 are commercial stops).
    Expected: 24 rows with train_id 2026-11-24_1, increasing stop_seq, first stop HKI DEPARTURE, dt = departure date."""
    out = sorted(rows(digitraffic_trains.parse(frame(spark, [train_json()]))), key=lambda r: r["stop_seq"])
    assert len(out) == 24
    assert {r["train_id"] for r in out} == {"2026-11-24_1"}
    assert (out[0]["station"], out[0]["stop_type"]) == ("HKI", "DEPARTURE")
    assert out[0]["train_category"] == "Long-distance" and out[0]["train_type"] == "IC"
    assert {r["dt"] for r in out} == {"2026-11-24"}
    actual(f"{len(out)} stop events, route {out[0]['station']} -> {out[-1]['station']}")


def test_delay_labels_and_causes(spark, actual):
    """FT-TR-02: Delay label, measured flag and delay causes
    Objective: The delay in minutes is the regression target; is_actual marks ground truth; causes explain delays.
    Steps: Parse train IC 1 and inspect the PAR departure (5 min late, cause category L) and the HKI departure.
    Expected: PAR: delay_min 5, is_actual true, cause_codes ['L']; HKI: delay_min 1, scheduled 04:54, actual 04:54:54."""
    out = rows(digitraffic_trains.parse(frame(spark, [train_json()])))
    par = next(r for r in out if r["station"] == "PAR" and r["stop_type"] == "DEPARTURE")
    hki = next(r for r in out if r["station"] == "HKI")
    assert (par["delay_min"], par["is_actual"], par["cause_codes"]) == (5, True, ["L"])
    assert hki["delay_min"] == 1
    assert hki["scheduled_time"] == datetime(2026, 11, 24, 4, 54)
    assert hki["actual_time"] == datetime(2026, 11, 24, 4, 54, 54)
    actual(f"PAR delay={par['delay_min']} causes={par['cause_codes']}; HKI delay={hki['delay_min']}")


def test_delay_derived_when_difference_missing(spark, actual):
    """FT-TR-03: Delay is derived when differenceInMinutes is missing
    Objective: Rows with only a live estimate may lack differenceInMinutes; the delay must then be computed from the times.
    Steps: Remove differenceInMinutes and actualTime from the first row and set liveEstimateTime 7 minutes after scheduledTime.
    Expected: delay_min = 7, is_actual = false, estimate_time set."""
    def mutate(t):
        row = t["timeTableRows"][0]
        row.pop("differenceInMinutes", None)
        row.pop("actualTime", None)
        row["liveEstimateTime"] = "2026-11-24T05:01:00.000Z"
    out = rows(digitraffic_trains.parse(frame(spark, [train_json(mutate)])))
    first = next(r for r in out if r["stop_seq"] == 0)
    assert (first["delay_min"], first["is_actual"]) == (7, False)
    assert first["estimate_time"] == datetime(2026, 11, 24, 5, 1)
    actual(f"delay_min={first['delay_min']} is_actual={first['is_actual']}")


def test_invalid_trains_dropped(spark, actual):
    """QT-TR-04: Messages that are not valid trains are rejected
    Objective: Malformed MQTT payloads must not produce stop events.
    Steps: Parse a train without trainNumber, a train without timetable rows, and text that is not JSON.
    Expected: 0 rows."""
    def no_number(t):
        del t["trainNumber"]

    def no_rows(t):
        t["timeTableRows"] = []
    out = rows(digitraffic_trains.parse(frame(spark, [train_json(no_number), train_json(no_rows), "{"])))
    assert out == []
    actual("0 rows")


def test_train_locations(spark, actual):
    """FT-TL-01: Train GPS positions are mapped
    Objective: Check the train-location parser on a live response of /train-locations/latest.
    Steps: Parse every element of the captured response (one MQTT message each).
    Expected: one row per element; longitude/latitude taken from GeoJSON order [lon, lat]; train_id = departureDate_trainNumber."""
    locations = fixture_json("dt_locations.json")
    out = rows(digitraffic_locations.parse(frame(spark, [json.dumps(l) for l in locations])))
    assert len(out) == len(locations)
    first = locations[0]
    r = next(x for x in out if x["train_number"] == first["trainNumber"])
    assert (r["longitude"], r["latitude"]) == tuple(first["location"]["coordinates"])
    assert r["train_id"] == f"{first['departureDate']}_{first['trainNumber']}"
    assert all(59 < x["latitude"] < 71 for x in out)
    actual(f"{len(out)} positions; {r['train_id']} at ({r['latitude']}, {r['longitude']})")
