from __future__ import annotations

import json
from datetime import datetime

import pytest

from conftest import fixture_json, fixture_lines, frame
from travelradar import quality
from travelradar.sources import ALL


def test_evaluate_counts_each_violation(spark, actual):
    """QT-QC-01: Quality checks count each type of violation
    Objective: Verify the quality-check engine itself before trusting its reports.
    Steps: Evaluate rules not_null, range, regex, allowed, max_lag_seconds and unique on 5 hand-made rows with exactly one violation of each kind.
    Expected: failed = 1 for every check, total = 5."""
    t = datetime(2026, 11, 25, 12, 0, 0)
    rows = [  # id, speed, code, mode, event_time, ingest_time
        ("a", 10.0, "AB1", "BUS", t, t),
        (None, 10.0, "AB2", "BUS", t, t),                                   # null id
        ("c", 999.0, "AB3", "BUS", t, t),                                   # speed out of range
        ("d", 10.0, "zz", "SPACESHIP", datetime(2026, 11, 25, 11, 0), t),   # regex, allowed, stale
        ("a", 10.0, "AB1", "BUS", datetime(2026, 11, 25, 12, 5), t),       # future, duplicate id
    ]
    df = spark.createDataFrame(rows, "id string, speed double, code string, mode string, "
                                     "event_time timestamp, ingest_time timestamp")
    rules = {"id": {"not_null": True}, "speed": {"range": (0, 100)}, "code": {"regex": "^[A-Z]{2}[0-9]$"},
             "mode": {"allowed": ["BUS", "TRAM"]}, "event_time": {"max_lag_seconds": 600}}
    results = {(r.column, r.check): r.failed for r in quality.evaluate(df, "t", rules, ("id", "code"))}
    assert results == {("id", "not_null"): 1, ("speed", "range"): 1, ("code", "regex"): 1,
                       ("mode", "allowed"): 1, ("event_time", "staleness"): 1,
                       ("event_time", "future"): 1, ("id,code", "unique"): 1}
    actual(results)


FIXTURE_VALUES = {
    "opensky": lambda: fixture_lines("opensky.jsonl"),
    "ais": lambda: fixture_lines("ais.jsonl"),
    "digitraffic_trains": lambda: [json.dumps(t) for t in fixture_json("dt_train_1.json")],
    "digitraffic_locations": lambda: [json.dumps(l) for l in fixture_json("dt_locations.json")],
    "entur": lambda: [json.dumps(v) for v in fixture_json("entur.json")["data"]["vehicles"]],
    "weather": lambda: [json.dumps(e) for e in fixture_json("openmeteo.json")],
}


@pytest.mark.parametrize("name", list(ALL))
def test_real_samples_pass_quality_rules(spark, name, actual):
    """QT-QC-02: Cleaned real samples satisfy the quality rules of their table
    Objective: The rules (value ranges, formats, allowed values, uniqueness) hold for real data captured from each source after cleaning.
    Steps: For each source: parse its captured sample, evaluate the table's quality rules except staleness (the samples are days old).
    Expected: 0 failing rows for every rule, for all six cleaned tables."""
    source = ALL[name]
    cleaned = source.parse(frame(spark, FIXTURE_VALUES[name]()))
    if source.dedup_keys:
        cleaned = cleaned.dropDuplicates(list(source.dedup_keys))
    rules = {c: {k: v for k, v in r.items() if k != "max_lag_seconds"}
             for c, r in source.quality_rules.items()}
    results = quality.evaluate(cleaned, source.table, rules, source.dedup_keys)
    bad = quality.failing(results)
    assert results[0].total > 0
    assert bad == [], [b.as_dict() for b in bad]
    actual(f"{source.table}: {results[0].total} rows, {len(results)} checks, 0 failures")
