from __future__ import annotations

import json
import time
from collections import namedtuple
from datetime import datetime

import fakeredis

from conftest import frame, kafka_stream, write_kafka_batch
from test_ais import report
from test_opensky import vector
from travelradar import hot
from travelradar.sources import ais, opensky

Row = namedtuple("Row", "bucket id data")


def test_flight_snapshot_one_row_per_minute_and_aircraft(spark, actual):
    """FT-RS-01: Flight snapshots keep the newest fix per aircraft and minute
    Objective: The map shows one icon per aircraft per minute.
    Steps: Parse 3 vectors of the same aircraft received in the same minute (fix times t, t+10, t+20) and one vector without true_track; build snapshot rows.
    Expected: 1 row for the aircraft, holding the fix t+20; the vector without track is excluded (the map needs it to rotate the icon)."""
    t = 1795281400
    values = [vector(tpos=t + d, last=t + d, lon=6.0 + d / 100) for d in (0, 20, 10)]
    values.append(vector(icao="aaaaaa", track=None))
    cleaned = opensky.parse(frame(spark, values, datetime(2026, 11, 22, 1, 17, 30)))
    snaps = hot.flight_snapshot_rows(cleaned).collect()
    assert len(snaps) == 1
    data = json.loads(snaps[0]["data"])
    assert data["longitude"] == 6.2
    assert snaps[0]["bucket"] == int(datetime(2026, 11, 22, 1, 17).timestamp())
    actual(f"{len(snaps)} snapshot row; newest longitude {data['longitude']}")


def test_ship_snapshot_five_minute_buckets(spark, actual):
    """FT-RS-02: Ship snapshots use 5-minute buckets
    Objective: Ships move slowly, so the map keeps one position per ship per 5 minutes.
    Steps: Build snapshot rows from two reports of one ship received at 12:01 and 12:04, and one received at 12:06.
    Expected: 2 rows: bucket 12:00 holds the 12:04 report, bucket 12:05 holds the 12:06 report."""
    def at(minute, lat):
        return frame(spark, [report(lat=lat, t=f"2026-11-25 12:0{minute}:00 +0000 UTC")],
                     datetime(2026, 11, 25, 12, minute))
    cleaned = at(1, 59.1).unionByName(at(4, 59.4)).unionByName(at(6, 59.6))
    snaps = {r["bucket"]: json.loads(r["data"]) for r in hot.ship_snapshot_rows(ais.parse(cleaned)).collect()}
    b0 = int(datetime(2026, 11, 25, 12, 0).timestamp())
    assert snaps == {b0: snaps[b0], b0 + 300: snaps[b0 + 300]}
    assert snaps[b0]["latitude"] == 59.4 and snaps[b0 + 300]["latitude"] == 59.6
    actual({str(datetime.utcfromtimestamp(b)): s["latitude"] for b, s in snaps.items()})


def test_write_snapshots_layout_and_ttl(actual):
    """FT-RS-03: Redis snapshot layout and expiry
    Objective: Check the key layout the backend reads (kind:snaps, kind:snap:<t>) and the 24 h expiry.
    Steps: write_snapshots for two aircraft in bucket t and one in bucket t+60 into an in-memory Redis.
    Expected: sorted set holds t and t+60; hash for t has 2 fields; each hash expires at bucket + 86,400 s."""
    r = fakeredis.FakeRedis(decode_responses=True)
    t = int(time.time()) // 60 * 60    # a recent minute: older hashes would already be expired
    n = hot.write_snapshots(r, "planes", [Row(t, "a", "{}"), Row(t, "b", "{}"), Row(t + 60, "a", "{}")])
    assert n == 3
    assert r.zrange("planes:snaps", 0, -1, withscores=True) == [(str(t), t), (str(t + 60), t + 60)]
    assert r.hlen(f"planes:snap:{t}") == 2
    assert r.expiretime(f"planes:snap:{t}") == t + 86400
    actual(f"keys {sorted(r.keys())}; expiretime {r.expiretime(f'planes:snap:{t}')}")


def test_trim_removes_expired_index_entries(actual):
    """FT-RS-04: Expired snapshot times are removed from the index
    Objective: The time slider must not offer snapshots whose hash has already expired.
    Steps: Index buckets now-25h and now-1h, then call trim_snapshots.
    Expected: only now-1h remains."""
    r = fakeredis.FakeRedis(decode_responses=True)
    now = 1795281420
    r.zadd("ships:snaps", {str(now - 25 * 3600): now - 25 * 3600, str(now - 3600): now - 3600})
    removed = hot.trim_snapshots(r, "ships", now=now)
    assert removed == 1 and r.zrange("ships:snaps", 0, -1) == [str(now - 3600)]
    actual(f"removed {removed}, left {r.zrange('ships:snaps', 0, -1)}")


def test_hot_stream_end_to_end(spark, hdfs_root, tmp_path, monkeypatch, actual):
    """FT-RS-05: Streaming query writes the backend's contract to Redis
    Objective: Run the real hot-path query (start_hot) on a stream and check the JSON the backend reads.
    Steps: 1) Feed 2 OpenSky records through start_hot('planes') with an in-memory Redis. 2) Read planes:snap:<minute>.
    Expected: 2 aircraft in the hash; each JSON has icao24, callsign, origin_country, longitude, latitude, altitude, velocity, true_track."""
    server = fakeredis.FakeServer()
    monkeypatch.setattr(hot, "_redis", lambda: fakeredis.FakeRedis(server=server))
    src = tmp_path / "kafka"
    # received now: snapshots of older minutes would already have expired
    minute = int(time.time()) // 60 * 60
    write_kafka_batch(src, "opensky", [vector(icao="000001"), vector(icao="000002")],
                      datetime.utcfromtimestamp(minute + 30))
    q = hot.start_hot(spark, "planes", "availableNow", kafka_stream(spark, src))
    q.awaitTermination()
    r = fakeredis.FakeRedis(server=server, decode_responses=True)
    planes = {k: json.loads(v) for k, v in r.hgetall(f"planes:snap:{minute}").items()}
    assert set(planes) == {"000001", "000002"}
    assert set(planes["000001"]) == set(hot.FLIGHT_FIELDS)
    actual(f"planes:snap:{minute} -> {sorted(planes)}; fields {sorted(planes['000001'])}")
