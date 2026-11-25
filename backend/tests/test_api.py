from __future__ import annotations

from conftest import put_snapshot

PLANE = {"icao24": "4caf28", "callsign": "RYR2KK", "origin_country": "Ireland", "longitude": 6.289,
         "latitude": 55.6628, "altitude": 11010.9, "velocity": 255.92, "true_track": 99.25}
SHIP = {"mmsi": 265520430, "ship_name": "VAXO", "latitude": 59.33, "longitude": 18.08,
        "speed_knots": 0.1, "heading": 216.0, "nav_status": 5}


def test_config_endpoint(client, actual):
    """FT-API-01: Frontend configuration endpoint
    Objective: The frontend receives the Mapbox token and style from the backend, never from its own bundle.
    Steps: GET /api/config.
    Expected: HTTP 200 with camelCase keys mapboxToken and mapboxStyle."""
    r = client.get("/api/config")
    assert r.status_code == 200
    assert set(r.json()) == {"mapboxToken", "mapboxStyle"}
    actual(f"200, keys {sorted(r.json())}")


def test_timestamps_sorted(client, redis_db, actual):
    """FT-API-02: Snapshot timestamps for the time slider
    Objective: The slider lists every available snapshot, oldest first.
    Steps: Store plane snapshots at t+120, t, t+60 (in that order); GET /api/flights/timestamps.
    Expected: [t, t+60, t+120]."""
    t = 1795281420
    for ts in (t + 120, t, t + 60):
        put_snapshot(redis_db, "planes", ts, {"4caf28": PLANE})
    r = client.get("/api/flights/timestamps")
    assert r.json() == {"timestamps": [t, t + 60, t + 120]}
    actual(r.json())


def test_latest_flights_geojson(client, redis_db, actual):
    """FT-API-03: Latest flights as GeoJSON
    Objective: The map layer gets a valid GeoJSON FeatureCollection built from the newest snapshot.
    Steps: Store an older snapshot (1 plane) and a newer one (2 planes); GET /api/flights/latest.
    Expected: FeatureCollection with time = newest snapshot, 2 Point features with [lon, lat] coordinates and properties callsign, country, track, altitude (rounded), velocity (rounded)."""
    t = 1795281420
    put_snapshot(redis_db, "planes", t, {"4caf28": PLANE})
    put_snapshot(redis_db, "planes", t + 60, {"4caf28": PLANE, "abcdef": {**PLANE, "icao24": "abcdef", "callsign": None}})
    body = client.get("/api/flights/latest").json()
    assert body["type"] == "FeatureCollection" and body["time"] == t + 60
    assert len(body["features"]) == 2
    f = next(f for f in body["features"] if f["properties"]["icao24"] == "4caf28")
    assert f["geometry"] == {"type": "Point", "coordinates": [6.289, 55.6628]}
    assert f["properties"] == {"icao24": "4caf28", "callsign": "RYR2KK", "country": "Ireland",
                               "track": 99.25, "altitude": 11011, "velocity": 256}
    nameless = next(f for f in body["features"] if f["properties"]["icao24"] == "abcdef")
    assert nameless["properties"]["callsign"] == "abcdef"   # falls back to icao24
    actual(f"time={body['time']}, {len(body['features'])} features, sample {f['properties']}")


def test_snapshot_at_or_before(client, redis_db, actual):
    """FT-API-04: Snapshot lookup for a slider position
    Objective: A slider position between snapshots shows the closest earlier snapshot (ships: 5-minute buckets).
    Steps: Store ship snapshots at t and t+300; GET /api/ships/{t+299}, /api/ships/{t+300}, /api/ships/{t-1}.
    Expected: time t, t+300, and an empty FeatureCollection with time null."""
    t = 1795281300
    put_snapshot(redis_db, "ships", t, {"265520430": SHIP})
    put_snapshot(redis_db, "ships", t + 300, {"265520430": {**SHIP, "latitude": 59.4}})
    got = [client.get(f"/api/ships/{ts}").json() for ts in (t + 299, t + 300, t - 1)]
    assert [g["time"] for g in got] == [t, t + 300, None]
    assert got[1]["features"][0]["geometry"]["coordinates"] == [18.08, 59.4]
    assert got[2]["features"] == []
    p = got[0]["features"][0]["properties"]
    assert p == {"mmsi": 265520430, "name": "VAXO", "speed": 0.1, "heading": 216.0, "navStatus": 5}
    actual(f"times {[g['time'] for g in got]}; properties {p}")


def test_empty_redis(client, actual):
    """FT-API-05: API works before any data has arrived
    Objective: A fresh stack (empty Redis) must not produce server errors.
    Steps: GET /api/flights/latest, /api/ships/latest, /api/flights/timestamps on an empty Redis.
    Expected: HTTP 200, empty FeatureCollections with time null, empty timestamp list."""
    a = client.get("/api/flights/latest")
    b = client.get("/api/ships/latest")
    c = client.get("/api/flights/timestamps")
    assert (a.status_code, b.status_code, c.status_code) == (200, 200, 200)
    assert a.json() == {"type": "FeatureCollection", "time": None, "features": []}
    assert b.json()["features"] == [] and c.json() == {"timestamps": []}
    actual("all 200, empty collections")


def test_invalid_timestamp(client, actual):
    """FT-API-06: Invalid timestamp is rejected
    Objective: Input validation on the path parameter.
    Steps: GET /api/flights/yesterday.
    Expected: HTTP 422 (validation error), not 500."""
    r = client.get("/api/flights/yesterday")
    assert r.status_code == 422
    actual(f"HTTP {r.status_code}")
