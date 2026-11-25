"""Static checks of the NiFi flow definitions (no running stack needed)."""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

REPO = Path(__file__).resolve().parents[1]
NIFI = REPO / "nifi"
FLOWS = {p.name.removesuffix("ProcessGroup.json"): json.loads(p.read_text())
         for p in sorted(NIFI.glob("*ProcessGroup.json")) if not p.name.startswith("Example")}


def processors(flow: dict) -> list[dict]:
    return flow["flowContents"]["processors"]


def by_type(kind: str):
    for name, flow in FLOWS.items():
        for p in processors(flow):
            if p["type"].endswith("." + kind):
                yield name, flow, p


def kafka_topics() -> dict[str, int]:
    out = {}
    for line in (REPO / "kafka/topics.txt").read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            name, partitions, *_ = line.split()
            out[name] = int(partitions)
    return out


def seconds(period: str) -> float:
    value, unit = period.split()
    return float(value) * {"sec": 1, "secs": 1, "min": 60, "mins": 60, "hour": 3600}[unit]


def test_every_topic_has_one_producer(actual):
    """FT-NF-01: Every Kafka topic is fed by exactly one NiFi flow
    Objective: Check the wiring between the acquisition flows and the Kafka topics the Spark pipeline reads.
    Steps: Collect the Topic Name of every PublishKafka processor in nifi/*ProcessGroup.json and compare with kafka/topics.txt.
    Expected: the two sets are equal and no topic has two producers."""
    produced = [p["properties"]["Topic Name"] for _, _, p in by_type("PublishKafka")]
    assert sorted(produced) == sorted(kafka_topics())
    actual(f"{len(produced)} topics, each with one producer: {sorted(produced)}")


def test_no_silent_data_loss(actual):
    """FT-NF-02: Records that Kafka rejects are retried, not dropped
    Objective: PublishKafka must not auto-terminate its failure relationship (this was the case in the first OpenSky and AIS flows).
    Steps: For every PublishKafka processor, check autoTerminatedRelationships and the outgoing connections.
    Expected: failure is never auto-terminated and loops back to the same processor."""
    for name, flow, p in by_type("PublishKafka"):
        assert "failure" not in (p.get("autoTerminatedRelationships") or []), name
        retry = [c for c in flow["flowContents"]["connections"]
                 if c["source"]["id"] == p["identifier"] and "failure" in c["selectedRelationships"]]
        assert retry and retry[0]["destination"]["id"] == p["identifier"], name
    actual(f"{len(list(by_type('PublishKafka')))} publishers retry on failure")


def test_polling_within_api_limits(actual):
    """FT-NF-03: Polling intervals respect the API limits
    Objective: OpenSky allows 4,000 credits/day at 4 credits per Nordic call (>= 86.4 s between calls); Entur and Open-Meteo are polled at the rates assumed in the report.
    Steps: Read the scheduling period of every InvokeHTTP/GenerateFlowFile that starts a request.
    Expected: OpenSky >= 86.4 s, Entur = 60 s, Open-Meteo = 15 min."""
    periods = {}
    for name, _, p in list(by_type("InvokeHTTP")) + list(by_type("GenerateFlowFile")):
        if p["schedulingPeriod"] != "0 sec":
            periods[name] = seconds(p["schedulingPeriod"])
    assert periods["OpenSky"] >= 4 * 86400 / 4000
    assert periods["Entur"] == 60
    assert periods["OpenMeteo"] == 900
    actual(periods)


def _spark_rule_box(module: str) -> tuple[float, float, float, float]:
    text = (REPO / f"spark/jobs/travelradar/sources/{module}.py").read_text()
    lat = re.search(r'"latitude": \{[^}]*"range": \(([\d.]+), ([\d.]+)\)', text).groups()
    lon = re.search(r'"longitude": \{[^}]*"range": \(([\d.]+), ([\d.]+)\)', text).groups()
    return float(lat[0]), float(lon[0]), float(lat[1]), float(lon[1])


def test_bounding_boxes_match_quality_rules(actual):
    """FT-NF-04: Acquisition area and quality rules agree
    Objective: The bounding box requested from OpenSky and subscribed at AISStream must equal the area the cleaned-data quality rules accept, otherwise valid data would be reported as out of range.
    Steps: Parse lamin/lomin/lamax/lomax from the OpenSky URL and the BoundingBoxes of the AIS subscription message; compare with the latitude/longitude ranges in the Spark source modules.
    Expected: all three boxes are (47.0, 4.5, 71.5, 32.0)."""
    url = next(p for _, _, p in by_type("InvokeHTTP") if "opensky" in p["properties"]["HTTP URL"])
    q = {k: float(v[0]) for k, v in parse_qs(urlparse(url["properties"]["HTTP URL"]).query).items()}
    opensky_box = (q["lamin"], q["lomin"], q["lamax"], q["lomax"])
    sub = next(p for _, _, p in by_type("ReplaceText"))["properties"]["Replacement Value"]
    (lat1, lon1), (lat2, lon2) = json.loads(re.sub(r"\$\{[^}]+\}", "", sub))["BoundingBoxes"][0]
    ais_box = (lat1, lon1, lat2, lon2)
    assert opensky_box == ais_box == _spark_rule_box("opensky") == _spark_rule_box("ais")
    actual(f"OpenSky {opensky_box}, AIS {ais_box}, Spark rules {_spark_rule_box('opensky')}")


def test_weather_cities_match_parser(actual):
    """FT-NF-05: The weather request asks for exactly the cities the parser knows
    Objective: The parser recovers the city from coordinates; a city added only to the NiFi URL would be assigned to a wrong neighbour.
    Steps: Parse latitude/longitude lists from the Open-Meteo URL and the CITIES table from spark/jobs/travelradar/sources/weather.py.
    Expected: the same 12 coordinate pairs in the same order."""
    url = next(p for n, _, p in by_type("InvokeHTTP") if n == "OpenMeteo")["properties"]["HTTP URL"]
    q = parse_qs(urlparse(url).query)
    requested = list(zip(map(float, q["latitude"][0].split(",")), map(float, q["longitude"][0].split(","))))
    tree = ast.parse((REPO / "spark/jobs/travelradar/sources/weather.py").read_text())
    cities = next(ast.literal_eval(node.value) for node in ast.walk(tree)
                  if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "CITIES")
    assert requested == [(lat, lon) for lat, lon, _ in cities.values()]
    actual(f"{len(requested)} cities: {list(cities)}")


def test_no_secrets_in_flows(actual):
    """FT-NF-06: No API secrets are committed
    Objective: Flow definitions live in git; keys must come from nifi/.env (git-ignored) at runtime.
    Steps: Scan every flow for sensitive parameter values, the AISStream key and OAuth client secrets; check .gitignore.
    Expected: sensitive parameters have no value; the AIS key is referenced as ${AISSTREAM_API_KEY}; the client secret is #{OPENSKY_CLIENT_SECRET}; nifi/.env is ignored."""
    for name, flow in FLOWS.items():
        for ctx in (flow.get("parameterContexts") or {}).values():
            for prm in ctx["parameters"]:
                if prm["sensitive"]:
                    assert not prm.get("value"), (name, prm["name"])
        text = json.dumps(flow)
        assert "APIKey\\\":\\\"${AISSTREAM_API_KEY}" in text or "aisstream" not in text.lower()
        for svc in flow["flowContents"]["controllerServices"]:
            secret = svc["properties"].get("client-secret")
            assert secret in (None, "#{OPENSKY_CLIENT_SECRET}"), name
    assert ".env" in (NIFI / ".gitignore").read_text().split()
    actual(f"{len(FLOWS)} flows checked, no secret values")


@pytest.mark.parametrize("name", sorted(FLOWS))
def test_flow_is_self_contained(name, actual):
    """FT-NF-07: Each flow imports into an empty NiFi
    Objective: nifi/deploy.py imports the JSON files into a fresh NiFi; every controller service a processor uses must be part of the file.
    Steps: For every processor property that references a controller service id, look the id up in the flow's own controller services.
    Expected: every reference resolves inside the file."""
    flow = FLOWS[name]["flowContents"]
    services = {s["identifier"] for s in flow["controllerServices"]}
    refs = []
    for p in flow["processors"]:
        for prop, desc in (p.get("propertyDescriptors") or {}).items():
            if desc.get("identifiesControllerService") and p["properties"].get(prop):
                refs.append(p["properties"][prop])
    for s in flow["controllerServices"]:
        for prop, desc in (s.get("propertyDescriptors") or {}).items():
            if desc.get("identifiesControllerService") and s["properties"].get(prop):
                refs.append(s["properties"][prop])
    missing = [r for r in refs if r not in services]
    assert refs and not missing
    actual(f"{len(refs)} service references, all resolved")
