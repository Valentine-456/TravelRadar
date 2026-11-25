"""Imports the NiFi flows of this folder into the running NiFi and starts them (idempotent).

    python3 nifi/deploy.py                  # import missing groups, sync keys, start everything valid
    python3 nifi/deploy.py --replace Entur  # re-import one group after its JSON changed
    python3 nifi/deploy.py --status         # only print the state of every group

Replaces the manual UI steps: imports each group (they carry their own Kafka3ConnectionService),
loads the OpenSky/AIS keys from NiFi's environment (nifi/.env) into the "Environment Variables"
parameter context, enables controller services and starts each group whose processors are valid.
Groups that stay invalid (e.g. OpenSky without keys) are reported and left stopped.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from nifi_api import NiFi

HERE = Path(__file__).resolve().parent
SKIP = {"Example"}            # ExampleProcessGroup.json is a tutorial flow, not part of the pipeline
KAFKA_SERVICE = "Kafka3ConnectionService"
ENV_REGEX = "(OPENSKY|AISSTREAM)_.*"


def flow_files() -> dict[str, Path]:
    files = {p.name.removesuffix("ProcessGroup.json"): p for p in sorted(HERE.glob("*ProcessGroup.json"))}
    return {k: v for k, v in files.items() if k not in SKIP}


def ensure_root_kafka_service(n: NiFi) -> None:
    root = n.root_id()
    svc = next((s for s in n.controller_services(root) if s["component"]["name"] == KAFKA_SERVICE), None)
    if svc is None:
        t = next(t for t in n.get("/flow/controller-service-types")["controllerServiceTypes"]
                 if t["type"].endswith(".Kafka3ConnectionService"))
        svc = n.call("POST", f"/process-groups/{root}/controller-services", {
            "revision": {"version": 0, "clientId": n.client_id},
            "component": {"type": t["type"], "bundle": t["bundle"], "name": KAFKA_SERVICE,
                          "properties": {"bootstrap.servers": "kafka:9092"}}})
        print(f"created root {KAFKA_SERVICE}")
    if svc["component"]["state"] != "ENABLED":
        n.call("PUT", f"/controller-services/{svc['id']}/run-status",
               {"revision": svc["revision"] | {"clientId": n.client_id}, "state": "ENABLED"})


def sync_env_parameters(n: NiFi) -> None:
    """Fetch OPENSKY_* / AISSTREAM_* from NiFi's environment into the parameter context."""
    providers = n.get("/flow/parameter-providers")["parameterProviders"]
    provider = next((p for p in providers
                     if p["component"]["type"].endswith("EnvironmentVariableParameterProvider")), None)
    if provider is None:
        return
    pid = provider["id"]
    props = provider["component"]["properties"]
    if props.get("include-environment-variables") != ENV_REGEX:
        provider = n.call("PUT", f"/parameter-providers/{pid}", {
            "revision": provider["revision"] | {"clientId": n.client_id},
            "component": {"id": pid, "properties": {"environment-variable-inclusion-strategy": "regex",
                                                    "include-environment-variables": ENV_REGEX}}})
    fetched = n.call("POST", f"/parameter-providers/{pid}/parameters/fetch-requests",
                     {"id": pid, "revision": provider["revision"] | {"clientId": n.client_id}})
    groups = fetched["component"].get("parameterGroupConfigurations", [])
    names = {name for g in groups for name in (g.get("parameterSensitivities") or {})}
    if "OPENSKY_CLIENT_SECRET" not in names:
        print("parameters: OPENSKY_* not in NiFi's environment (create nifi/.env, then "
              "`docker compose up -d nifi`); OpenSky stays invalid")
        return
    for g in groups:
        g["parameterContextName"] = g.get("parameterContextName") or g["groupName"]
        g["synchronized"] = True
        g["parameterSensitivities"] = {k: ("SENSITIVE" if k.endswith(("SECRET", "KEY")) else "NON_SENSITIVE")
                                       for k in g["parameterSensitivities"]}
    req = n.call("POST", f"/parameter-providers/{pid}/apply-parameters-requests", {
        "id": pid, "revision": fetched["revision"] | {"clientId": n.client_id},
        "parameterGroupConfigurations": groups})["request"]
    while not req["complete"]:
        time.sleep(1)
        req = n.get(f"/parameter-providers/{pid}/apply-parameters-requests/{req['requestId']}")["request"]
    n.call("DELETE", f"/parameter-providers/{pid}/apply-parameters-requests/{req['requestId']}")
    print(f"parameters: synced {sorted(names)}" + (f" ({req['failureReason']})" if req.get("failureReason") else ""))


def invalid_processors(n: NiFi, gid: str) -> list[str]:
    return [f"{p['component']['name']}: {(p['component'].get('validationErrors') or ['?'])[0][:160]}"
            for p in n.processors(gid) if p["component"]["validationStatus"] == "INVALID"]


def status(n: NiFi) -> None:
    for name, g in sorted(n.groups().items()):
        c = g["component"]
        s = n.group_status(g["id"])
        print(f"  {name:16} running {c['runningCount']}, stopped {c['stoppedCount']}, "
              f"invalid {c['invalidCount']}; sent to Kafka (5 min): {s['flowFilesSent']}; "
              f"queued {s['queued']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replace", nargs="*", default=[], help="groups to delete and import again")
    ap.add_argument("--no-start", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--url", default="https://localhost:8443/nifi-api")
    args = ap.parse_args()

    n = NiFi(args.url)
    if args.status:
        status(n)
        return
    # flows exported "with external services" reference a Kafka service on the root canvas
    if any(json.loads(p.read_text()).get("externalControllerServices") for p in flow_files().values()):
        ensure_root_kafka_service(n)
    root = n.root_id()
    existing = n.groups(root)
    for i, (name, path) in enumerate(flow_files().items()):
        if name in existing and name in args.replace:
            print(f"{name}: deleting old group")
            n.delete_group(existing.pop(name))
        if name in existing:
            print(f"{name}: already imported")
            continue
        n.upload_group(root, name, path.read_bytes(), x=(i % 3) * 450, y=(i // 3) * 250)
        print(f"{name}: imported from {path.name}")

    sync_env_parameters(n)
    if args.no_start:
        return
    groups = n.groups(root)
    for name in flow_files():
        gid = groups[name]["id"]
        n.set_services_state(gid, "ENABLED")
    time.sleep(3)
    failed = False
    for name in flow_files():
        gid = groups[name]["id"]
        bad = invalid_processors(n, gid)
        if bad:
            failed = True
            print(f"{name}: NOT started, invalid processors:\n    " + "\n    ".join(bad))
            continue
        n.set_group_state(gid, "RUNNING")
        print(f"{name}: running")
    status(n)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
