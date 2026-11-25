# NiFi

NiFi pulls (or receives) data from the external APIs and publishes it to Kafka. Flows are stored here
as JSON so they are versioned in git.

- UI: https://localhost:8443/nifi (`admin` / `travelradar123`)
- Kafka inside NiFi: `kafka:9092` (each flow carries its own `Kafka3ConnectionService`)

## Flows

| Group | Source | Mechanism | Kafka topic (key) |
| --- | --- | --- | --- |
| `OpenSky` | OpenSky Network states, Nordic box | `InvokeHTTP` every 120 s (OAuth2) → `SplitJson` | `opensky` |
| `AISStream` | AIS position reports, same box | `ConnectWebSocket` + subscription message | `ais` |
| `DigitrafficRail` | Finnish trains: full timetables and GPS | `ConsumeMQTT` (wss://rata.digitraffic.fi/mqtt), push | `digitraffic-trains`, `digitraffic-locations` (`trainKey` = date_number) |
| `Entur` | every Norwegian public transport vehicle | `GenerateFlowFile` 60 s → `InvokeHTTP` POST GraphQL | `entur-vehicles` (`vehicleId`) |
| `OpenMeteo` | current weather, 12 cities | `InvokeHTTP` every 15 min → `SplitJson` | `weather` |

All publishers retry on failure (failure loops back) instead of dropping records.

Notes:
- **Keys.** NiFi 2.0.0 `PublishKafka` ignores `Kafka Key` / `kafka.key` with the `USE_VALUE` strategy
  and can only key by a *string* record field. Entur and Digitraffic therefore publish in record mode
  (`JsonTreeReader` → one Kafka message per record); Digitraffic adds the string field `trainKey` with
  a Jolt step first. OpenSky (bare arrays) and AIS (nested MMSI) are unkeyed; the Spark processing does
  not rely on per-key order (de-duplication and snapshots use event time).
- **Record mode for Entur** reads the whole 2 MB response as ~4,000 records without creating one
  FlowFile per vehicle.

## Deploy

```bash
python3 nifi/deploy.py                   # import missing groups, load keys, enable, start
python3 nifi/deploy.py --replace Entur   # re-import a group after its JSON changed
python3 nifi/deploy.py --status
```

Keys: `nifi/.env` (from `.env.example`) is passed to the container; `deploy.py` loads `OPENSKY_*`
into the parameter context "Environment Variables" via the `EnvironmentVariableParameterProvider`.
After creating or changing `nifi/.env`: `docker compose up -d nifi`, then `python3 nifi/deploy.py`.

## Changing a flow

Git only sees what you export, so the canvas and this folder are not synced automatically.

- **Push:** edit in NiFi → right-click the group → **Download flow definition** → **With external
  services** → overwrite the file here → `cd tests && uv run pytest` (flow checks) → commit
- **Pull:** `git pull` → `python3 nifi/deploy.py --replace <Group>`
- One person edits a flow at a time, because the JSON is hard to merge.

`ExampleProcessGroup.json` is the original tutorial flow and is not deployed.
