# Milestone Gap Analysis: Data Acquisition & Pre-processing

**Team:** Fable 6.7 (Nordic Travel Radar)  
**Repo:** https://github.com/Valentine-456/TravelRadar  
**Baseline:** current `dev` / `main` vs. milestone requirements (data acquisition & pre-processing)  
**Reference design:** `documentation/Fable6.7.pdf` (Milestone 1 / PM1)

---

## Executive summary

The repo already has a working **stream slice** for two sources:

`OpenSky` / `AISStream` → **NiFi** → **Kafka** → **Spark Structured Streaming** → **Redis** → **FastAPI** → **Mapbox UI**

Against this milestone’s deliverables, the largest gaps are:

1. **Missing acquisition & pre-processing for the primary ML sources** (rail, bus, weather, history, timetables).
2. **No HDFS/Hive (or equivalent durable warehouse)** and no batch path.
3. **No tests** and **no Test / Presentation / updated Report PDFs**.
4. **Report content gaps**: conceptual data model, Lambda/Kappa labelling, dataset catalogue updates, ML feature definitions, reprocessing story.

Code that exists today is valuable demos for flights/ships, but the milestone’s stated focus is **data acquisition + pre-processing modules** with documented quality evidence — not the map UI alone.

---

## Deliverable checklist (submission package)

| Deliverable | Required name / form | Status | What to do |
|-------------|----------------------|--------|------------|
| Project Report | `TeamName_Report.pdf` (e.g. `Fable6.7_Report.pdf`) | **Missing** (only PM1 PDF exists) | Write updated report covering §§1–6 below; export PDF |
| Presentation | `TeamName_Presentation.pdf` | **Missing** | Build slides for the 90‑min meeting; export PDF |
| Test Documentation | `TeamName_Tests.pdf` | **Missing** | Functional + quality tests with tabular cases + evidence |
| Git repository link | Source + config + docs | **Partial** | Keep `dev`/`main` clean; ensure README + flows + jobs + docs are committed |
| ProjectMeetings slot | MS Teams register | **Process** | Reserve supervisor discussion slot; revise report by Wednesday EOD after feedback |

---

## Current implementation snapshot

| Area | Implemented | Not implemented |
|------|-------------|-----------------|
| Infra | Docker Compose: Kafka, NiFi, Redis, Spark | HDFS, Hive, schema registry |
| Acquisition | NiFi flows: OpenSky (REST ~120s), AISStream (WebSocket) | Digitraffic rail/GPS/AIS REST, Entur, Trafiklab GTFS-RT, DB Timetables, Open-Meteo, static GTFS, Finnish rail archive |
| Streaming transform | `opensky_job.py`, `aisstream_job.py` → Redis snapshots (24h TTL) | Feature engineering, weather joins, delay labels, Parquet→warehouse writes |
| Serving / UI | FastAPI GeoJSON + map (planes/ships, timeline) | Heatmap/stats wired to real data; delay/reliability views |
| ML | — | Delay regression, reliability classification, anomaly detection |
| Tests | — | Functional acquisition tests, transformation quality tests, CI |
| Architecture docs | Informal READMEs + PM1 PDF | Explicit Lambda/Kappa diagram, conceptual ER/data model, updated assumptions |

---

## Requirement-by-requirement gap analysis

### 1. Updates to PM1 Report (business & technical assumptions)

| Item | Status | Needs implementation / writing |
|------|--------|--------------------------------|
| Revisions to assumptions with justification | **Gap** | Document what changed since 18 Oct 2026 PM1 |

**Likely assumption updates to justify (based on repo vs PM1):**

| PM1 assumption | Current reality | Action |
|----------------|-----------------|--------|
| Digitraffic Marine AIS (REST) for ships | Implemented **AISStream WebSocket** instead | Justify (coverage, rate limits, ease of streaming); note Finnish REST may still be added |
| OpenSky poll ~90s / 4 credits | NiFi flow uses **120s** | Align numbers with measured quota usage |
| Full NiFi→Kafka→**HDFS/Hive** path | Only NiFi→Kafka→Spark→**Redis** | State warehouse as next/in-progress; revise MS2 scope if deferred |
| Normalization in NiFi for all feeds | Only OpenSky split + AIS pass-through | Describe actual normalization locus (NiFi vs Spark) per source |
| Parquet micro-batches every 10 min | **Not written** to durable storage | Implement or explicitly defer with impact |
| Feasibility prototype with 18 tests / Hive DDL | **Not present** in this repo | Either port artifacts or drop claim and re-baseline |

---

### 2. Conceptual Data Model

| Item | Status | Needs implementation / writing |
|------|--------|--------------------------------|
| Key entities and relationships | **Gap** (no ER/conceptual diagram in repo) | Produce model + short narrative |

**Entities to define (minimum for this milestone):**

```
Operator ──< Vehicle / Trip ──< StopEvent / PositionUpdate
                │                     │
                │                     ├── DelayObservation (label)
                │                     └── AnomalyFlag (later)
                │
                ├── Mode (rail, bus, ferry, flight, ship, coach)
                │
WeatherStation / CityWeather ── (spatial/temporal join) ── StopEvent
TimetableStop / StaticSchedule ── (batch) ── Trip
Snapshot (Redis hot state) ← derived from PositionUpdate stream
```

**Also document:** keys (`icao24`, `mmsi`, trip/vehicle IDs per country), grain of each event, and how Redis snapshot entities relate to Kafka topics.

---

### 3. Description of Data Sets (all sources)

PM1 already surveyed 11 sources. This milestone needs an **updated catalogue** reflecting **what is actually acquired** and planned.

For **each** source (implemented + planned), the report must include:

| Required field | OpenSky (impl.) | AISStream (impl.) | Rail/bus/weather/history (PM1) |
|----------------|-----------------|-------------------|--------------------------------|
| Data category | Aircraft positions | Vessel positions | Mostly **not ingested in code** |
| Avg records & inflow frequency | Partial (PM1 ~0.26M/day @ ~90s; code 120s) | Partial (PM1 Digitraffic AIS figures; source differs) | Re-measure when connected |
| Features per record | Documented in Spark job | Documented in Spark schema | Need schemas + examples |
| Time period covered | Live + Redis 24h | Live + Redis 24h | History: FI archive from 2022 (planned) |
| Data quality assessment | Light (`dropna` lat/lon/track) | Bounds + heading fallback | Entur/FI quality notes in PM1 — refresh with pipeline metrics |
| Source format | JSON REST | JSON WebSocket | JSON / protobuf / XML / GTFS as per PM1 |
| Independent / dependent features (ML inputs) | N/A for delay ML (map/anomaly only) | N/A for delay ML | **Required** for rail/bus delay & reliability |
| ML transformations | None | None | Must specify when pre-processing exists |

**Must implement or explicitly scope out before report freeze:**

1. At least the **MS2-critical feeds** from PM1 plan: Digitraffic rail (+ GPS), Entur realtime, Trafiklab GTFS-RT, Open-Meteo, Finnish rail archive, static timetables — **or** a supervisor-agreed reduced set with clear justification.
2. Sample payloads + measured volume/velocity tables (refresh PM1 numbers with current collectors).
3. For ML-bound datasets: list **X** (features) and **y** (delay minutes; late≥3 min), plus transforms (units, cyclic time, joins, filters for COMPLETED/stale delays, etc.).

---

### 4. Data Acquisition and Transformation

| Sub-requirement | Status | What needs to be implemented |
|-----------------|--------|------------------------------|
| Acquisition strategy (tech, libraries, API limits) | **Partial** (OpenSky OAuth2, AISStream WS; README + NiFi JSON) | NiFi (or collectors) for remaining sources; adaptive polling for Trafiklab/OpenSky/DB quotas; document retries/rate limits |
| Transformation steps | **Partial** (parse, cast, dedupe, bucket, Redis write) | Cleaning rules, weather join, feature rows, timetable alignment, Parquet/Hive load |
| Storage strategy | **Partial** (Kafka topics + Redis 24h) | Durable store (HDFS/Hive or agreed alternative), retention, partitioning, schemas |

**Concrete engineering backlog (priority order):**

| Priority | Work item | Notes |
|----------|-----------|-------|
| P0 | NiFi → Kafka for Digitraffic live-trains (+ optional GPS) | Core delay labels / features |
| P0 | NiFi → Kafka for Entur realtime | Highest volume (~5.5M rec/day in PM1) |
| P0 | Spark pre-processing job(s): clean, dedupe, normalize cross-source schema | Shared event model |
| P0 | Durable sink (Hive/HDFS Parquet **or** documented substitute) | Required by architecture story |
| P1 | Open-Meteo current (+ archive backfill) | Weather features |
| P1 | Finnish rail daily archive loader | Offline training / EDA |
| P1 | Trafiklab GTFS-RT with adaptive polling | Quota-sensitive |
| P2 | DB Timetables, Flix/Entur/Trafiklab static GTFS | Routing later; still “datasets” for report |
| P2 | Align ship source story (AISStream vs Digitraffic Marine) | Report + optional second flow |
| P3 | Replace `/tmp` Spark checkpoints with persistent volume | Ops quality |

---

### 5. System Architecture

| Sub-requirement | Status | What needs to be done |
|-----------------|--------|------------------------|
| Diagram + detailed description | **Partial** (PM1 Fig. 5–6; outdated vs code) | New diagram matching **implemented + planned** stack |
| All technologies (impl. + planned) | **Partial** | Mark each box: *live* / *in progress* / *planned* |
| Data flows | **Partial** | Per-topic flow: source → NiFi → Kafka → Spark → Redis/Hive → API → UI |
| Explicit **Lambda or Kappa** alignment on diagram | **Missing** (words never used in repo/PM1) | Choose and label clearly |
| If Kappa: show historical stream reprocessing | **Missing** | Demo replay from Kafka (or stored topics) through same job |

**Architecture decision to lock for the report:**

| Pattern | Fit to TravelRadar | Implication |
|---------|--------------------|-------------|
| **Lambda** (matches PM1 intent) | Stream path (live features/scores) + batch path (Hive history, offline GBT, timetables) | Must show **both** layers on diagram; implement at least a thin batch/history path |
| **Kappa** | Single streaming pipeline; Kafka (or log) replay for reprocessing | Must **demonstrate reprocessing** of historical data through the same job; batch Hive becomes optional |

**Recommendation:** Stay with **Lambda** (as PM1 “stream core + batch supporting”), implement a minimal batch/history leg, and label the diagram **Lambda**. Only pick Kappa if you drop Hive and can show Kafka replay end-to-end.

---

### 6. Analytical Approach

| Sub-requirement | Status | What to document / prepare |
|-----------------|--------|----------------------------|
| Planned ML / analytical tasks | **In PM1 only** | Reaffirm: delay regression, reliability classification, anomaly detection; map-only for flights/ships |
| Batch vs stream use per task | **In PM1 only** | Stream: online score + prequential learn; Batch: baselines, initial GBT, timetable index, EDA |
| How results are shown later | **Partial UI** | Map layers, heatmap, replay slider, alerts, model-quality panel — mark which exist vs MS3/MS4 |

**No ML code is required to be production-ready in this milestone**, but the report must state feature definitions and transforms for datasets that will feed classification/regression. Prefer implementing **pre-processing that emits feature-ready tables/topics**, even if model training waits for MS3.

---

## Testing requirements (gap)

| Requirement | Status | What to implement |
|-------------|--------|-------------------|
| Functional tests for acquisition modules | **Missing** | e.g. NiFi/Kafka produce expected schema; API poll returns 2xx; WS subscription active; topic message rate > 0 |
| Quality tests for transformation | **Missing** | null/range checks, dedupe correctness, bucket alignment, Redis key TTL, schema conformance, row counts in/out |
| Tabular test case documentation | **Missing** | Table: objective → steps → expected → actual (screenshot/log/summary) → PDF `TeamName_Tests.pdf` |

**Suggested starter test matrix**

| ID | Objective | Module | Expected |
|----|-----------|--------|----------|
| FT-01 | OpenSky poll publishes to Kafka | NiFi / `opensky` | Messages arrive ≤ poll interval + skew |
| FT-02 | AISStream positions publish to Kafka | NiFi / `ais` | `MessageType=PositionReport` JSON |
| FT-03 | Spark OpenSky job writes Redis snap | `opensky_job` | `planes:snaps` score increases; hash fields valid JSON |
| FT-04 | Spark AIS job writes Redis snap | `aisstream_job` | `ships:snap:*` keys; invalid coords dropped |
| FT-05 | API serves latest flights GeoJSON | Backend | HTTP 200; FeatureCollection with lon/lat |
| FT-06 | API serves ships + timestamps | Backend | Closest earlier 5‑min snap |
| QT-01 | Lat/lon within valid bounds | Transform | No out-of-range points in Redis |
| QT-02 | Dedup per vehicle per bucket | Transform | ≤1 record per `(minute, icao24)` / `(bucket, mmsi)` |
| QT-03 | Retention window | Storage | Keys older than 24h removed |
| QT-04 | Schema required fields present | Transform | Reject/drop incomplete records; count logged |

Extend the same pattern for each new rail/bus/weather feed as it lands.

---

## Presentation session (non-code)

| Requirement | Action |
|-------------|--------|
| 90‑minute project presentation | Prepare `TeamName_Presentation.pdf` covering architecture, datasets, acquisition demo, pre-processing, tests, next steps |
| All members attend; ask questions / give feedback | Process — schedule + participate |
| Reserve ProjectMeetings slot | Book supervisor review; revise report by Wednesday EOD |

---

## Suggested work plan (implementation → docs)

### Phase A — Close the “acquisition + pre-processing” story (code)

1. Agree source scope with team (full MS2 list vs reduced set).
2. Add NiFi flows + Kafka topics for priority live feeds.
3. Add Spark pre-processing (normalize, quality filters, optional weather join).
4. Add durable storage path (Hive/HDFS Parquet **or** Kappa replay demo).
5. Keep Redis serving path for dashboard demos.

### Phase B — Evidence

6. Automate FT/QT tests; capture logs/screenshots.
7. Measure volume/velocity/quality for the report tables.

### Phase C — Documents

8. Update report §§1–6 (assumptions, conceptual model, datasets, acquisition, **Lambda/Kappa** architecture, analytics).
9. Export `Fable6.7_Report.pdf`, `Fable6.7_Presentation.pdf`, `Fable6.7_Tests.pdf`.
10. Supervisor meeting → revise → final submit Wednesday EOD.

---

## Priority backlog (what “needs to be implemented”)

### Critical for this milestone (do these)

- [ ] Acquisition modules for **rail / bus / weather** (and history/timetables as scoped)
- [ ] Pre-processing jobs that **clean, normalize, and quality-check** beyond the current map snapshots
- [ ] **Durable storage or Kappa reprocessing** story that matches the architecture claim
- [ ] **Functional + quality tests** with recorded actual results
- [ ] **Conceptual data model** diagram
- [ ] Architecture diagram **explicitly labelled Lambda or Kappa**
- [ ] Updated **dataset descriptions** (all sources) including ML feature definitions where applicable
- [ ] Written justification of **PM1 assumption changes**
- [ ] PDFs: Report, Presentation, Tests (`Fable6.7_*.pdf`)

### Important but secondary for this milestone

- [ ] Trafiklab adaptive polling; DB Timetables; static GTFS loaders
- [ ] Wire UI heatmap/stats to real aggregates
- [ ] Persistent Spark checkpoints / better ops readiness

### Explicitly later (MS3/MS4 — mention in report, don’t block submission)

- Online delay models (River), offline GBT, drift detection
- LLM query parser, route finder, Trip Watch / alerts
- Full anomaly detection productization

---

## Traceability: requirement → artifact

| Milestone requirement | Primary artifact to produce |
|-----------------------|-----------------------------|
| §1 Assumption updates | Report section + changelog vs `Fable6.7.pdf` |
| §2 Conceptual data model | Report diagram + short entity dictionary |
| §3 Dataset descriptions | Report tables (one per source) + sample files under `nifi/responses/` or `docs/samples/` |
| §4 Acquisition & transformation | NiFi JSON + Spark jobs + report narrative |
| §5 Architecture | Report diagram (Lambda/Kappa marked) + `docker-compose` / data-flow description |
| §6 Analytical approach | Report section (batch vs stream per task; UI plan) |
| Functional / quality tests | Test code + `TeamName_Tests.pdf` tables with evidence |
| Presentation | `TeamName_Presentation.pdf` |

---

## Bottom line

**Already strong:** end-to-end streaming demo for flights and ships, reproducible Docker setup, basic transforms to Redis, live map.

**Must still implement for this milestone:** broader **data acquisition**, real **pre-processing/quality** pipelines, a credible **storage/architecture** (Lambda batch leg or Kappa replay), **tests with evidence**, and the three **PDF deliverables** with an updated report that fills every required section (especially conceptual model, dataset catalogue, and explicit Lambda/Kappa alignment).
