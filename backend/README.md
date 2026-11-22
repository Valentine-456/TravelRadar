# TravelRadar Backend

FastAPI backend for TravelRadar. It serves the REST API under `/api` and the
compiled React frontend as a single-page application from `static/`.

## Setup

```bash
cp .env.example .env   # then fill in MAPBOX_TOKEN
uv sync
```

## Build the frontend into the backend

Run from the `backend/` folder. It builds the frontend and copies the result into `backend/static`:

```bash
(cd ../frontend && npm ci && npm run build) && rm -rf static && cp -r ../frontend/dist static
```

## Run

```bash
uv run fastapi dev   # development, auto-reload
uv run fastapi run   # production
```

The app runs on http://localhost:8000. Interactive API docs are at http://localhost:8000/docs.

## Configuration

Settings are read from `.env` (or environment variables):

| Variable       | Default                           | Description                      |
| -------------- | --------------------------------- | -------------------------------- |
| `MAPBOX_TOKEN` | empty                             | Public Mapbox token (`pk.…`)     |
| `MAPBOX_STYLE` | `mapbox://styles/mapbox/dark-v11` | Map style used by the frontend   |
| `STATIC_DIR`   | `backend/static`                  | Folder with the built frontend   |

The frontend gets its config from `GET /api/config`.

## API

| Endpoint | Returns |
| --- | --- |
| `GET /api/config` | Frontend config (Mapbox token and style) |
| `GET /api/flights/timestamps` | `{"timestamps": [...]}`, all minutes with a snapshot (Unix seconds), oldest first |
| `GET /api/flights/latest` | Newest snapshot as GeoJSON; `time` is its timestamp |
| `GET /api/flights/{timestamp}` | Snapshot at that minute, or the closest earlier one |
| `GET /api/ships/timestamps` | Same as flights, for ships (5-minute snapshots) |
| `GET /api/ships/latest` | Newest ships snapshot as GeoJSON |
| `GET /api/ships/{timestamp}` | Ships at that time, or the closest earlier snapshot |

Data is read from Redis (`planes:*` and `ships:*`), written by the Spark jobs `opensky_job.py` and `aisstream_job.py`.
