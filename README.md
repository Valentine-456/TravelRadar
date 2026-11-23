# Nordic Travel Radar Dashboard: 

*Multimodal travel intelligence with streaming delay prediction*

## Setup

Requirements: Docker Desktop (8 GB memory), [uv](https://docs.astral.sh/uv/), Node.js.

### 1. Secrets

```bash
cp nifi/.env.example nifi/.env         # OpenSky client id/secret, AISStream API key
cp backend/.env.example backend/.env   # Mapbox token
```

### 2. Infrastructure

```bash
docker compose up -d --build

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic opensky --partitions 3
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ais --partitions 3
```

### 3. NiFi

https://localhost:8443/nifi, login `admin` / `travelradar123`.

1. **☰ → Controller Settings → Parameter Providers → +** `EnvironmentVariableParameterProvider`, include regex `OPENSKY_.*` → **Fetch Parameters**, mark `OPENSKY_CLIENT_SECRET` sensitive.
2. Drag a **Process Group** onto the canvas → upload `nifi/OpenSkyProcessGroup.json`. Repeat for `nifi/AISStreamProcessGroup.json`.
3. OpenSky group → **Configure** → Parameter Context **Environment Variables**.
4. Each group → **Enable all controller services** → **Start**.

### 4. Spark jobs

```bash
cd spark
./submit.sh -d opensky_job.py
./submit.sh -d aisstream_job.py
```

Logs in `spark/logs/`. Stop a job: http://localhost:8080 → **(kill)**.

### 5. Backend + frontend

```bash
cd backend
(cd ../frontend && npm ci && npm run build) && rm -rf static && cp -r ../frontend/dist static
uv sync
uv run fastapi run
```

Open http://localhost:8000.

For frontend development: `uv run fastapi dev` in `backend/` and `npm run dev` in `frontend/`.
