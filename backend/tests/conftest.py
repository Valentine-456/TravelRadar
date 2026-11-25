"""Backend API tests: FastAPI TestClient against an in-memory Redis (no running stack needed).

    uv run pytest            # from backend/

Each test's docstring holds its objective / steps / expected result; tests call `actual(...)` with
the observed result. Both land in the JUnit report (tests/reports/backend.xml) for the test document.
"""
from __future__ import annotations

import inspect
import json

import fakeredis
import pytest
from fastapi.testclient import TestClient

from backend.api.snapshots import get_redis
from backend.main import app


@pytest.fixture(autouse=True)
def _document(request, record_property):
    record_property("doc", inspect.getdoc(request.function) or "")


@pytest.fixture
def actual(record_property):
    def record(text) -> None:
        text = text if isinstance(text, str) else json.dumps(text, default=str)
        print(text)
        record_property("actual", text)
    return record


@pytest.fixture
def redis_db():
    r = fakeredis.FakeRedis(decode_responses=True)
    app.dependency_overrides[get_redis] = lambda: r
    yield r
    app.dependency_overrides.clear()


@pytest.fixture
def client(redis_db):
    return TestClient(app)


def put_snapshot(r, kind: str, ts: int, objects: dict[str, dict]) -> None:
    """Write a snapshot exactly like spark/jobs/travelradar/hot.py does."""
    for oid, data in objects.items():
        r.hset(f"{kind}:snap:{ts}", oid, json.dumps(data))
    r.zadd(f"{kind}:snaps", {str(ts): ts})
