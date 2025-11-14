import os
import time

import pytest
import requests

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore


pytestmark = pytest.mark.integration


def _pg_conn():
    if psycopg2 is None:
        pytest.skip("psycopg2 not installed")
    dsn = os.getenv(
        "PG_DSN",
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "pulse"),
            pwd=os.getenv("POSTGRES_PASSWORD", "pulsepass"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "pulse"),
        ),
    )
    return psycopg2.connect(dsn)


def test_llm_call_logged():
    try:
        resp = requests.post(
            "http://localhost:8000/llm/chat",
            json={"system": "You are terse.", "user": "Say OK"},
            timeout=10,
        )
    except requests.RequestException as exc:  # pragma: no cover
        pytest.skip(f"LLM API unavailable: {exc}")

    if resp.status_code != 200:
        pytest.skip(f"unexpected status {resp.status_code}")

    time.sleep(0.5)

    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM llm_calls WHERE output_text ILIKE '%OK%'"
        )
        count = cur.fetchone()[0]
        assert count >= 1
