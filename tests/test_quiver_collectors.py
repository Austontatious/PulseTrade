import os

import pytest

from services.ingest.ingest.collectors import quiver_congress, quiver_insiders, quiver_offexchange
from services.ingest.ingest.common import upsert_metrics


def test_congress_row_sign():
    sample = [{"TransactionDate": "2024-10-01", "Transaction": "Buy", "TransactionAmountUSD": "10000", "Ticker": "AAPL"}]
    rows = quiver_congress._rows_from_congress_json(sample, src="test", ticker_hint="AAPL")  # type: ignore[attr-defined]
    assert len(rows) == 1
    row = rows[0]
    assert row["metric"] == "quiver_congress_net_usd"
    assert row["value"] == 10000.0


def test_insiders_rows():
    sample = [{
        "Date": "2024-09-15",
        "Ticker": "MSFT",
        "Shares": "1000",
        "PricePerShare": "250",
        "AcquiredDisposedCode": "D",
    }]
    rows = quiver_insiders._rows(sample, src="test", ticker_hint="MSFT")  # type: ignore[attr-defined]
    assert len(rows) == 1
    assert rows[0]["value"] == -250000.0


@pytest.mark.integration
def test_upsert_idempotent():
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
    try:
        import psycopg2

        conn = psycopg2.connect(dsn)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"database unavailable: {exc}")

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ingest_metrics WHERE symbol=%s AND metric=%s",
                ("TEST", "quiver_offex_shortvol_ratio"),
            )

    rows = [
        {
            "symbol": "TEST",
            "as_of": "2025-10-27",
            "metric": "quiver_offex_shortvol_ratio",
            "value": 0.55,
            "window": "1d",
            "src": "quiver:test",
            "raw": {},
        }
    ]
    first = upsert_metrics(rows)
    second = upsert_metrics(rows)
    assert first == 1 and second == 1

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ingest_metrics WHERE symbol=%s AND metric=%s",
                ("TEST", "quiver_offex_shortvol_ratio"),
            )
            count = cur.fetchone()[0]
    conn.close()
    assert count == 1
