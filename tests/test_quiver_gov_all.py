from services.ingest.ingest.collectors import quiver_govcontracts_all  # type: ignore[attr-defined]


def test_govcontracts_all_live_rows():
    sample = [{"Ticker": "MSFT", "Date": "2025-05-01T00:00:00Z", "Amount": "2500000"}]
    rows = quiver_govcontracts_all._rows_live_all(sample)  # type: ignore[attr-defined]
    assert rows and rows[0]["metric"] == "quiver_gov_award_usd" and rows[0]["value"] == 2500000
