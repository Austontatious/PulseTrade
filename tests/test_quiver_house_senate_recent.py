from services.ingest.ingest.collectors import quiver_house, quiver_senate  # type: ignore[attr-defined]


def test_house_recent_map():
    sample = [
        {
            "Ticker": "AAPL",
            "Date": "2025-10-01T00:00:00Z",
            "Transaction": "Buy",
            "Amount": 1000,
            "Type": "Stock",
        }
    ]
    rows = quiver_house._rows_live(sample)  # type: ignore[attr-defined]
    assert rows and rows[0]["metric"] == "quiver_house_net_usd" and rows[0]["value"] == 1000


def test_senate_recent_map_excludes_options():
    sample = [
        {
            "Ticker": "AAPL",
            "Date": "2025-10-01T00:00:00Z",
            "Transaction": "Sale",
            "Amount": 500,
            "Type": "Stock Option",
        }
    ]
    rows = quiver_senate._rows_live(sample)  # type: ignore[attr-defined]
    assert rows == []
