import libs.broker.alpaca_client as alp


def test_is_shortable_true(monkeypatch):
    monkeypatch.setattr(alp, "_request", lambda *args, **kwargs: {"shortable": True, "easy_to_borrow": True})
    assert alp.is_shortable("AAPL") is True


def test_place_short_sell_payload(monkeypatch):
    captured = {}

    def fake_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["payload"] = kwargs.get("json")
        return {"id": "order-1"}

    monkeypatch.setattr(alp, "_request", fake_request)
    resp = alp.place_short_sell("msft", 25, tif="day")

    assert resp["id"] == "order-1"
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/orders")
    assert captured["payload"]["symbol"] == "MSFT"
    assert captured["payload"]["side"] == "sell"
    assert captured["payload"]["qty"] == "25"


def test_trailing_buy_to_cover_converts_percentage(monkeypatch):
    captured = {}

    def fake_request(method, path, **kwargs):
        captured.update(kwargs.get("json") or {})
        return {"id": "trail-1"}

    monkeypatch.setattr(alp, "_request", fake_request)
    alp.trailing_buy_to_cover("tsla", 0.02, 5, tif="day")
    assert captured["type"] == "trailing_stop"
    assert captured["trail_percent"] == 2.0
