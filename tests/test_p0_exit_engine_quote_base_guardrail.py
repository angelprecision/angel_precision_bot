from pathlib import Path
import re
import textwrap


SRC = (Path(__file__).resolve().parents[1] / "ap_exit_engine.py").read_text()


def _extract_fetch_broker_quote():
    m = re.search(
        r"^    def _fetch_broker_quote\(self,\s*sym: str\) -> dict:.*?(?=^    def )",
        SRC,
        re.DOTALL | re.MULTILINE,
    )
    assert m, "_fetch_broker_quote method not found"
    body = textwrap.dedent(m.group(0))

    ns = {}
    exec("import logging\nlog = logging.getLogger('test.exit_engine')\n" + body, ns)
    return ns["_fetch_broker_quote"]


class _Engine:
    def __init__(self, broker, email="test@example.com"):
        self.broker = broker
        self._email = email


class _Broker:
    def __init__(self, *, base_url=None, access_token=None, shadow_token=None):
        self.base_url = base_url
        self.access_token = access_token
        self._access_token = shadow_token


def test_source_does_not_hardcode_live_fallback():
    idx = SRC.find("def _fetch_broker_quote")
    end = SRC.find("\n    def ", idx + 1)
    body = SRC[idx:end]
    assert 'or "https://api.tradier.com"' not in body
    assert "BROKER_QUOTE_BASE_URL_MISSING" in body
    assert "BROKER_QUOTE_TOKEN_MISSING" in body


def test_missing_base_url_returns_empty_and_skips_request(monkeypatch):
    fn = _extract_fetch_broker_quote()
    called = {"n": 0}

    def _boom(*_a, **_kw):
        called["n"] += 1
        raise AssertionError("requests.get must not be called")

    import requests
    monkeypatch.setattr(requests, "get", _boom)

    eng = _Engine(_Broker(base_url=None, access_token="tok"))
    out = fn(eng, "SPY260620C00500000")
    assert out == {"bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0}
    assert called["n"] == 0


def test_missing_token_returns_empty_and_skips_request(monkeypatch):
    fn = _extract_fetch_broker_quote()
    called = {"n": 0}

    def _boom(*_a, **_kw):
        called["n"] += 1
        raise AssertionError("requests.get must not be called")

    import requests
    monkeypatch.setattr(requests, "get", _boom)

    eng = _Engine(_Broker(base_url="https://sandbox.tradier.com", access_token=None, shadow_token=None))
    out = fn(eng, "SPY260620C00500000")
    assert out == {"bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0}
    assert called["n"] == 0


def test_valid_broker_uses_own_base_url(monkeypatch):
    fn = _extract_fetch_broker_quote()
    seen = {}

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"quotes": {"quote": {"bid": 1.0, "ask": 1.2, "mark": 1.1, "last": 1.1}}}

    def _fake_get(url, **kwargs):
        seen["url"] = url
        seen["kwargs"] = kwargs
        return _Resp()

    import requests
    monkeypatch.setattr(requests, "get", _fake_get)

    eng = _Engine(_Broker(base_url="https://sandbox.tradier.com", access_token="tok"))
    out = fn(eng, "SPY260620C00500000")
    assert seen["url"] == "https://sandbox.tradier.com/v1/markets/quotes"
    assert out["bid"] == 1.0
    assert out["ask"] == 1.2
    assert out["mid"] == 1.1


def test_network_error_is_non_fatal(monkeypatch):
    fn = _extract_fetch_broker_quote()

    def _boom(*_a, **_kw):
        raise RuntimeError("net")

    import requests
    monkeypatch.setattr(requests, "get", _boom)

    eng = _Engine(_Broker(base_url="https://sandbox.tradier.com", access_token="tok"))
    out = fn(eng, "SPY260620C00500000")
    assert out == {"bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0}
