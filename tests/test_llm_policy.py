import json

import pytest

from libs.llm.client import LLMClient
from libs.llm.prompts import POLICY_SYSTEM, POLICY_USER_TEMPLATE


@pytest.mark.integration
def test_policy_json_shape():
    client = LLMClient()
    try:
        raw = client.chat(
            [
                LLMClient.sys(POLICY_SYSTEM),
                LLMClient.user(
                    POLICY_USER_TEMPLATE.format(
                        ticker="TEST",
                        as_of="2025-10-27",
                        earnings_date="2025-10-29",
                        liquidity_usd=f"{1_000_000:,}",
                        borrow_ok="True",
                        factor_dispersion="3.0",
                        coverage_pct="70%",
                        macro_regime="hawkish",
                        signal_label="BUY",
                        signal_value="1.2",
                        min_liquidity=f"{2_000_000:,}",
                    )
                ),
            ]
        )
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"LLM service unavailable: {exc}")
    data = json.loads(raw)
    assert set(data.keys()) == {"allow", "reasons", "flags"}
