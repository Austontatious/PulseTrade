from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import date, timedelta
from functools import partial
from typing import Any, Dict, Iterable, Optional

from libs.llm.run_and_log import LLMRunner
from libs.llm.schemas import POLICY_SCHEMA


runner = LLMRunner()
SHADOW_MODE = os.getenv("LLM_SHADOW_MODE", "false").lower() == "true"
ORDER_GATE_DISABLED = os.getenv("ORDER_GATE_DISABLED", "false").lower() == "true"
QUIVER_PUBLIC_EXTRAS = os.getenv("QUIVER_ENABLE_PUBLIC_EXTRAS", "false").lower() == "true"


def _parse_ab_buckets() -> Dict[str, list[str]]:
    mapping: Dict[str, list[str]] = {}
    raw = os.getenv("LLM_AB_BUCKETS", "")
    if not raw:
        return mapping
    for segment in raw.split(";"):
        segment = segment.strip()
        if not segment:
            continue
        try:
            key, versions_str = segment.split(":", 1)
        except ValueError:
            continue
        versions = [v.strip() for v in versions_str.split(",") if v.strip()]
        if versions:
            mapping[key.strip()] = versions
    return mapping


AB_BUCKETS = _parse_ab_buckets()


async def _fetch_quiver_summary(conn, ticker: str) -> Dict[str, Any]:
    aggregates: Dict[str, Any] = {}
    bullets: list[str] = []
    policy_flags: Dict[str, Any] = {}

    try:
        row = await conn.fetchrow(
            """
            SELECT
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_congress_net_usd' AND as_of >= CURRENT_DATE - INTERVAL '30 days'), 0) AS congress_30d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_congress_net_usd' AND as_of >= CURRENT_DATE - INTERVAL '90 days'), 0) AS congress_90d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_insider_net_usd' AND as_of >= CURRENT_DATE - INTERVAL '30 days'), 0) AS insiders_30d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_insider_net_usd' AND as_of >= CURRENT_DATE - INTERVAL '90 days'), 0) AS insiders_90d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_house_net_usd' AND as_of >= CURRENT_DATE - INTERVAL '30 days'), 0) AS house_30d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_senate_net_usd' AND as_of >= CURRENT_DATE - INTERVAL '30 days'), 0) AS senate_30d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_lobbying_spend_usd' AND as_of >= CURRENT_DATE - INTERVAL '365 days'), 0) AS lobbying_12m,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_gov_award_usd' AND as_of >= CURRENT_DATE - INTERVAL '365 days'), 0) AS gov_12m,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_gov_award_usd' AND as_of >= CURRENT_DATE - INTERVAL '90 days'), 0) AS gov_90d,
              COALESCE(AVG(value) FILTER (WHERE metric='quiver_app_rating' AND as_of >= CURRENT_DATE - INTERVAL '30 days'), NULL) AS app_rating_30d,
              COALESCE(SUM(value) FILTER (WHERE metric='quiver_app_reviews' AND as_of >= CURRENT_DATE - INTERVAL '30 days'), NULL) AS app_reviews_30d,
              COALESCE(AVG(value) FILTER (WHERE metric='quiver_etf_weight_pct' AND as_of >= CURRENT_DATE - INTERVAL '7 days'), NULL) AS etf_weight_7d,
              COALESCE(MAX(value) FILTER (WHERE metric='quiver_political_beta_today' AND as_of >= CURRENT_DATE - INTERVAL '2 days'), NULL) AS political_beta_today,
              COALESCE(AVG(value) FILTER (WHERE metric='quiver_political_beta_bulk' AND as_of >= CURRENT_DATE - INTERVAL '90 days'), NULL) AS political_beta_avg_90d
            FROM ingest_metrics
            WHERE symbol=$1
            """,
            ticker,
        )
    except Exception:
        return {"aggregates": {}, "bullets": [], "policy_flags": {}}

    if row:
        congress_30d = float(row.get("congress_30d") or 0.0)
        congress_90d = float(row.get("congress_90d") or 0.0)
        insiders_30d = float(row.get("insiders_30d") or 0.0)
        insiders_90d = float(row.get("insiders_90d") or 0.0)
        house_30d = float(row.get("house_30d") or 0.0)
        senate_30d = float(row.get("senate_30d") or 0.0)
        lobbying_12m = float(row.get("lobbying_12m") or 0.0)
        gov_12m = float(row.get("gov_12m") or 0.0)
        gov_90d = float(row.get("gov_90d") or 0.0)
        app_rating_30d = row.get("app_rating_30d")
        app_reviews_30d = row.get("app_reviews_30d")
        etf_weight_7d = row.get("etf_weight_7d")
        political_beta_today = row.get("political_beta_today")
        political_beta_avg_90d = row.get("political_beta_avg_90d")

        aggregates.update(
            {
                "congress_net_usd_30d": congress_30d,
                "congress_net_usd_90d": congress_90d,
                "insiders_net_usd_30d": insiders_30d,
                "insiders_net_usd_90d": insiders_90d,
                "house_net_usd_30d": house_30d,
                "senate_net_usd_30d": senate_30d,
                "lobbying_spend_12m": lobbying_12m,
                "gov_awards_12m": gov_12m,
                "gov_awards_90d": gov_90d,
                "app_rating_30d_avg": app_rating_30d,
                "app_reviews_30d": app_reviews_30d,
                "etf_weight_pct_7d_avg": etf_weight_7d,
                "political_beta_today": political_beta_today,
                "political_beta_avg_90d": political_beta_avg_90d,
            }
        )

        if abs(congress_30d) >= 1e5:
            bullets.append(_format_usd_bullet(congress_30d, "Congress net", "30d"))
        if abs(insiders_30d) >= 1e5:
            bullets.append(_format_usd_bullet(insiders_30d, "Insiders net", "30d"))
        if abs(house_30d) >= 1e5:
            bullets.append(_format_usd_bullet(house_30d, "House net", "30d"))
        if abs(senate_30d) >= 1e5:
            bullets.append(_format_usd_bullet(senate_30d, "Senate net", "30d"))
        if lobbying_12m >= 1e6:
            bullets.append(_format_usd_bullet(lobbying_12m, "Lobbying", "12m"))
        if gov_12m >= 1e6:
            bullets.append(_format_usd_bullet(gov_12m, "Gov awards", "12m"))

        policy_flags.update(
            {
                "has_congress_buy_30d": congress_30d > 0,
                "has_congress_sell_30d": congress_30d < 0,
                "insider_heavy_buy_30d": insiders_30d > 0,
                "insider_heavy_sell_30d": insiders_30d < 0,
                "has_house_buy_30d": house_30d > 0,
                "has_senate_buy_30d": senate_30d > 0,
                "gov_awards_large_90d": gov_90d >= 1_000_000,
            }
        )

        if QUIVER_PUBLIC_EXTRAS:
            if app_rating_30d is not None:
                bullets.append(
                    f"App rating {app_rating_30d:.2f} (30d avg{f', {app_reviews_30d:,.0f} reviews' if app_reviews_30d else ''})"
                )
            if etf_weight_7d is not None:
                bullets.append(f"ETF weight {etf_weight_7d:.2f}% (7d avg)")
            if political_beta_today is not None:
                bullets.append(f"Political beta {float(political_beta_today):+.2f}")
    else:
        aggregates.update(
            {
                "congress_net_usd_30d": 0.0,
                "congress_net_usd_90d": 0.0,
                "insiders_net_usd_30d": 0.0,
                "insiders_net_usd_90d": 0.0,
                "house_net_usd_30d": 0.0,
                "senate_net_usd_30d": 0.0,
                "lobbying_spend_12m": 0.0,
                "gov_awards_12m": 0.0,
                "gov_awards_90d": 0.0,
            }
        )
        policy_flags.update(
            {
                "has_congress_buy_30d": False,
                "has_congress_sell_30d": False,
                "insider_heavy_buy_30d": False,
                "insider_heavy_sell_30d": False,
                "has_house_buy_30d": False,
                "has_senate_buy_30d": False,
                "gov_awards_large_90d": False,
            }
        )

    # Off-exchange short volume dynamics
    offex_rows = []
    try:
        offex_rows = await conn.fetch(
            """
            SELECT as_of, value
            FROM ingest_metrics
            WHERE symbol=$1 AND metric='quiver_offex_shortvol_ratio'
              AND as_of >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY as_of DESC
            """,
            ticker,
        )
    except Exception:
        offex_rows = []

    if offex_rows:
        today = date.today()
        recent_cut = today - timedelta(days=5)
        prev_cut = today - timedelta(days=10)
        recent_values = [float(r["value"]) for r in offex_rows if r["as_of"] >= recent_cut]
        prev_values = [float(r["value"]) for r in offex_rows if prev_cut <= r["as_of"] < recent_cut]
        avg_recent = sum(recent_values) / len(recent_values) if recent_values else None
        avg_prev = sum(prev_values) / len(prev_values) if prev_values else None
        if avg_recent is not None:
            aggregates["offex_shortvol_ratio_5d_avg"] = avg_recent
        if avg_prev is not None:
            aggregates["offex_shortvol_ratio_prev5d_avg"] = avg_prev
        if avg_recent is not None and avg_prev is not None:
            delta = avg_recent - avg_prev
            aggregates["offex_shortvol_ratio_delta"] = delta
            if abs(delta) >= 0.02:
                change_pp = delta * 100
                bullets.append(f"Off-exch short ratio {avg_recent*100:.1f}% (5d avg, {change_pp:+.1f}pp)")
            policy_flags["offex_short_spike_5d"] = delta >= 0.05

    policy_flags.setdefault("offex_short_spike_5d", False)

    return {"aggregates": aggregates, "bullets": bullets[:6], "policy_flags": policy_flags}


def _format_usd_bullet(value: float, label: str, window: str) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        formatted = f"{value / 1_000_000_000:.1f}B"
    elif abs_value >= 1_000_000:
        formatted = f"{value / 1_000_000:.1f}M"
    elif abs_value >= 1_000:
        formatted = f"{value / 1_000:.1f}K"
    else:
        formatted = f"{value:.0f}"
    prefix = "+" if value > 0 else "" if value < 0 else ""
    return f"{label} {prefix}${formatted} ({window})"


def _select_version(prompt_key: str, ticker: str, as_of: str) -> Optional[str]:
    candidates = AB_BUCKETS.get(prompt_key)
    if not candidates:
        return None
    seed = f"{prompt_key}:{ticker}:{as_of}"
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    idx = int(h, 16) % len(candidates)
    return candidates[idx]


async def _call_runner(
    prompt_key: str,
    tpl_vars: Dict[str, Any],
    *,
    version: str | None = None,
    schema: Dict[str, Any] | None = None,
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    func = partial(
        runner.cached_call,
        prompt_key,
        tpl_vars,
        version=version,
        schema=schema,
        max_tokens=max_tokens,
    )
    return await loop.run_in_executor(None, func)


def _format_factors(factors: Dict[str, float] | None) -> str:
    if not factors:
        return "none"
    return ", ".join(f"{k}:{v:+.2f}" for k, v in sorted(factors.items()))


def _format_headlines(headlines: Iterable[str] | None) -> str:
    items = [h.strip() for h in (headlines or []) if h and h.strip()]
    if not items:
        return "No material headlines in the past 24 hours."
    return " ; ".join(items[:5])


async def generate_rationale(
    *,
    ticker: str,
    as_of: str,
    signal_label: str,
    signal_value: float,
    factors: Dict[str, float] | None,
    headlines: Iterable[str] | None,
) -> Dict[str, Any]:
    tpl_vars = {
        "ticker": ticker,
        "as_of": as_of,
        "signal_label": signal_label,
        "signal_value": f"{signal_value:+.4f}",
        "factors": _format_factors(factors),
        "headlines": _format_headlines(headlines),
    }
    version = _select_version("rationale", ticker, as_of)
    result = await _call_runner("rationale", tpl_vars, version=version)
    return result


async def policy_filter(
    *,
    ticker: str,
    as_of: str,
    earnings_date: str,
    liquidity_usd: float,
    borrow_ok: bool,
    factor_dispersion: float,
    coverage_pct: float,
    macro_regime: str,
    signal_label: str,
    signal_value: float,
    min_liquidity: float,
    extra_inputs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    inputs = {
        "earnings_date": earnings_date,
        "liquidity_usd": liquidity_usd,
        "borrow_ok": borrow_ok,
        "factor_dispersion": factor_dispersion,
        "coverage_pct": coverage_pct,
        "macro_regime": macro_regime,
        "signal_label": signal_label,
        "signal_value": signal_value,
        "min_liquidity": min_liquidity,
    }
    if extra_inputs:
        inputs.update(extra_inputs)
    tpl_vars = {
        "ticker": ticker,
        "as_of": as_of,
        "inputs_json": json.dumps(inputs),
    }
    version = _select_version("policy", ticker, as_of)
    if ORDER_GATE_DISABLED:
        result = {
            "text": json.dumps({"allow": True, "reasons": ["kill_switch"], "flags": []}),
            "json": {"allow": True, "reasons": ["kill_switch"], "flags": []},
            "cached": False,
            "prompt_key": "policy",
            "prompt_version": version or runner.registry.get("policy").version,
            "prompt_hash": "kill_switch",
            "success": True,
            "shadow": SHADOW_MODE,
            "extra_inputs": extra_inputs or {},
        }
        return result

    result = await _call_runner("policy", tpl_vars, version=version, schema=POLICY_SCHEMA, max_tokens=512)
    if SHADOW_MODE:
        result["shadow"] = True
    result["extra_inputs"] = extra_inputs or {}
    return result


async def summarize_items(ticker: str, items: Iterable[str], as_of: str) -> Dict[str, Any]:
    tpl_vars = {
        "ticker": ticker,
        "items": "\n".join(items),
        "as_of": as_of,
    }
    version = _select_version("summary", ticker, as_of)
    return await _call_runner("summary", tpl_vars)
