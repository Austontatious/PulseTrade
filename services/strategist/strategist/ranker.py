import asyncpg
import json
import os
import datetime as dt
from statistics import mean
from typing import Any, Dict, List

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

LOOKBACK_MIN = int(os.getenv("STRAT_RECO_LOOKBACK_MIN", "30"))
MOM_SHORT_MIN = int(os.getenv("STRAT_MOM_SHORT_MIN", "60"))
MOM_MED_MIN = int(os.getenv("STRAT_MOM_MED_MIN", "360"))
MAX_RECO = int(os.getenv("STRAT_RECO_TOPK", "50"))
MIN_MKT_CAP = float(os.getenv("STRAT_MIN_MARKET_CAP", str(10_000_000_000)))
MIN_DOLLAR_VOL = float(os.getenv("STRAT_MIN_DOLLAR_VOL", str(10_000_000)))
ENABLE_TURNAROUND = os.getenv("STRAT_ENABLE_TURNAROUND", "0") == "1"

async def _universe(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch("SELECT ticker FROM symbols WHERE class='equity' ORDER BY ticker ASC LIMIT 1000")
    return [r[0] for r in rows]


async def _weekly_llm_reviews(conn: asyncpg.Connection, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    rows = await conn.fetch(
        """
        SELECT symbol, output_json
        FROM llm_symbol_reviews
        WHERE scope='weekly_deep_dive'
          AND as_of = (
            SELECT MAX(as_of) FROM llm_symbol_reviews WHERE scope='weekly_deep_dive'
          )
          AND symbol = ANY($1)
        """,
        symbols,
    )
    mapping: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sym = (row["symbol"] or "").upper()
        if not sym:
            continue
        payload = row["output_json"] or {}
        mapping[sym] = payload
    return mapping

async def _latest_factors(conn: asyncpg.Connection, symbol: str):
    row = await conn.fetchrow(
        """
        WITH mx AS (SELECT MAX(ds) AS ds FROM fundamentals_factors_v1)
        SELECT f.*
        FROM fundamentals_factors_v1 f
        JOIN mx USING (ds)
        WHERE f.symbol=$1
        LIMIT 1
        """,
        symbol,
    )
    return row

async def _liquidity(conn: asyncpg.Connection, symbol: str):
    row = await conn.fetchrow(
        "SELECT avg_dollar_vol_60d FROM mv_liquidity_60d WHERE symbol=$1",
        symbol,
    )
    return float(row["avg_dollar_vol_60d"]) if row and row["avg_dollar_vol_60d"] is not None else None

async def _mom(conn: asyncpg.Connection, ticker: str, minutes: int) -> float | None:
    rows = await conn.fetch(
        """
        SELECT ts, price FROM trades
        WHERE ticker=$1 AND ts > NOW() - make_interval(mins => $2::int)
        ORDER BY ts ASC
        LIMIT 600
        """,
        ticker,
        minutes,
    )
    if len(rows) < 3:
        return None
    p0 = float(rows[0]["price"]); p1 = float(rows[-1]["price"]) if rows else p0
    return (p1/p0) - 1.0 if p0 > 0 else 0.0

async def _features(conn: asyncpg.Connection, ticker: str):
    # Multi-horizon momentum + quality/liquidity factors
    mom_s = await _mom(conn, ticker, MOM_SHORT_MIN)
    mom_m = await _mom(conn, ticker, MOM_MED_MIN)
    if mom_s is None or mom_m is None:
        return None
    srow = await conn.fetchrow(
        """
        SELECT senti_mean FROM social_features
        WHERE ticker=$1 ORDER BY ts DESC, window_minutes ASC LIMIT 1
        """,
        ticker,
    )
    smean = float(srow["senti_mean"]) if srow and srow["senti_mean"] is not None else 0.0
    f = await _latest_factors(conn, ticker)
    liq = await _liquidity(conn, ticker)
    market_cap = float(f["market_cap"]) if f and f["market_cap"] is not None else None
    sector = f["sector"] if f else None
    feat = {
        "mom_s": mom_s,
        "mom_m": mom_m,
        "senti": smean,
        "z_roe": float(f["z_roe"]) if f and f["z_roe"] is not None else 0.0,
        "z_roa": float(f["z_roa"]) if f and f["z_roa"] is not None else 0.0,
        "z_gm": float(f["z_gm"]) if f and f["z_gm"] is not None else 0.0,
        "z_dta": float(f["z_dta"]) if f and f["z_dta"] is not None else 0.0,
        "z_rev": float(f["z_rev"]) if f and f["z_rev"] is not None else 0.0,
        "avg_dollar_vol_60d": liq if liq is not None else 0.0,
        "market_cap": market_cap if market_cap is not None else 0.0,
        "sector": sector,
    }
    # Cross-sectional revisions z-score from view, if available
    try:
        zr = await conn.fetchrow(
            "SELECT z_rev_eps FROM fmp_revisions_cs WHERE symbol=$1",
            ticker,
        )
        if zr and zr["z_rev_eps"] is not None:
            feat["z_rev_eps"] = float(zr["z_rev_eps"]) or 0.0
        else:
            feat["z_rev_eps"] = None
    except Exception:
        feat["z_rev_eps"] = None
    # Analyst estimates revision signal (EPS/Revenue average delta)
    try:
        est = await conn.fetch(
            """
            SELECT date, payload
            FROM fmp_analyst_estimates
            WHERE symbol=$1 AND date >= (CURRENT_DATE - INTERVAL '120 days')
            ORDER BY date DESC
            LIMIT 6
            """,
            ticker,
        )
        eps_vals = []
        rev_vals = []
        for r in est:
            p = r["payload"] or {}
            eps = p.get("estimatedEpsAvg") or p.get("estimatedEpsLow") or p.get("estimatedEpsHigh")
            rev = p.get("estimatedRevenueAvg") or p.get("estimatedRevenueLow") or p.get("estimatedRevenueHigh")
            if isinstance(eps, (int, float)):
                eps_vals.append(float(eps))
            if isinstance(rev, (int, float)):
                rev_vals.append(float(rev))
        def delta(vals):
            if len(vals) >= 2:
                return (vals[0] - vals[-1]) / (abs(vals[-1]) + 1e-6)
            return 0.0
        feat["rev_eps_chg"] = 0.5*delta(eps_vals) + 0.5*delta(rev_vals)
    except Exception:
        feat["rev_eps_chg"] = 0.0
    # Rating score (normalize ~0..5 to -1..1)
    try:
        r = await conn.fetchrow(
            """
            SELECT payload
            FROM fmp_rating
            WHERE symbol=$1
            ORDER BY date DESC
            LIMIT 1
            """,
            ticker,
        )
        score = None
        if r and r["payload"]:
            s = r["payload"].get("ratingScore")
            if isinstance(s, (int, float)):
                score = float(s)
        if score is not None:
            feat["rating_score"] = max(-1.0, min(1.0, (score - 2.5)/2.5))
        else:
            feat["rating_score"] = 0.0
    except Exception:
        feat["rating_score"] = 0.0
    return feat

def _score(feat, regime: str) -> float:
    # Blend short/med momentum, sentiment, and quality; clamp to [-1,1]
    q = (
        0.15*feat["z_roe"] + 0.10*feat["z_roa"] + 0.10*feat["z_gm"]
        - 0.10*feat["z_dta"] + 0.10*feat["z_rev"]
    )
    rev_term = feat.get("z_rev_eps") if feat.get("z_rev_eps") is not None else feat.get("rev_eps_chg", 0.0)
    s = 0.5*feat["mom_s"] + 0.2*feat["mom_m"] + 0.15*q + 0.1*feat["senti"] + 0.06*rev_term + 0.04*feat.get("rating_score", 0.0)
    # Sector/regime tilt
    sector = (feat.get("sector") or "").upper()
    if regime == "on" and sector in {"UTILITIES", "REAL ESTATE"}:
        s -= 0.05
    if regime == "off" and sector in {"TECHNOLOGY", "INFORMATION TECHNOLOGY", "INDUSTRIALS"}:
        s -= 0.03
    return max(-1.0, min(1.0, s))

async def _current_regime(conn: asyncpg.Connection) -> str:
    try:
        row = await conn.fetchrow(
            """
            SELECT value FROM policy_knobs
            WHERE key='risk_regime' AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY ts DESC LIMIT 1
            """
        )
        if row:
            v = row["value"]
            if isinstance(v, dict) and "value" in v:
                return str(v["value"]).lower()
            if isinstance(v, str):
                try:
                    j = json.loads(v)
                    if isinstance(j, dict) and "value" in j:
                        return str(j["value"]).lower()
                except Exception:
                    return v.lower()
            return "on"
    except Exception:
        pass
    return "on"

async def generate_recommendations() -> None:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        regime = await _current_regime(conn)
        tickers = await _universe(conn)
        llm_reviews = await _weekly_llm_reviews(conn, tickers)
        recos = []
        for t in tickers:
            feat = await _features(conn, t)
            if not feat:
                continue
            # Liquidity and cap guards
            if feat["market_cap"] and feat["market_cap"] < MIN_MKT_CAP:
                continue
            if feat["avg_dollar_vol_60d"] and feat["avg_dollar_vol_60d"] < MIN_DOLLAR_VOL:
                continue
            # Optional turnaround filter
            if ENABLE_TURNAROUND:
                if not (feat["z_rev"] > 0 and feat["mom_s"] > 0 and feat["mom_m"] > 0):
                    continue
            s = _score(feat, regime)
            side = "buy" if s >= 0 else "sell"
            review_obj = llm_reviews.get(t.upper())
            if isinstance(review_obj, dict):
                review = review_obj
                if review.get("risk_flag") == "avoid":
                    continue
                sentiment = float(review.get("sentiment") or 0.0)
                s += 0.05 * sentiment
                stance = (review.get("stance") or "neutral").lower()
                if stance == "bearish" and s > 0:
                    s *= 0.8
                elif stance == "bullish" and s < 0:
                    s *= 0.8
            else:
                review = None
            recos.append((t, side, s, feat, review))
        # Rank by absolute score and take top-k
        recos.sort(key=lambda r: -abs(r[2]))
        recos = recos[:MAX_RECO]
        now = dt.datetime.now(dt.timezone.utc)
        await conn.executemany(
            """
            INSERT INTO strategist_recos(ts, ticker, side, score, horizon, reason, meta)
            VALUES($1,$2,$3,$4,$5,$6,$7)
            """,
            [
                (
                    now,
                    t,
                    side,
                    float(score),
                    "15m",
                    f"blend(mom_q_senti); regime={regime}",
                    json.dumps({
                        "feat": feat,
                        "llm": review,
                        "contrib": {
                            "mom_s": feat["mom_s"],
                            "mom_m": feat["mom_m"],
                            "z_roe": feat["z_roe"],
                            "z_roa": feat["z_roa"],
                            "z_gm": feat["z_gm"],
                            "z_dta": feat["z_dta"],
                            "z_rev": feat["z_rev"],
                        },
                    }),
                )
                for (t, side, score, feat, review) in recos
            ],
        )
        print(f"strategist recos: wrote {len(recos)} regime={regime}")
    finally:
        await conn.close()
