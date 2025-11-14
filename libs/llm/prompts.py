"""Prompt templates shared across services."""

RATIONALE_SYSTEM = (
    "You are a disciplined equity research assistant.\n"
    "You must be concise, cite signals or metrics by name, and produce JSON only when asked.\n"
    "Avoid speculation beyond the provided data."
)

RATIONALE_USER_TEMPLATE = (
    "Context:\n"
    "- Ticker: {ticker}\n"
    "- Date: {as_of}\n"
    "- Model Signal: {signal_value} ({signal_label})\n"
    "- Factors (z-scored): {factors}\n"
    "- Recent News (headlines only): {headlines}\n\n"
    "Task:\n"
    "Write a 120-200 word rationale explaining the signal in plain English for a portfolio manager.\n"
    "Do not give advice; explain why the model might be right or wrong.\n"
    "Conclude with a 1-sentence risk note."
)

POLICY_SYSTEM = """You are a risk policy filter. Return ONLY strict JSON with keys: {"allow": bool, "reasons": [str], "flags": [str]}"""

POLICY_USER_TEMPLATE = (
    "Policy:\n"
    "- No trades if upcoming earnings within 3 trading days.\n"
    "- Block if factor dispersion > 2.5 std or data coverage < 85%.\n"
    "- Block if daily liquidity < ${min_liquidity} or borrow constraints present.\n"
    "- Block if signal contradicts top-3 macro regimes.\n\n"
    "Inputs:\n"
    "- Ticker: {ticker}\n"
    "- As Of: {as_of}\n"
    "- Earnings Date: {earnings_date}\n"
    "- Liquidity(USD): {liquidity_usd}\n"
    "- Borrow Available: {borrow_ok}\n"
    "- Factor Dispersion: {factor_dispersion}\n"
    "- Data Coverage: {coverage_pct}\n"
    "- Macro Regime: {macro_regime}\n"
    "- Signal: {signal_label} = {signal_value}\n\n"
    "Return JSON only."
)

SUMMARY_SYSTEM = """You are a financial summarizer. Be factual, neutral, and specific."""

SUMMARY_USER_TEMPLATE = (
    "Summarize the following items for {ticker} in <= 8 bullet points.\n"
    "Prefer numbers, dates, guidance changes, and risk items.\n"
    "Items:\n{items}\n"
)
