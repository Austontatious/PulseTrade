#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys, csv, datetime as dt, calendar
import psycopg2

DEFAULT_SYMBOLS = "/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt"

def db_url() -> str:
    return (os.getenv("DATABASE_URL")
            or f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}"
               f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}")

def read_symbols(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        out = []
        seen = set()
        for line in f:
            s = line.strip().upper()
            if s and not s.startswith("#") and s not in seen:
                seen.add(s); out.append(s)
        return out

SQL = """
WITH horizon AS (
  SELECT (CURRENT_DATE - (%s || ' days')::interval)::date AS start_date
),
per_symbol AS (
  SELECT dr.symbol, COUNT(*) AS have_days, MIN(ds) AS first_day, MAX(ds) AS last_day
  FROM daily_returns dr, horizon
  WHERE ds >= horizon.start_date
    AND dr.symbol IN (SELECT symbol FROM tmp_universe)
  GROUP BY dr.symbol
)
SELECT u.symbol AS ticker,
       COALESCE(p.have_days, 0) AS have_days,
       p.first_day,
       p.last_day
FROM tmp_universe u
LEFT JOIN per_symbol p ON p.symbol = u.symbol
ORDER BY have_days ASC, ticker
LIMIT %s;
"""

def expected_trading_days(start_date: dt.date, end_date: dt.date) -> int:
    """Count weekdays (Mon-Fri) between two dates inclusive."""
    d = start_date
    count = 0
    while d <= end_date:
        if calendar.weekday(d.year, d.month, d.day) < 5:
            count += 1
        d += dt.timedelta(days=1)
    return count

def main():
    ap = argparse.ArgumentParser(description="Coverage report for Alpaca universe over last N days.")
    ap.add_argument("--symbols-file", default=DEFAULT_SYMBOLS)
    ap.add_argument("--lookback", type=int, default=120)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--csv-out", default="/mnt/data/PulseTrade/db/reports/coverage_report.csv")
    args = ap.parse_args()

    syms = read_symbols(args.symbols_file)
    if not syms:
        print("No symbols found in symbols file.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(args.csv_out), exist_ok=True)
    conn = psycopg2.connect(db_url())
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE tmp_universe(symbol text primary key);")
            # batch insert symbols
            batch = 1000
            for i in range(0, len(syms), batch):
                chunk = [(s,) for s in syms[i:i+batch]]
                cur.executemany("INSERT INTO tmp_universe(symbol) VALUES (%s) ON CONFLICT DO NOTHING;", chunk)

            # main report
            cur.execute(SQL, (str(args.lookback), args.limit))
            fetched = cur.fetchall()

        rows = [
            {"ticker": t, "have_days": have, "first_day": first, "last_day": last}
            for (t, have, first, last) in fetched
        ]

        today = dt.date.today()
        window_start = today - dt.timedelta(days=args.lookback)
        slack = 3
        full_cov = 0
        for r in rows:
            have = int(r["have_days"] or 0)
            first = r["first_day"]
            start = max(first or window_start, window_start)
            expected = expected_trading_days(start, today)
            missing = max(expected - have, 0)
            is_full = have + slack >= expected
            r.update(
                {
                    "expected_days": expected,
                    "missing": missing,
                    "is_full": is_full,
                }
            )
            if is_full:
                full_cov += 1

        total = len(rows)

        # Print a quick console preview
        print(
            f"[coverage] Lookback={args.lookback}d  full_coverage(relative slack={slack})={full_cov} / total_listed={total}"
        )
        for r in rows[:10]:
            print(
                f"{r['ticker']:<8} have_days={r['have_days']:>3} expected={r['expected_days']:>3} "
                f"missing={r['missing']:>3} first={r['first_day']} last={r['last_day']}"
            )

        # Write CSV
        with open(args.csv_out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(
                [
                    "ticker",
                    "have_days",
                    "expected_days",
                    "missing_days",
                    "is_full",
                    "first_day",
                    "last_day",
                    "lookback_days",
                    "generated_at",
                ]
            )
            now = dt.datetime.utcnow().isoformat() + "Z"
            for row in rows:
                w.writerow(
                    [
                        row["ticker"],
                        row["have_days"],
                        row["expected_days"],
                        row["missing"],
                        "true" if row["is_full"] else "false",
                        row["first_day"] or "",
                        row["last_day"] or "",
                        args.lookback,
                        now,
                    ]
                )

        conn.commit()
        print(f"[coverage] Wrote {len(rows)} rows -> {args.csv_out}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
