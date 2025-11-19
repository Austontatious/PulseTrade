CREATE TABLE IF NOT EXISTS universe_monte_carlo (
    as_of date NOT NULL,
    symbol text NOT NULL,
    rank integer,
    mean_profit double precision NOT NULL,
    profit_std_dev double precision NOT NULL,
    best_score double precision NOT NULL,
    mean_return double precision,
    return_std_dev double precision,
    sim_count integer,
    sim_days integer,
    llm_risk_flag text,
    llm_sentiment double precision,
    meta jsonb,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of, symbol)
);

CREATE INDEX IF NOT EXISTS idx_universe_monte_carlo_asof_rank
    ON universe_monte_carlo (as_of DESC, rank ASC NULLS LAST);
