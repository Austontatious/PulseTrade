CREATE TABLE IF NOT EXISTS llm_symbol_reviews (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL,
    symbol TEXT NOT NULL,
    as_of DATE NOT NULL,
    prompt_key TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    output_json JSONB,
    output_text TEXT,
    input_payload JSONB,
    extra JSONB,
    cached BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS llm_symbol_reviews_scope_symbol_asof_idx
    ON llm_symbol_reviews(scope, symbol, as_of);

CREATE INDEX IF NOT EXISTS llm_symbol_reviews_scope_asof_idx
    ON llm_symbol_reviews(scope, as_of);
