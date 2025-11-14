CREATE TABLE IF NOT EXISTS llm_calls (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  prompt_key TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  prompt_hash TEXT NOT NULL,
  input_payload JSONB NOT NULL,
  output_text TEXT,
  output_json JSONB,
  model TEXT NOT NULL,
  temperature REAL NOT NULL,
  top_p REAL NOT NULL,
  max_output_tokens INT NOT NULL,
  latency_ms INT,
  tokens_prompt INT,
  tokens_output INT,
  success BOOLEAN NOT NULL DEFAULT FALSE,
  error TEXT
);

CREATE INDEX IF NOT EXISTS llm_calls_prompt_idx ON llm_calls (prompt_key, prompt_version);
CREATE INDEX IF NOT EXISTS llm_calls_hash_idx ON llm_calls (prompt_hash);
CREATE INDEX IF NOT EXISTS llm_calls_ts_idx ON llm_calls (ts);
