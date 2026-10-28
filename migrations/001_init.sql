CREATE TABLE IF NOT EXISTS subscriptions (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    condition VARCHAR(8) NOT NULL CHECK (condition IN ('GT', 'LT')),
    threshold NUMERIC(18, 8) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    triggered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_symbol_active ON subscriptions (symbol, active);
CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions (user_id);

CREATE TABLE IF NOT EXISTS alert_events (
    id VARCHAR(64) PRIMARY KEY,
    subscription_id VARCHAR(64) NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    user_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    condition VARCHAR(8) NOT NULL,
    threshold NUMERIC(18, 8) NOT NULL,
    actual_price NUMERIC(18, 8) NOT NULL,
    source VARCHAR(32) NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_events_user_id ON alert_events (user_id, triggered_at DESC);
