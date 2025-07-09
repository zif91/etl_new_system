-- Migration 005: Create GA4 metrics table
BEGIN;

CREATE TABLE ga4_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    sessions INTEGER,
    users INTEGER,
    conversions INTEGER,
    purchase_revenue NUMERIC,
    transactions INTEGER,
    transaction_ids JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (metric_date, utm_source, utm_medium, utm_campaign, transaction_ids)
);

-- Indexes
CREATE INDEX idx_ga4_metrics_date ON ga4_metrics(metric_date);
CREATE INDEX idx_ga4_metrics_campaign ON ga4_metrics(utm_campaign);

COMMIT;
