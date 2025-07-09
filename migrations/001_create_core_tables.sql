-- Migration script: Create core tables for advertising analytics system
BEGIN;

-- Table: campaigns
CREATE TABLE campaigns (
    id SERIAL PRIMARY KEY,
    campaign_id TEXT NOT NULL UNIQUE,
    campaign_name TEXT NOT NULL,
    parsed_campaign JSONB,
    account_id TEXT,
    objective TEXT,
    status TEXT,
    daily_budget NUMERIC,
    adset_id TEXT,
    adset_name TEXT,
    ad_id TEXT,
    campaign_type TEXT,
    campaign_goal TEXT,
    source TEXT,
    restaurant TEXT,
    city TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table: daily_metrics
CREATE TABLE daily_metrics (
    id SERIAL PRIMARY KEY,
    campaign_id TEXT REFERENCES campaigns(campaign_id),
    metric_date DATE NOT NULL,
    impressions INTEGER,
    clicks INTEGER,
    spend NUMERIC,
    reach INTEGER,
    cpm NUMERIC,
    cpc NUMERIC,
    ctr NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (campaign_id, metric_date)
);

-- Table: media_plan
CREATE TABLE media_plan (
    id SERIAL PRIMARY KEY,
    campaign_id TEXT REFERENCES campaigns(campaign_id),
    plan_date DATE NOT NULL,
    planned_budget NUMERIC,
    planned_impressions INTEGER,
    planned_clicks INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (campaign_id, plan_date)
);

-- Table: promo_orders
CREATE TABLE promo_orders (
    id SERIAL PRIMARY KEY,
    campaign_id TEXT REFERENCES campaigns(campaign_id),
    order_id TEXT NOT NULL,
    promo_code TEXT,
    order_date DATE,
    revenue NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (order_id)
);

-- Indexes for performance
CREATE INDEX idx_daily_metrics_impressions ON daily_metrics (impressions);
CREATE INDEX idx_daily_metrics_clicks ON daily_metrics (clicks);
CREATE INDEX idx_media_plan_planned_budget ON media_plan (planned_budget);
CREATE INDEX idx_promo_orders_promo_code ON promo_orders (promo_code);

-- Добавляем ON DELETE и ON UPDATE каскад для внешних ключей и проверочные ограничения
ALTER TABLE daily_metrics
  DROP CONSTRAINT IF EXISTS daily_metrics_campaign_id_fkey,
  ADD CONSTRAINT daily_metrics_campaign_id_fkey FOREIGN KEY (campaign_id)
    REFERENCES campaigns(campaign_id)
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE media_plan
  DROP CONSTRAINT IF EXISTS media_plan_campaign_id_fkey,
  ADD CONSTRAINT media_plan_campaign_id_fkey FOREIGN KEY (campaign_id)
    REFERENCES campaigns(campaign_id)
    ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE promo_orders
  DROP CONSTRAINT IF EXISTS promo_orders_campaign_id_fkey,
  ADD CONSTRAINT promo_orders_campaign_id_fkey FOREIGN KEY (campaign_id)
    REFERENCES campaigns(campaign_id)
    ON DELETE CASCADE ON UPDATE CASCADE;

-- Проверочные ограничения на неотрицательные значения
ALTER TABLE daily_metrics
  ADD CONSTRAINT chk_daily_metrics_nonneg CHECK (impressions >= 0 AND clicks >= 0 AND spend >= 0 AND reach >= 0 AND cpm >= 0 AND cpc >= 0 AND ctr >= 0);
ALTER TABLE media_plan
  ADD CONSTRAINT chk_media_plan_nonneg CHECK (planned_budget >= 0 AND planned_impressions >= 0 AND planned_clicks >= 0);
ALTER TABLE promo_orders
  ADD CONSTRAINT chk_promo_orders_nonneg CHECK (revenue >= 0);

COMMIT;
