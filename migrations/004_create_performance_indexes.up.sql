-- Migration 004: Create performance indexes to optimize queries
BEGIN;

-- Composite index for metrics lookup by campaign and date
CREATE INDEX IF NOT EXISTS idx_daily_metrics_campaign_date ON daily_metrics(campaign_id, metric_date);

-- Composite index for media plan lookup by campaign and date
CREATE INDEX IF NOT EXISTS idx_media_plan_campaign_date ON media_plan(campaign_id, plan_date);

-- Index on campaigns.created_at for recent campaigns queries
CREATE INDEX IF NOT EXISTS idx_campaigns_created_at ON campaigns(created_at);

-- GIN index on parsed_campaign JSONB to speed up attribute filtering
CREATE INDEX IF NOT EXISTS idx_campaigns_parsed_gin ON campaigns USING GIN(parsed_campaign);

COMMIT;
