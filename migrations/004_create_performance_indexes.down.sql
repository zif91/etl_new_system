-- Migration 004 DOWN: Drop performance indexes
BEGIN;

DROP INDEX IF EXISTS idx_daily_metrics_campaign_date;
DROP INDEX IF EXISTS idx_media_plan_campaign_date;
DROP INDEX IF EXISTS idx_campaigns_created_at;
DROP INDEX IF EXISTS idx_campaigns_parsed_gin;

COMMIT;
