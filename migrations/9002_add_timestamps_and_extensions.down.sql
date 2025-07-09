-- Migration DOWN 002: Remove triggers and extension for timestamp updates
BEGIN;

-- Drop triggers on core tables
DROP TRIGGER IF EXISTS set_timestamp_campaigns ON campaigns;
DROP TRIGGER IF EXISTS set_timestamp_daily_metrics ON daily_metrics;
DROP TRIGGER IF EXISTS set_timestamp_media_plan ON media_plan;
DROP TRIGGER IF EXISTS set_timestamp_promo_orders ON promo_orders;

-- Drop trigger function
DROP FUNCTION IF EXISTS trigger_set_timestamp();

-- Disable uuid-ossp extension if not used elsewhere
DROP EXTENSION IF EXISTS "uuid-ossp";

COMMIT;
