-- Migration DOWN: Drop core tables for rollback
BEGIN;

DROP TABLE IF EXISTS promo_orders CASCADE;
DROP TABLE IF EXISTS media_plan CASCADE;
DROP TABLE IF EXISTS daily_metrics CASCADE;
DROP TABLE IF EXISTS campaigns CASCADE;

COMMIT;
