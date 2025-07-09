-- Migration DOWN 003: Remove seed data
BEGIN;

DELETE FROM promo_orders WHERE order_id='order_001';
DELETE FROM daily_metrics WHERE campaign_id='camp_001';
DELETE FROM media_plan WHERE campaign_id='camp_001';
DELETE FROM campaigns WHERE campaign_id='camp_001';

COMMIT;
