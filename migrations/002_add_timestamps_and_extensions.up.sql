-- Migration 002: Enable extensions and add trigger for updated_at
BEGIN;

-- Enable uuid-ossp if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Add trigger function to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all core tables
CREATE TRIGGER set_timestamp_campaigns
BEFORE UPDATE ON campaigns
FOR EACH ROW EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp_daily_metrics
BEFORE UPDATE ON daily_metrics
FOR EACH ROW EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp_media_plan
BEFORE UPDATE ON media_plan
FOR EACH ROW EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp_promo_orders
BEFORE UPDATE ON promo_orders
FOR EACH ROW EXECUTE PROCEDURE trigger_set_timestamp();

COMMIT;
