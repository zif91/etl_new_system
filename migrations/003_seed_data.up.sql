-- Migration 003: Seed initial data
BEGIN;

-- Sample campaign
INSERT INTO campaigns (campaign_id, campaign_name, parsed_campaign, account_id, objective, status, daily_budget, adset_id, adset_name, ad_id, campaign_type, campaign_goal, source, restaurant, city)
VALUES (
  'camp_001',
  'TestCampaign | CPC | CityX | RestaurantY | AudienceZ | Promo',
  '{"platform":"TestCampaign","channel":"CPC","city":"CityX","restaurant":"RestaurantY","audience_type":"AudienceZ","additional":"Promo"}',
  'acct_001',
  'OUTCOME_SALES',
  'ACTIVE',
  1000,
  'aset_001',
  'TestAdSet',
  'ad_001',
  'Performance',
  'Заказы',
  'TestSource',
  'RestaurantY',
  'CityX'
);

-- Sample media plan entry
INSERT INTO media_plan (campaign_id, plan_date, planned_budget, planned_impressions, planned_clicks)
VALUES ('camp_001', CURRENT_DATE, 1000, 10000, 150);

-- Sample daily metric entry
INSERT INTO daily_metrics (campaign_id, metric_date, impressions, clicks, spend, reach, cpm, cpc, ctr)
VALUES ('camp_001', CURRENT_DATE, 8000, 120, 400, 7500, 50, 3.33, 1.5);

-- Sample promo order
INSERT INTO promo_orders (campaign_id, order_id, promo_code, order_date, revenue)
VALUES ('camp_001', 'order_001', 'PROMO10', CURRENT_DATE, 500);

COMMIT;
