-- Migration 005 DOWN: Drop ga4_metrics table
BEGIN;

DROP TABLE IF EXISTS ga4_metrics CASCADE;

COMMIT;
