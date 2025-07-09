"""
GA4 data importer: fetch and transform GA4 report rows.
"""
from typing import List, Dict
from src.ga4_client import run_ga4_report
from .db import insert_ga4_metrics


def import_ga4_data(start_date: str, end_date: str) -> List[Dict]:
    """
    Fetch GA4 data for given date range and transform into dicts matching schema.
    Returns list of dicts with keys: date, utm_source, utm_medium, utm_campaign, sessions, users, conversions, purchase_revenue, transactions, transaction_ids
    """
    # Specify dimensions and metrics
    dimensions = ['date', 'sessionSourceMedium', 'sessionCampaignName', 'transactionId']
    metrics = ['sessions', 'users', 'conversions', 'purchaseRevenue']

    # Run GA4 report
    rows = run_ga4_report(start_date, end_date, dimensions, metrics)
    result: List[Dict] = []
    # Transform each row
    for row in rows:
        date, source_medium, campaign, transaction_id, sessions, users, conversions, revenue = row
        # Split source/medium
        if '/' in source_medium:
            source, medium = source_medium.split('/', 1)
        else:
            source = source_medium
            medium = None
        result.append({
            'date': date,
            'utm_source': source,
            'utm_medium': medium,
            'utm_campaign': campaign,
            'sessions': int(sessions),
            'users': int(users),
            'conversions': int(conversions),
            'purchase_revenue': float(revenue),
            'transactions': 1,
            'transaction_ids': [transaction_id]
        })
    return result


def import_and_store_ga4_data(start_date: str, end_date: str) -> List[Dict]:
    """
    Fetch GA4 data for the date range and persist to database.

    Args:
        start_date: Start of date range (YYYY-MM-DD).
        end_date: End of date range (YYYY-MM-DD).
    Returns:
        List of GA4 metrics dicts persisted to DB.
    """
    records = import_ga4_data(start_date, end_date)
    insert_ga4_metrics(records)
    return records
