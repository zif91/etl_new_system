"""
Transformer for GA4 API response records.
"""
from typing import List, Dict, Any


def transform_ga4_record(record: List[Any]) -> Dict[str, Any]:
    """
    Convert a GA4 report row (list of values) into a dict matching storage schema.
    Expects record in order: date, source, medium, campaign, sessions, users, conversions, purchase_revenue
    """
    date, source, medium, campaign, sessions, users, conversions, revenue = record
    return {
        'date': date,
        'utm_source': source,
        'utm_medium': medium,
        'utm_campaign': campaign,
        'sessions': int(sessions),
        'users': int(users),
        'conversions': int(conversions),
        'purchase_revenue': float(revenue)
    }
