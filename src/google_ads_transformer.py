"""
Transformation layer for Google Ads API data.
"""
from typing import Dict, Any


def transform_campaign(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform raw Google Ads campaign data into DB-ready format.
    """
    # raw includes 'campaign_id', 'campaign_name', 'status', 'advertising_channel_type'
    from .campaign_parser import parse_campaign_name, determine_campaign_type_and_goal

    parsed = parse_campaign_name(raw.get('campaign_name', ''))
    meta = determine_campaign_type_and_goal(parsed, '')
    return {
        'campaign_id': raw.get('campaign_id'),
        'campaign_name': raw.get('campaign_name'),
        'parsed_campaign': parsed,
        'account_id': raw.get('customer_id'),
        'objective': None,
        'status': raw.get('status'),
        'daily_budget': None,
        'adset_id': None,
        'adset_name': None,
        'ad_id': None,
        'campaign_type': meta.get('type'),
        'campaign_goal': meta.get('goal'),
        'source': meta.get('source'),
        'restaurant': meta.get('restaurant'),
        'city': parsed.get('city')
    }


def transform_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform raw Google Ads metrics into DB-ready format matching daily_metrics schema.
    """
    date = row.get('date')
    impressions = row.get('impressions', 0)
    clicks = row.get('clicks', 0)
    cost_micros = row.get('cost_micros', 0)
    spend = cost_micros / 1e6
    cpm = (spend / impressions * 1000) if impressions else 0
    cpc = (spend / clicks) if clicks else 0
    ctr = (clicks / impressions) if impressions else 0
    return {
        'campaign_id': row.get('campaign_id'),
        'metric_date': date,
        'impressions': int(impressions),
        'clicks': int(clicks),
        'spend': float(spend),
        'reach': None,
        'cpm': float(cpm),
        'cpc': float(cpc),
        'ctr': float(ctr)
    }
