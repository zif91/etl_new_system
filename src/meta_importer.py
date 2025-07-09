"""
Meta data importer: fetch insights data for campaigns and persist to database.
"""
from typing import List, Dict, Optional
from .meta_client import init_meta_api, fetch_campaign_insights
from .db import insert_daily_metrics


def import_meta_insights(
    account_id: str,
    campaign_ids: List[str],
    params: Optional[Dict] = None
) -> List[Dict]:
    """
    Retrieve daily insights for given campaigns from Meta Marketing API and insert into database.

    Args:
        account_id: Facebook Ad Account ID (without 'act_' prefix).
        campaign_ids: List of campaign IDs to fetch insights for.
        params: Optional query parameters for insights API (e.g., date range).
    Returns:
        List of metrics dicts with keys matching daily_metrics schema.
    """
    # Initialize API client
    init_meta_api()

    # Prepare query params with daily breakdown
    query_params = params.copy() if params else {}
    query_params.setdefault('time_increment', 1)

    metrics_list: List[Dict] = []
    for cid in campaign_ids:
        insights = fetch_campaign_insights(account_id, cid, params=query_params)
        for rec in insights:
            metrics_list.append({
                'campaign_id': cid,
                'metric_date': rec.get('date_start'),
                'impressions': int(rec.get('impressions', 0)),
                'clicks': int(rec.get('clicks', 0)),
                'spend': float(rec.get('spend', 0.0)),
                'reach': int(rec.get('reach', 0)),
                'cpm': float(rec.get('cpm', 0.0)),
                'cpc': float(rec.get('cpc', 0.0)),
                'ctr': float(rec.get('ctr', 0.0))
            })

    # Persist metrics to database
    insert_daily_metrics(metrics_list)

    return metrics_list
