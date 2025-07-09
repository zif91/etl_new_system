"""
Google Ads campaign data importer using Google Ads API.
"""
from src.google_ads_client import init_google_ads_client
from typing import List, Dict
from .db import upsert_campaign, insert_daily_metrics
from .google_ads_transformer import transform_campaign as ga_transform_campaign, transform_metrics as ga_transform_metrics


def fetch_google_ads_campaigns(customer_id: str, query: str = None) -> List[Dict]:
    """
    Retrieve campaigns for a given Google Ads customer account.
    customer_id: строка с customer ID в формате '1234567890'
    query: необязательный GAQL-запрос; по умолчанию будет выборка активных кампаний
    Возвращает список словарей с полями: campaign_id, campaign_name, status, advertising_channel_type
    """
    client = init_google_ads_client()
    ga_service = client.get_service("GoogleAdsService")
    if not query:
        query = (
            "SELECT campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type "
            "FROM campaign WHERE campaign.status = 'ENABLED'"
        )
    response = ga_service.search(customer_id=customer_id, query=query)
    campaigns = []
    for row in response:
        campaign = row.campaign
        campaigns.append({
            'campaign_id': str(campaign.id),
            'campaign_name': campaign.name,
            'status': campaign.status.name,
            'advertising_channel_type': campaign.advertising_channel_type.name
        })
    return campaigns


def fetch_google_ads_metrics(customer_id: str, start_date: str = None, end_date: str = None, campaign_id: str = None) -> list:
    """
    Fetch performance metrics from Google Ads for campaigns.
    customer_id: строка customer ID
    start_date, end_date: необязательные даты в формате YYYY-MM-DD
    campaign_id: необязательный ID кампании для фильтрации
    Возвращает список словарей с полями: campaign_id, date, impressions, clicks, cost_micros, conversions, conversion_value, quality_score
    """
    client = init_google_ads_client()
    ga_service = client.get_service("GoogleAdsService")
    # Формируем GAQL-запрос
    query = (
        "SELECT campaign.id, segments.date, metrics.impressions, metrics.clicks, "
        "metrics.cost_micros, metrics.conversions, metrics.conversion_value, metrics.quality_score "
        "FROM campaign "
    )
    where_clauses = []
    if campaign_id:
        where_clauses.append(f"campaign.id = {campaign_id}")
    if start_date and end_date:
        where_clauses.append(f"segments.date BETWEEN '{start_date}' AND '{end_date}'")
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    response = ga_service.search(customer_id=customer_id, query=query)
    metrics_list = []
    for row in response:
        metrics_list.append({
            'campaign_id': str(row.campaign.id),
            'date': row.segments.date.value,
            'impressions': row.metrics.impressions.value,
            'clicks': row.metrics.clicks.value,
            'cost_micros': row.metrics.cost_micros.value,
            'conversions': row.metrics.conversions.value,
            'conversion_value': row.metrics.conversion_value.value,
            'quality_score': row.metrics.quality_score.value
        })
    return metrics_list


def import_google_ads_data(customer_id: str) -> List[Dict]:
    """
    Fetch Google Ads campaigns and persist to database.

    Args:
        customer_id: Google Ads customer ID as string.
    Returns:
        List of campaign dicts inserted into DB.
    """
    raw_campaigns = fetch_google_ads_campaigns(customer_id)
    transformed: List[Dict] = []
    for raw in raw_campaigns:
        record = ga_transform_campaign({**raw, 'customer_id': customer_id})
        upsert_campaign(record)
        transformed.append(record)
    return transformed


def import_and_store_google_ads_metrics(
    customer_id: str,
    start_date: str,
    end_date: str
) -> List[Dict]:
    """
    Fetch Google Ads performance metrics and persist to daily_metrics table.

    Args:
        customer_id: Google Ads customer ID.
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).
    Returns:
        List of metrics dicts inserted.
    """
    rows = fetch_google_ads_metrics(customer_id, start_date=start_date, end_date=end_date)
    metrics_list: List[Dict] = []
    for row in rows:
        record = ga_transform_metrics(row)
        metrics_list.append(record)
    # Persist metrics
    insert_daily_metrics(metrics_list)
    return metrics_list
