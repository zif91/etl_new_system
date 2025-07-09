"""
Transformation layer for Meta API data.
"""
from src.campaign_parser import parse_campaign_name, determine_campaign_type_and_goal
from typing import Dict, Any, List


def transform_campaign(campaign_raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразовать необработанные данные кампании Facebook в формат для хранения в БД.
    campaign_raw: словарь с ключами id, name, status, objective, daily_budget, ...
    """
    parsed = parse_campaign_name(campaign_raw.get('name'))
    meta = determine_campaign_type_and_goal(parsed, campaign_raw.get('objective', ''))

    return {
        'campaign_id': campaign_raw.get('id'),
        'campaign_name': campaign_raw.get('name'),
        'parsed_campaign': parsed,
        'account_id': campaign_raw.get('account_id') or campaign_raw.get('id').split('_')[0],
        'objective': campaign_raw.get('objective'),
        'status': campaign_raw.get('status'),
        'daily_budget': float(campaign_raw.get('daily_budget', 0)) if campaign_raw.get('daily_budget') else None,
        'adset_id': campaign_raw.get('adset_id'),
        'adset_name': campaign_raw.get('adset_name'),
        'ad_id': campaign_raw.get('ad_id'),
        'campaign_type': meta.get('type'),
        'campaign_goal': meta.get('goal'),
        'source': meta.get('source'),
        'restaurant': meta.get('restaurant'),
        'city': parsed.get('city'),
    }


def transform_insights(insights_raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразовать список необработанных инсайтов кампании в формат для хранения в БД.
    Каждый элемент raw должен содержать campaign_id, date_start или date_stop, и метрики.
    """
    transformed: List[Dict[str, Any]] = []
    for record in insights_raw:
        transformed.append({
            'campaign_id': record.get('campaign_id'),
            'metric_date': record.get('date_start') or record.get('date_stop'),
            'impressions': int(record.get('impressions', 0)),
            'clicks': int(record.get('clicks', 0)),
            'spend': float(record.get('spend', 0)),
            'reach': int(record.get('reach', 0)),
            'cpm': float(record.get('cpm', 0)),
            'cpc': float(record.get('cpc', 0)),
            'ctr': float(record.get('ctr', 0)),
        })
    return transformed
