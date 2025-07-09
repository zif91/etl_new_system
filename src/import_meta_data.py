"""
Module for importing campaign data from Meta Marketing API.
"""
from typing import List, Dict, Optional
from .meta_client import init_meta_api, fetch_campaign_data
from .meta_transformer import transform_campaign
from .db import upsert_campaign


def import_meta_data(
    account_id: str,
    fields: Optional[List[str]] = None,
    params: Optional[Dict] = None
) -> List[Dict]:
    """
    Retrieve basic campaign information from Meta Marketing API.

    Args:
        account_id: The numeric Facebook Ad Account ID (without 'act_' prefix).
        fields: List of Campaign fields to fetch. Defaults to id, name, status, objective.
        params: Additional query parameters (e.g., date_preset, filtering).
    Returns:
        A list of dicts with campaign data.
    """
    # Initialize API client
    init_meta_api()

    # Prepare query fields and params
    # Default fields to fetch if not provided
    default_fields = ['id', 'name', 'status', 'objective', 'daily_budget', 'targeting']
    query_fields = fields or default_fields
    query_params = params or {}

    # Fetch campaign data with retry and error handling
    campaigns_data = fetch_campaign_data(account_id, fields=query_fields, params=query_params)
    transformed: List[Dict] = []
    for raw in campaigns_data:
        campaign = transform_campaign(raw)
        # Persist to database
        upsert_campaign(campaign)
        transformed.append(campaign)
    return transformed
