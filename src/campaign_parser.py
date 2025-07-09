"""
Core parsing logic for campaign names.
"""
import re
from typing import Any, Dict, Union


def parse_campaign_name(campaign_name: Any) -> Union[Dict[str, Any], Any]:
    """
    Parse campaign name strings into structured components.

    Expected patterns: use '|' or '-' or ',' as separators. Fallback to raw name for all fields on error.
    """
    if not isinstance(campaign_name, str):
        return campaign_name

    separators = ['|', '-', ',']
    for sep in separators:
        if sep in campaign_name:
            parts = [p.strip() for p in campaign_name.split(sep)]
            break
    else:
        parts = [campaign_name.strip()]

    fields = [
        'platform',
        'channel',
        'city',
        'restaurant',
        'audience_type',
        'additional'
    ]
    parsed: Dict[str, Any] = {}
    try:
        for idx, field in enumerate(fields):
            parsed[field] = parts[idx] if idx < len(parts) and parts[idx] else None
    except Exception:
        # Fallback: assign raw name to platform, leave others None
        parsed = {field: None for field in fields}
        parsed['platform'] = campaign_name.strip()
    return parsed


# Mapping dictionaries
CHANNEL_TO_TYPE = {
    'CPM': 'Awareness',
    'CPC': 'Performance',
    'CPС': 'Performance'
}

TYPE_TO_GOAL = {
    'Awareness': 'Охват/Узнаваемость',
    'Performance': 'Заказы'
}

PLATFORM_TO_SOURCE = {
    'Instagram': 'Мета',
    'Facebook': 'Мета',
    'Search': 'Google Search',
    'Network': 'Google Display'
}

CITY_TO_COUNTRY = {
    'Almaty': 'Казахстан',
    'Astana': 'Казахстан',
    'Tashkent': 'Узбекистан'
}

RESTAURANT_STANDARD = {
    'Bella': 'Белла',
    'Tanuki': 'Тануки'
}

# Objective to goal mapping
OBJECTIVE_TO_GOAL = {
    'OUTCOME_AWARENESS': 'Охват/Узнаваемость',
    'OUTCOME_SALES': 'Заказы',
    'VIDEO_VIEWS': 'Просмотры видео',
    'ENGAGEMENT': 'Вовлеченность'
}


def determine_campaign_type_and_goal(parsed: Dict[str, Any], objective: str) -> Dict[str, Any]:
    """
    Determine campaign type, goal, source, country, and standardized restaurant name based on parsed data.
    """
    channel = parsed.get('channel') or ''
    ctype = CHANNEL_TO_TYPE.get(channel)
    # Primary goal from type mapping, fallback to objective mapping
    goal = TYPE_TO_GOAL.get(ctype) or OBJECTIVE_TO_GOAL.get(objective) or objective
    source = PLATFORM_TO_SOURCE.get(parsed.get('platform'), parsed.get('platform'))
    country = CITY_TO_COUNTRY.get(parsed.get('city'))
    restaurant = RESTAURANT_STANDARD.get(parsed.get('restaurant'), parsed.get('restaurant'))
    return {
        'type': ctype,
        'goal': goal,
        'source': source,
        'country': country,
        'restaurant': restaurant
    }


def validate_parsed_campaign(parsed: Dict[str, Any]) -> bool:
    """
    Validate that parsed campaign has required non-empty fields.
    Required fields: platform, channel, city, restaurant.
    Returns True if all required fields are present and non-null.
    """
    required = ['platform', 'channel', 'city', 'restaurant']
    return all(parsed.get(field) for field in required)


if __name__ == '__main__':
    # Example usage
    test_campaigns = [
        "Instagram | CPM | Almaty | Bella | Interests | День Рождения",
        "Search|CPC|Astana|Tanuki|No_Brand",
        "Facebook - CPM - Tashkent - Bella - Interests - Летняя Распродажа",
        "Network,CPC,Almaty,Tanuki,No_Brand"
    ]
    for name in test_campaigns:
        parsed_name = parse_campaign_name(name)
        campaign_details = determine_campaign_type_and_goal(parsed_name, 'Заказы')
        print(name, '->', parsed_name, '->', campaign_details)
