import pytest
from src.campaign_parser import (
    parse_campaign_name,
    determine_campaign_type_and_goal,
    CHANNEL_TO_TYPE,
    TYPE_TO_GOAL,
    OBJECTIVE_TO_GOAL
)

@ pytest.mark.parametrize("input_name, expected", [
    (
        "Instagram | CPM | Almaty | Bella | Interests | Promo",
        {
            'platform': 'Instagram',
            'channel': 'CPM',
            'city': 'Almaty',
            'restaurant': 'Bella',
            'audience_type': 'Interests',
            'additional': 'Promo'
        }
    ),
    (
        "Search|CPC|Astana|Tanuki|No_Brand",
        {
            'platform': 'Search',
            'channel': 'CPC',
            'city': 'Astana',
            'restaurant': 'Tanuki',
            'audience_type': 'No_Brand',
            'additional': None
        }
    ),
    (
        "NoSeparatorCampaign",
        {
            'platform': 'NoSeparatorCampaign',
            'channel': None,
            'city': None,
            'restaurant': None,
            'audience_type': None,
            'additional': None
        }
    ),
    (12345, 12345),
])
def test_parse_campaign_name(input_name, expected):
    result = parse_campaign_name(input_name)
    assert result == expected

@ pytest.mark.parametrize("parsed, objective, expected_goal", [
    ({'channel': 'CPM', 'platform': 'Instagram', 'city': 'Almaty', 'restaurant': 'Bella'}, 'OUTCOME_AWARENESS', 'Охват/Узнаваемость'),
    ({'channel': 'CPC', 'platform': 'Search', 'city': 'Astana', 'restaurant': 'Tanuki'}, 'OUTCOME_SALES', 'Заказы'),
    ({'channel': None, 'platform': 'Unknown', 'city': 'Tashkent', 'restaurant': None}, 'VIDEO_VIEWS', 'Просмотры видео'),
])
def test_determine_campaign_type_and_goal(parsed, objective, expected_goal):
    details = determine_campaign_type_and_goal(parsed, objective)
    assert details['goal'] == expected_goal
    # type from channel mapping
    if parsed.get('channel') in CHANNEL_TO_TYPE:
        assert details['type'] == CHANNEL_TO_TYPE[parsed['channel']]
    else:
        assert details['type'] is None

def test_objective_to_goal_fallback():
    # If channel not mapped, goal falls back to OBJECTIVE_TO_GOAL
    parsed = {'channel': 'UNKNOWN'}
    objective = 'ENGAGEMENT'
    details = determine_campaign_type_and_goal(parsed, objective)
    assert details['goal'] == OBJECTIVE_TO_GOAL[objective]

if __name__ == '__main__':
    pytest.main()
