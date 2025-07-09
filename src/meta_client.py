"""
Meta API client initialization and helper functions.
"""
import os
import time
import logging
import threading
from time import perf_counter
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError


# Конфигурирование логгирования
logger = logging.getLogger(__name__)


def init_meta_api():
    """
    Initialize Facebook Ads API client using credentials from environment variables.
    Expects environment variables:
    - FACEBOOK_APP_ID
    - FACEBOOK_APP_SECRET
    - FACEBOOK_ACCESS_TOKEN
    """
    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')

    if not all([app_id, app_secret, access_token]):
        raise EnvironmentError(
            'Missing Facebook API credentials. Please set FACEBOOK_APP_ID, FACEBOOK_APP_SECRET, and FACEBOOK_ACCESS_TOKEN.'
        )

    FacebookAdsApi.init(app_id, app_secret, access_token)

    return FacebookAdsApi.get_default_api()


def retry_request(retries: int = 3, delay: float = 1.0):
    """
    Декоратор для повторных попыток в случае ошибки FacebookRequestError с экспоненциальным бэкоффом.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            _delay = delay
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except FacebookRequestError as e:
                    if attempt < retries - 1:
                        time.sleep(_delay)
                        _delay *= 2
                    else:
                        raise
        return wrapper
    return decorator


def safe_execute(func):
    """
    Декоратор для безопасного выполнения функций API с логированием ошибок и возвратом пустого списка в случае исключений.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            return []
    return wrapper


class RateLimiter:
    """
    Decorator to limit function calls to max_calls per period (in seconds).
    """
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self.lock:
                now = perf_counter()
                # Remove timestamps outside of the current period
                self.calls = [t for t in self.calls if now - t < self.period]
                if len(self.calls) >= self.max_calls:
                    # Sleep until a slot is available
                    sleep_time = self.period - (now - self.calls[0])
                    time.sleep(sleep_time)
                self.calls.append(perf_counter())
            return func(*args, **kwargs)
        return wrapper


# Apply rate limiter: e.g., max 50 calls per minute
RateLimited = RateLimiter(max_calls=50, period=60)


# Применяем safe_execute для безопасного вызова
@safe_execute
@retry_request(retries=5, delay=1)
@RateLimited
def fetch_campaign_data(account_id: str, fields=None, params=None) -> list:
    """
    Fetch campaign data for given ad account.
    account_id: строка без префикса 'act_'
    fields: список полей для запроса, например ['id', 'name', 'status', 'objective', 'daily_budget']
    params: дополнительные параметры, например {'effective_status': ['ACTIVE']}
    Возвращает список словарей с полями кампаний.
    """
    if fields is None:
        fields = ['id', 'name', 'status', 'objective', 'daily_budget']
    if params is None:
        params = {}
    ad_account = AdAccount(f'act_{account_id}')
    campaigns = []
    # Итеративно получаем все страницы
    for campaign in ad_account.get_campaigns(fields=fields, params=params):
        campaigns.append(campaign)
    return campaigns


@safe_execute
@retry_request(retries=5, delay=1)
@RateLimited
def fetch_campaign_insights(account_id: str, campaign_id: str, fields=None, params=None) -> list:
    """
    Fetch campaign insights data for given campaign ID.
    account_id: строка без префикса 'act_'
    campaign_id: ID кампании
    fields: список полей-метрик, например ['impressions','clicks','spend','reach','frequency','cpm','cpc','ctr']
    params: дополнительные параметры, например {'date_preset':'last_30d','level':'campaign'}
    Возвращает список инсайтов в виде словарей.
    """
    if fields is None:
        fields = ['impressions', 'clicks', 'spend', 'reach', 'frequency', 'cpm', 'cpc', 'ctr']
    default_filter = [{'field':'campaign.id','operator':'IN','value':[campaign_id]}]
    if params is None:
        params = {
            'level': 'campaign',
            'filtering': default_filter,
            'date_preset': 'last_30d'
        }
    else:
        params.setdefault('filtering', default_filter)
    ad_account = AdAccount(f'act_{account_id}')
    insights = []
    for record in ad_account.get_insights(fields=fields, params=params):
        insights.append(record)
    return insights


if __name__ == '__main__':
    api = init_meta_api()
    print('Facebook Ads API initialized:', api)
