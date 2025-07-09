"""
Google Analytics 4 (GA4) Data API client initialization using service account.
"""
import os
from google.analytics.data import BetaAnalyticsDataClient
from google.oauth2 import service_account
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric


def init_ga4_client():
    """
    Initialize GA4 Data API client using service account credentials.
    Expects GA4_CREDENTIALS_JSON in environment pointing to JSON key file.
    """
    creds_path = os.getenv('GA4_CREDENTIALS_JSON')
    if not creds_path or not os.path.isfile(creds_path):
        raise EnvironmentError('GA4_CREDENTIALS_JSON not set or file does not exist')
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    client = BetaAnalyticsDataClient(credentials=credentials)
    return client


def run_ga4_report(start_date: str, end_date: str, dimensions: list, metrics: list, filters: dict = None) -> list:
    """
    Выполнить запрос RunReport к GA4 Data API.
    start_date, end_date: строки в формате 'YYYY-MM-DD'
    dimensions: список имен измерений, например ['sessionSource', 'sessionCampaign']
    metrics: список имен метрик, например ['sessions', 'conversions', 'purchaseRevenue']
    filters: необязательный словарь фильтров в формате API
    Возвращает список строк отчета в виде списков значений.
    """
    client = init_ga4_client()
    property_id = os.getenv('GA4_PROPERTY_ID')
    if not property_id:
        raise EnvironmentError('GA4_PROPERTY_ID not set')

    request = RunReportRequest(
        property=f'properties/{property_id}',
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
    )
    if filters:
        request.dimension_filter = filters

    response = client.run_report(request=request)
    # Преобразуем строки ответа в список
    results = []
    for row in response.rows:
        results.append([val.string_value for val in row.dimension_values] + [val.value for val in row.metric_values])
    return results

if __name__ == '__main__':
    client = init_ga4_client()
    print('GA4 client initialized:', client)
