"""
AppsFlyer Reporting API client.
"""
import os
import time
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

import requests

logger = logging.getLogger(__name__)

class AppsFlyerClient:
    """
    Клиент для работы с AppsFlyer Reporting API.
    """

    BASE_URL = "https://hq.appsflyer.com/api"
    DEFAULT_TIMEOUT = 30  # Тайм-аут запросов в секундах
    MAX_RETRIES = 3  # Максимальное количество повторных попыток

    def __init__(self, api_token: Optional[str] = None, app_id: Optional[str] = None):
        """
        Инициализирует клиент AppsFlyer API.

        Args:
            api_token: API токен AppsFlyer (если не указан, берется из переменной среды APPSFLYER_API_TOKEN)
            app_id: Идентификатор приложения (если не указан, берется из переменной среды APPSFLYER_APP_ID)
        """
        self.api_token = api_token or os.getenv("APPSFLYER_API_TOKEN")
        self.app_id = app_id or os.getenv("APPSFLYER_APP_ID")
        
        if not self.api_token:
            raise ValueError("API token not provided and APPSFLYER_API_TOKEN not set in environment")
        if not self.app_id:
            raise ValueError("App ID not provided and APPSFLYER_APP_ID not set in environment")
        
        # Заголовки для всех запросов
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Выполняет HTTP-запрос к API с обработкой ошибок и повторами.
        
        Args:
            endpoint: Конечная точка API (путь после BASE_URL)
            params: Параметры запроса
            
        Returns:
            Ответ API в формате словаря
        """
        url = f"{self.BASE_URL}{endpoint}"
        retries = 0
        backoff_factor = 2  # для экспоненциального backoff
        
        while retries <= self.MAX_RETRIES:
            try:
                logger.debug(f"Making request to {url} with params {params}")
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.DEFAULT_TIMEOUT
                )
                
                # Проверяем код ответа
                response.raise_for_status()
                
                # Пытаемся разобрать ответ как JSON
                return response.json()
            
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                
                # Обрабатываем различные ошибки
                if status_code == 429:  # Too Many Requests
                    retry_after = int(e.response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limit exceeded, retrying after {retry_after} seconds")
                    time.sleep(retry_after)
                    retries += 1
                    continue
                    
                elif status_code == 401:  # Unauthorized
                    logger.error("Authentication error: Invalid API token")
                    raise ValueError("Invalid API token") from e
                    
                elif status_code == 403:  # Forbidden
                    logger.error("Authorization error: Insufficient permissions")
                    raise ValueError("Insufficient permissions") from e
                    
                elif status_code == 404:  # Not Found
                    logger.error(f"Resource not found: {url}")
                    raise ValueError(f"Resource not found: {url}") from e
                    
                elif 500 <= status_code < 600:  # Server errors
                    if retries < self.MAX_RETRIES:
                        wait_time = backoff_factor ** retries
                        logger.warning(f"Server error ({status_code}), retrying in {wait_time} seconds")
                        time.sleep(wait_time)
                        retries += 1
                        continue
                    else:
                        logger.error(f"Server error after {self.MAX_RETRIES} retries")
                        raise
                else:
                    # Неизвестная ошибка
                    logger.error(f"HTTP error: {e}")
                    raise
                    
            except requests.exceptions.RequestException as e:
                # Проблемы с сетью или таймаут
                if retries < self.MAX_RETRIES:
                    wait_time = backoff_factor ** retries
                    logger.warning(f"Request failed: {e}. Retrying in {wait_time} seconds")
                    time.sleep(wait_time)
                    retries += 1
                    continue
                else:
                    logger.error(f"Request failed after {self.MAX_RETRIES} retries: {e}")
                    raise
            
            except json.JSONDecodeError:
                logger.error("Failed to parse API response as JSON")
                raise ValueError("Invalid API response format") from None
        
        raise Exception(f"Request failed after {self.MAX_RETRIES} retries")

    def get_installs_report(self, 
                          start_date: str, 
                          end_date: str,
                          media_source: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает отчет об установках приложения.
        
        Args:
            start_date: Начальная дата в формате 'YYYY-MM-DD'
            end_date: Конечная дата в формате 'YYYY-MM-DD'
            media_source: Опциональный фильтр по источнику трафика
            
        Returns:
            Список данных об установках
        """
        endpoint = f"/install-reports/v5/apps/{self.app_id}/installs"
        
        params = {
            "from": start_date,
            "to": end_date,
            "groupings": "media_source,campaign"
        }
        
        if media_source:
            params["media_source"] = media_source
            
        data = self._make_request(endpoint, params)
        return data.get("results", [])
    
    def get_in_app_events_report(self, 
                               start_date: str, 
                               end_date: str,
                               event_name: Optional[str] = None,
                               media_source: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает отчет о событиях внутри приложения (покупки и др.).
        
        Args:
            start_date: Начальная дата в формате 'YYYY-MM-DD'
            end_date: Конечная дата в формате 'YYYY-MM-DD'
            event_name: Опциональный фильтр по названию события
            media_source: Опциональный фильтр по источнику трафика
            
        Returns:
            Список данных о событиях
        """
        endpoint = f"/in-app-events-report/v5/apps/{self.app_id}/events"
        
        params = {
            "from": start_date,
            "to": end_date,
            "groupings": "media_source,campaign,event_name"
        }
        
        if event_name:
            params["event_name"] = event_name
            
        if media_source:
            params["media_source"] = media_source
            
        data = self._make_request(endpoint, params)
        return data.get("results", [])
    
    def get_retention_report(self, 
                           start_date: str, 
                           end_date: str,
                           media_source: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает отчет о показателях удержания пользователей.
        
        Args:
            start_date: Начальная дата в формате 'YYYY-MM-DD'
            end_date: Конечная дата в формате 'YYYY-MM-DD'
            media_source: Опциональный фильтр по источнику трафика
            
        Returns:
            Список данных о удержании пользователей
        """
        endpoint = f"/cohort-reports/v5/apps/{self.app_id}/retention"
        
        params = {
            "from": start_date,
            "to": end_date,
            "groupings": "media_source,campaign"
        }
        
        if media_source:
            params["media_source"] = media_source
            
        data = self._make_request(endpoint, params)
        return data.get("results", [])
    
    def get_uninstall_report(self, 
                           start_date: str, 
                           end_date: str,
                           media_source: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает отчет о удалениях приложения.
        
        Args:
            start_date: Начальная дата в формате 'YYYY-MM-DD'
            end_date: Конечная дата в формате 'YYYY-MM-DD'
            media_source: Опциональный фильтр по источнику трафика
            
        Returns:
            Список данных о удалениях приложения
        """
        endpoint = f"/uninstall-reports/v5/apps/{self.app_id}/uninstalls"
        
        params = {
            "from": start_date,
            "to": end_date,
            "groupings": "media_source,campaign"
        }
        
        if media_source:
            params["media_source"] = media_source
            
        data = self._make_request(endpoint, params)
        return data.get("results", [])
    
    def get_ltv_report(self, 
                     start_date: str, 
                     end_date: str,
                     days_range: int = 7,
                     media_source: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает отчет о показателях LTV (Lifetime Value).
        
        Args:
            start_date: Начальная дата в формате 'YYYY-MM-DD'
            end_date: Конечная дата в формате 'YYYY-MM-DD'
            days_range: Диапазон дней для расчета LTV (по умолчанию 7)
            media_source: Опциональный фильтр по источнику трафика
            
        Returns:
            Список данных о LTV
        """
        endpoint = f"/ltv-reports/v5/apps/{self.app_id}/ltv"
        
        params = {
            "from": start_date,
            "to": end_date,
            "groupings": "media_source,campaign",
            "days_range": days_range
        }
        
        if media_source:
            params["media_source"] = media_source
            
        data = self._make_request(endpoint, params)
        return data.get("results", [])


def init_appsflyer_client() -> AppsFlyerClient:
    """
    Инициализирует клиент AppsFlyer API с помощью переменных окружения.
    
    Returns:
        Настроенный экземпляр AppsFlyerClient
    """
    api_token = os.getenv('APPSFLYER_API_TOKEN')
    app_id = os.getenv('APPSFLYER_APP_ID')

    if not api_token:
        raise EnvironmentError('APPSFLYER_API_TOKEN not set')
    if not app_id:
        raise EnvironmentError('APPSFLYER_APP_ID not set')
        
    return AppsFlyerClient(api_token=api_token, app_id=app_id)
