"""
Tests for AppsFlyer API integration.
"""
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.appsflyer_client import AppsFlyerClient
from src.appsflyer_transformer import (
    transform_appsflyer_installs,
    transform_appsflyer_events,
    transform_appsflyer_retention,
    transform_appsflyer_ltv,
    merge_appsflyer_data
)
from src.appsflyer_importer import import_appsflyer_data, import_and_store_appsflyer_data


class TestAppsFlyerClient(unittest.TestCase):
    """Test AppsFlyer API client"""
    
    @patch('src.appsflyer_client.requests.get')
    def test_get_installs_report(self, mock_get):
        """Test fetching installs report"""
        # Настройка мока
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [
                {
                    "date": "2025-07-01",
                    "media_source": "facebook",
                    "campaign": "summer_promo",
                    "installs": 150,
                    "clicks": 2500,
                    "impressions": 50000,
                    "cost": 750.00,
                    "cost_per_install": 5.00
                }
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        # Вызов метода
        client = AppsFlyerClient(api_token="test_token", app_id="test_app_id")
        result = client.get_installs_report("2025-07-01", "2025-07-01")
        
        # Проверка результатов
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["media_source"], "facebook")
        self.assertEqual(result[0]["installs"], 150)
        
        # Проверка, что запрос был выполнен с правильными параметрами
        mock_get.assert_called_once()
        call_args = mock_get.call_args[0][0]
        self.assertIn("/install-reports/v5/apps/test_app_id/installs", call_args)


class TestAppsFlyerTransformer(unittest.TestCase):
    """Test AppsFlyer data transformation"""
    
    def test_transform_installs(self):
        """Test transformation of installs data"""
        raw_data = [
            {
                "date": "2025-07-01",
                "media_source": "facebook",
                "campaign": "summer_promo",
                "installs": 150,
                "clicks": 2500,
                "impressions": 50000,
                "cost": 750.00,
                "cost_per_install": 5.00
            }
        ]
        
        result = transform_appsflyer_installs(raw_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["installs"], 150)
        self.assertEqual(result[0]["media_source"], "facebook")
        self.assertEqual(result[0]["campaign"], "summer_promo")
    
    def test_transform_events(self):
        """Test transformation of in-app events data"""
        raw_data = [
            {
                "date": "2025-07-01",
                "media_source": "facebook",
                "campaign": "summer_promo",
                "event_name": "purchase",
                "event_counter": 50,
                "event_revenue": 2500.00
            }
        ]
        
        result = transform_appsflyer_events(raw_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["purchases"], 50)  # событие "purchase" считается покупкой
        self.assertEqual(result[0]["revenue"], 2500.00)
    
    def test_merge_data(self):
        """Test merging of different data types"""
        installs_data = [
            {
                "date": "2025-07-01",
                "media_source": "facebook",
                "campaign": "summer_promo",
                "installs": 150,
                "clicks": 2500,
                "impressions": 50000,
                "cost": 750.00,
                "cost_per_install": 5.00
            }
        ]
        
        events_data = [
            {
                "date": "2025-07-01",
                "media_source": "facebook",
                "campaign": "summer_promo",
                "purchases": 50,
                "revenue": 2500.00
            }
        ]
        
        result = merge_appsflyer_data(installs_data, events_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["installs"], 150)
        self.assertEqual(result[0]["purchases"], 50)
        self.assertEqual(result[0]["revenue"], 2500.00)


class TestAppsFlyerImporter(unittest.TestCase):
    """Test AppsFlyer data importer"""
    
    @patch('src.appsflyer_importer.init_appsflyer_client')
    def test_import_appsflyer_data(self, mock_init_client):
        """Test importing data from AppsFlyer API"""
        # Настройка моков
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client
        
        # Настройка возвращаемых значений для методов клиента
        mock_client.get_installs_report.return_value = [{
            "date": "2025-07-01",
            "media_source": "facebook",
            "campaign": "summer_promo",
            "installs": 150
        }]
        
        mock_client.get_in_app_events_report.return_value = [{
            "date": "2025-07-01",
            "media_source": "facebook",
            "campaign": "summer_promo",
            "event_name": "purchase",
            "event_counter": 50,
            "event_revenue": 2500.00
        }]
        
        # Вызов тестируемого метода
        result = import_appsflyer_data("2025-07-01", "2025-07-01", include_retention=False, include_ltv=False)
        
        # Проверка результатов
        self.assertTrue(len(result) > 0)
        self.assertIn("installs", result[0])
        self.assertIn("purchases", result[0])
        
        # Проверка, что методы клиента были вызваны с правильными параметрами
        mock_client.get_installs_report.assert_called_once_with("2025-07-01", "2025-07-01", None)
        mock_client.get_in_app_events_report.assert_called_once_with("2025-07-01", "2025-07-01", None, None)


if __name__ == '__main__':
    unittest.main()
