"""
Тесты для metrics_calculator.py
"""

import unittest
import sys
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

# Добавляем корневую директорию проекта в PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.metrics_calculator import (
    calculate_metrics_task,
    calculate_metrics,
    get_deduplicated_transactions,
    get_advertising_costs,
    calculate_mobile_metrics
)

class TestMetricsCalculator(unittest.TestCase):
    """Тесты для модуля расчета метрик."""
    
    def test_calculate_mobile_metrics(self):
        """Проверяет корректность расчета мобильных метрик."""
        # Тестовые данные
        ad_costs = {
            'appsflyer': {
                'total_installs': 1000,
                'total_sessions': 5000,
                'total_events': 2500,
                'metrics_by_campaign': {
                    'campaign1': {
                        'installs': 600,
                        'sessions': 3000,
                        'events': 1500
                    },
                    'campaign2': {
                        'installs': 400,
                        'sessions': 2000,
                        'events': 1000
                    }
                }
            },
            'campaigns': {
                'appsflyer': {
                    'campaign1': 3000.0,
                    'campaign2': 2000.0
                }
            },
            'totals': {
                'appsflyer': 5000.0
            }
        }
        
        # Вызываем функцию расчета мобильных метрик
        result = calculate_mobile_metrics(ad_costs)
        
        # Проверяем результаты
        self.assertEqual(result['total_installs'], 1000)
        self.assertEqual(result['total_sessions'], 5000)
        self.assertEqual(result['total_events'], 2500)
        
        # Проверяем расчет CPI и CPE
        self.assertEqual(result['cpi'], 5.0)  # 5000 / 1000 = 5.0
        self.assertEqual(result['cpe'], 2.0)  # 5000 / 2500 = 2.0
        
        # Проверяем детализацию по кампаниям
        self.assertEqual(len(result['by_campaign']), 2)
        self.assertEqual(result['by_campaign']['campaign1']['cpi'], 5.0)  # 3000 / 600 = 5.0
        self.assertEqual(result['by_campaign']['campaign2']['cpi'], 5.0)  # 2000 / 400 = 5.0

    def test_calculate_mobile_metrics_no_data(self):
        """Проверяет расчет мобильных метрик при отсутствии данных AppsFlyer."""
        # Тестовые данные без AppsFlyer
        ad_costs = {
            'campaigns': {
                'meta': {'campaign1': 1000.0},
                'google_ads': {'campaign2': 2000.0}
            },
            'totals': {
                'meta': 1000.0,
                'google_ads': 2000.0,
                'total': 3000.0
            }
        }
        
        # Вызываем функцию
        result = calculate_mobile_metrics(ad_costs)
        
        # Проверяем нулевые результаты
        self.assertEqual(result['total_installs'], 0)
        self.assertEqual(result['total_sessions'], 0)
        self.assertEqual(result['total_events'], 0)
        self.assertEqual(result['cpi'], 0)
        self.assertEqual(result['cpe'], 0)

    @patch('src.metrics_calculator.get_connection')
    def test_get_advertising_costs_with_appsflyer(self, mock_get_connection):
        """Тестирует функцию получения расходов на рекламу с данными AppsFlyer."""
        # Создаем мок для соединения и курсора
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        
        # Настраиваем возвращаемые значения для разных запросов
        mock_cur.fetchall.side_effect = [
            # Результаты для Meta
            [('meta_campaign1', 1000.0), ('meta_campaign2', 2000.0)],
            # Результаты для Google Ads
            [('google_campaign1', 1500.0), ('google_campaign2', 2500.0)],
            # Результаты для AppsFlyer
            [('app_campaign1', 1000.0, 500, 2000, 1000), ('app_campaign2', 2000.0, 800, 3500, 1800)]
        ]
        
        # Вызываем функцию с тестовой датой
        test_date = datetime(2025, 7, 1).date()
        result = get_advertising_costs(test_date)
        
        # Проверяем структуру результата
        self.assertIn('campaigns', result)
        self.assertIn('totals', result)
        self.assertIn('appsflyer', result)
        
        # Проверяем данные Meta
        self.assertEqual(len(result['campaigns']['meta']), 2)
        self.assertEqual(result['totals']['meta'], 3000.0)
        
        # Проверяем данные Google Ads
        self.assertEqual(len(result['campaigns']['google_ads']), 2)
        self.assertEqual(result['totals']['google_ads'], 4000.0)
        
        # Проверяем данные AppsFlyer
        self.assertEqual(len(result['campaigns']['appsflyer']), 2)
        self.assertEqual(result['totals']['appsflyer'], 3000.0)
        self.assertEqual(result['appsflyer']['total_installs'], 1300)
        self.assertEqual(result['appsflyer']['total_sessions'], 5500)
        self.assertEqual(result['appsflyer']['total_events'], 2800)
        
        # Проверяем общую сумму
        self.assertEqual(result['totals']['total'], 10000.0)  # 3000 + 4000 + 3000 = 10000

if __name__ == '__main__':
    unittest.main()
