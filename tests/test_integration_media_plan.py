"""
Интеграционный тест для проверки работы модуля сравнения с медиапланом.
"""

import unittest
import os
import sys
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

# Добавляем корневую директорию проекта в PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.media_plan_matcher import MediaPlanMatcher
from src.media_plan_integrator import compare_with_media_plan_task
from src.multi_dimensional_analyzer import multi_dimensional_analysis_task

class TestMediaPlanIntegration(unittest.TestCase):
    """
    Тест для проверки интеграции модулей сравнения с медиапланом.
    """
    
    def setUp(self):
        """Настройка тестовых данных."""
        # Тестовые данные медиаплана
        self.media_plan_data = [
            {
                'id': 1,
                'month': '2025-07-01',
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'goal': 'Заказы',
                'source': 'Google search',
                'planned_budget': 10000.0,
                'planned_impressions': 500000,
                'planned_clicks': 10000,
                'planned_orders': 500,
                'planned_revenue': 50000.0
            },
            {
                'id': 2,
                'month': '2025-07-01',
                'restaurant': 'Белла',
                'country': 'Казахстан',
                'campaign_type': 'Awareness',
                'goal': 'Охват/Узнаваемость',
                'source': 'Мета',
                'planned_budget': 8000.0,
                'planned_impressions': 1000000,
                'planned_clicks': 5000,
                'planned_orders': 0,
                'planned_revenue': 0.0
            }
        ]
        
        # Тестовые данные кампании
        self.campaign_data = {
            'date': datetime.strptime('2025-07-15', '%Y-%m-%d'),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы',
            'source': 'Google search',
            'campaign_name': 'Search|CPC|Almaty|Tanuki|No_Brand|Keywords',
            'spend': 9500.0,
            'impressions': 480000,
            'clicks': 9800,
            'orders': 520,
            'revenue': 52000.0
        }
        
        # Тестовые данные для нечеткого сопоставления
        self.fuzzy_campaign_data = {
            'date': datetime.strptime('2025-07-15', '%Y-%m-%d'),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Установки приложения',  # Отличается от медиаплана
            'source': 'Google search',
            'campaign_name': 'Search|CPC|Almaty|Tanuki|APP_Install|Keywords',
            'spend': 9500.0,
            'impressions': 480000,
            'clicks': 9800,
            'orders': 520,
            'revenue': 52000.0
        }
        
        # Тестовые ручные сопоставления
        self.manual_mappings = {
            ('2025-07-01', 'Белла', 'Казахстан', 'Awareness', 'Охват/Узнаваемость', 'Мета', 
             'Instagram | CPM | Almaty | Bella | Interests | День Рождения'): 2
        }

    @patch('src.media_plan_integrator._load_media_plan')
    @patch('src.media_plan_integrator._load_manual_mappings')
    @patch('src.media_plan_integrator._get_campaign_metrics_from_db')
    @patch('src.media_plan_integrator._save_comparison_results')
    @patch('src.media_plan_integrator._save_results_to_db')
    def test_compare_with_media_plan_task(self, mock_save_db, mock_save_results, 
                                         mock_get_metrics, mock_load_mappings, mock_load_plan):
        """Тест задачи сравнения с медиапланом."""
        # Настраиваем моки
        mock_load_plan.return_value = self.media_plan_data
        mock_load_mappings.return_value = self.manual_mappings
        mock_get_metrics.return_value = [self.campaign_data, self.fuzzy_campaign_data]
        mock_save_results.return_value = "data/comparisons/test_comparison.json"
        
        # Вызываем функцию
        result = compare_with_media_plan_task(execution_date="2025-07-15")
        
        # Проверяем, что функция вернула ожидаемый результат
        self.assertIsNotNone(result)
        
        # Проверяем, что моки были вызваны
        mock_load_plan.assert_called_once()
        mock_load_mappings.assert_called_once()
        mock_get_metrics.assert_called_once()
        mock_save_results.assert_called_once()
        mock_save_db.assert_called_once()
        
    def test_matcher_exact_match(self):
        """Тест точного сопоставления кампании с медиапланом."""
        matcher = MediaPlanMatcher(self.media_plan_data)
        result = matcher.match_campaign_to_media_plan(self.campaign_data)
        
        # Проверяем результаты сопоставления
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 1)
        self.assertFalse(result['is_fuzzy'])
        self.assertFalse(result['is_manual'])
        
        # Проверяем расчет отклонений
        self.assertIn('variances', result)
        self.assertIn('spend', result['variances'])
        self.assertIn('impressions', result['variances'])
        
        # Проверяем значения отклонений
        spend_variance = result['variances']['spend']
        self.assertEqual(spend_variance['fact'], 9500.0)
        self.assertEqual(spend_variance['plan'], 10000.0)
        self.assertEqual(spend_variance['absolute_variance'], -500.0)
        
    def test_matcher_fuzzy_match(self):
        """Тест нечеткого сопоставления кампании с медиапланом."""
        matcher = MediaPlanMatcher(self.media_plan_data)
        result = matcher.match_campaign_to_media_plan(self.fuzzy_campaign_data)
        
        # Проверяем результаты нечеткого сопоставления
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 1)
        self.assertTrue(result['is_fuzzy'])
        self.assertFalse(result['is_manual'])
        self.assertGreaterEqual(result['match_score'], 3)  # Минимальный порог для совпадения
        
    def test_matcher_manual_match(self):
        """Тест ручного сопоставления кампании с медиапланом."""
        # Создаем тестовую кампанию, которая должна быть сопоставлена по ручному маппингу
        manual_campaign_data = {
            'date': datetime.strptime('2025-07-15', '%Y-%m-%d'),
            'restaurant': 'Белла',
            'country': 'Казахстан',
            'campaign_type': 'Awareness',
            'campaign_goal': 'Охват/Узнаваемость',
            'source': 'Мета',
            'campaign_name': 'Instagram | CPM | Almaty | Bella | Interests | День Рождения',
            'spend': 8500.0,
            'impressions': 1100000,
            'clicks': 5500,
            'orders': 0,
            'revenue': 0.0
        }
        
        matcher = MediaPlanMatcher(self.media_plan_data, self.manual_mappings)
        result = matcher.match_campaign_to_media_plan(manual_campaign_data)
        
        # Проверяем результаты ручного сопоставления
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 2)
        self.assertFalse(result['is_fuzzy'])
        self.assertTrue(result['is_manual'])

    @patch('src.multi_dimensional_analyzer.get_connection')
    def test_multi_dimensional_analysis(self, mock_get_conn):
        """Тест многомерного анализа."""
        # Создаем моки для БД
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        
        # Тестовые данные для сравнения
        mock_cursor.fetchall.return_value = []
        # Пропускаем запрос к БД, который вызывает ошибку в тесте
        mock_cursor.execute = MagicMock()
        mock_cursor.description = [
            ('id', None, None, None, None, None, None),
            ('comparison_date', None, None, None, None, None, None),
            ('campaign_date', None, None, None, None, None, None),
            ('restaurant', None, None, None, None, None, None),
            ('country', None, None, None, None, None, None),
            ('campaign_type', None, None, None, None, None, None),
            ('campaign_goal', None, None, None, None, None, None),
            ('source', None, None, None, None, None, None),
            ('campaign_name', None, None, None, None, None, None),
            ('matched', None, None, None, None, None, None),
            ('is_manual', None, None, None, None, None, None),
            ('is_fuzzy', None, None, None, None, None, None),
            ('is_ambiguous', None, None, None, None, None, None),
            ('match_score', None, None, None, None, None, None),
            ('media_plan_id', None, None, None, None, None, None),
            ('variances', None, None, None, None, None, None)
        ]
        
        # Вызываем функцию
        multi_dimensional_analysis_task('2025-07-15')
        
        # Проверяем, что функции БД были вызваны
        mock_cursor.execute.assert_called()
        mock_cursor.fetchall.assert_called()
        

if __name__ == '__main__':
    unittest.main()
