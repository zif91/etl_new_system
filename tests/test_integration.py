"""
Интеграционный тест для проверки полного цикла обработки:
- Сопоставление с медиапланом
- Многомерный анализ
- Взаимодействие с базой данных
"""
import unittest
import json
from unittest.mock import patch, MagicMock, call
from datetime import datetime

# Добавляем путь к проекту в sys.path для корректного импорта
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Импортируем функции, которые будем тестировать
from src.media_plan_matcher import MediaPlanMatcher
from src.multi_dimensional_analyzer import MultiDimensionalAnalyzer

class TestFullETLFlow(unittest.TestCase):

    def setUp(self):
        """Настройка тестового окружения перед каждым тестом."""
        self.execution_date = '2025-07-09'
        self.campaign_date = datetime.strptime(self.execution_date, '%Y-%m-%d')

        # 1. Тестовые данные для кампаний (имитация данных из БД)
        self.mock_campaigns = [
            {
                'campaign_id': 'cmp1',
                'campaign_name': 'promo_summer_sale_ru_cpc',
                'date': self.campaign_date,
                'source': 'google_ads',
                'country': 'RU',
                'restaurant': 'Burger Palace',
                'campaign_type': 'sales',
                'campaign_goal': 'orders',
                'spend': 100, 'impressions': 10000, 'clicks': 500, 'orders': 10, 'revenue': 1000
            },
            {
                'campaign_id': 'cmp2',
                'campaign_name': 'brand_awareness_de',
                'date': self.campaign_date,
                'source': 'meta',
                'country': 'DE',
                'restaurant': 'Pizza House',
                'campaign_type': 'awareness',
                'campaign_goal': 'reach',
                'spend': 250, 'impressions': 50000, 'clicks': 1000, 'orders': 5, 'revenue': 400
            },
            { # Кампания для нечеткого сопоставления
                'campaign_id': 'cmp3',
                'campaign_name': 'app_installs_fr',
                'date': self.campaign_date,
                'source': 'google_ads',
                'country': 'FR',
                'restaurant': 'Sushi Place',
                'campaign_type': 'installs',
                'campaign_goal': 'installs',
                'spend': 160, 'impressions': 12000, 'clicks': 300, 'orders': 20, 'revenue': 0
            }
        ]

        # 2. Тестовые данные для медиаплана
        self.mock_media_plan = [
            { # Точное совпадение для cmp1
                'id': 1, 'month': '2025-07-01', 'restaurant': 'Burger Palace', 'country': 'RU',
                'campaign_type': 'sales', 'goal': 'orders', 'source': 'google_ads',
                'planned_budget': 120, 'planned_impressions': 11000, 'planned_clicks': 550,
                'planned_orders': 12, 'planned_revenue': 1100
            },
            { # Совпадение для cmp2
                'id': 2, 'month': '2025-07-01', 'restaurant': 'Pizza House', 'country': 'DE',
                'campaign_type': 'awareness', 'goal': 'reach', 'source': 'meta',
                'planned_budget': 200, 'planned_impressions': 45000, 'planned_clicks': 900,
                'planned_orders': 4, 'planned_revenue': 350
            },
            { # Кандидат для нечеткого совпадения cmp3 (отличается goal)
                'id': 3, 'month': '2025-07-01', 'restaurant': 'Sushi Place', 'country': 'FR',
                'campaign_type': 'installs', 'goal': 'engagement', 'source': 'google_ads',
                'planned_budget': 150, 'planned_impressions': 10000, 'planned_clicks': 250,
                'planned_orders': 18, 'planned_revenue': 0
            }
        ]

    def test_media_plan_matcher(self):
        """Тестирует логику сопоставления кампаний с медиапланом."""
        
        # Создаем экземпляр матчера
        matcher = MediaPlanMatcher(self.mock_media_plan)
        
        # Тестируем точное совпадение
        result = matcher.match_campaign_to_media_plan(self.mock_campaigns[0])
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 1)
        self.assertFalse(result['is_fuzzy'])
        self.assertFalse(result['is_manual'])
        
        # Тестируем второе совпадение
        result = matcher.match_campaign_to_media_plan(self.mock_campaigns[1])
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 2)
        
        # Тестируем нечеткое совпадение
        result = matcher.match_campaign_to_media_plan(self.mock_campaigns[2])
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 3)
        self.assertTrue(result['is_fuzzy'])
        self.assertGreaterEqual(result['match_score'], 3)
        
    def test_multi_dimensional_analyzer(self):
        """Тестирует логику многомерного анализа."""
        
        # Подготавливаем данные для многомерного анализа
        analyzer = MultiDimensionalAnalyzer()
        
        # Данные сопоставления с медиапланом, как будто они пришли из matcher
        comparison_data = []
        matcher = MediaPlanMatcher(self.mock_media_plan)
        
        for campaign in self.mock_campaigns:
            match_result = matcher.match_campaign_to_media_plan(campaign)
            if match_result:
                comparison_data.append({
                    'matched': True,
                    'source': campaign['source'],
                    'country': campaign['country'],
                    'campaign_type': campaign['campaign_type'],
                    'variances': match_result['variances']
                })
        
        # Запускаем многомерный анализ
        dimensions = ['source', 'country', 'campaign_type']
        results = analyzer.analyze(comparison_data, dimensions)
        
        # Проверяем результаты анализа
        self.assertIn('source', results)
        self.assertIn('country', results)
        self.assertIn('campaign_type', results)
        
        # Проверяем, что есть результаты для google_ads и meta
        self.assertIn('google_ads', results['source'])
        self.assertIn('meta', results['source'])
        
        # Проверяем, что есть результаты для всех стран
        self.assertIn('RU', results['country'])
        self.assertIn('DE', results['country'])
        self.assertIn('FR', results['country'])
        
        # Проверяем, что есть результаты для всех типов кампаний
        self.assertIn('sales', results['campaign_type'])
        self.assertIn('awareness', results['campaign_type'])
        self.assertIn('installs', results['campaign_type'])


if __name__ == '__main__':
    unittest.main()
