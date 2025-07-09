"""
Тесты для MultiDimensionalAnalyzer.
"""
import unittest
import json
from src.multi_dimensional_analyzer import MultiDimensionalAnalyzer

class TestMultiDimensionalAnalyzer(unittest.TestCase):

    def setUp(self):
        """Настройка тестовых данных."""
        self.analyzer = MultiDimensionalAnalyzer()
        # Пример данных, которые могли бы прийти из media_plan_integrator
        self.comparison_data = [
            {
                'matched': True,
                'source': 'Google',
                'country': 'KZ',
                'campaign_type': 'Performance',
                'variances': {
                    'spend': {'plan': 100, 'fact': 120},
                    'impressions': {'plan': 10000, 'fact': 11000},
                    'clicks': {'plan': 500, 'fact': 550},
                    'orders': {'plan': 10, 'fact': 12},
                    'revenue': {'plan': 1000, 'fact': 1300}
                }
            },
            {
                'matched': True,
                'source': 'Google',
                'country': 'KZ',
                'campaign_type': 'Awareness',
                'variances': {
                    'spend': {'plan': 200, 'fact': 180},
                    'impressions': {'plan': 50000, 'fact': 52000},
                    'clicks': {'plan': 1000, 'fact': 900},
                    'orders': {'plan': 0, 'fact': 1},
                    'revenue': {'plan': 0, 'fact': 50}
                }
            },
            {
                'matched': True,
                'source': 'Meta',
                'country': 'KZ',
                'campaign_type': 'Performance',
                'variances': {
                    'spend': {'plan': 150, 'fact': 150},
                    'impressions': {'plan': 15000, 'fact': 16000},
                    'clicks': {'plan': 700, 'fact': 750},
                    'orders': {'plan': 15, 'fact': 18},
                    'revenue': {'plan': 1500, 'fact': 1800}
                }
            },
            {
                'matched': False, # Эта запись не должна учитываться
                'source': 'Yandex',
                'country': 'RU',
                'variances': {'spend': {'plan': 50, 'fact': 60}}
            }
        ]

    def test_analyze_by_source(self):
        """Тест анализа в разрезе 'source'."""
        results = self.analyzer.analyze(self.comparison_data, ['source'])
        
        self.assertIn('source', results)
        self.assertIn('Google', results['source'])
        self.assertIn('Meta', results['source'])
        self.assertNotIn('Yandex', results['source']) # т.к. matched=False

        google_stats = results['source']['Google']
        
        # Проверяем агрегированные базовые метрики
        self.assertAlmostEqual(google_stats['spend']['plan'], 300) # 100 + 200
        self.assertAlmostEqual(google_stats['spend']['fact'], 300) # 120 + 180
        self.assertAlmostEqual(google_stats['spend']['absolute_variance'], 0)
        self.assertAlmostEqual(google_stats['spend']['relative_variance_percent'], 0)

        self.assertAlmostEqual(google_stats['orders']['plan'], 10) # 10 + 0
        self.assertAlmostEqual(google_stats['orders']['fact'], 13) # 12 + 1
        self.assertAlmostEqual(google_stats['orders']['absolute_variance'], 3)
        self.assertAlmostEqual(google_stats['orders']['relative_variance_percent'], 30.0)

        # Проверяем пересчитанные производные метрики
        # План: CPO = 300 / 10 = 30
        # Факт: CPO = 300 / 13 = 23.0769...
        self.assertAlmostEqual(google_stats['cpo']['plan'], 30)
        self.assertAlmostEqual(google_stats['cpo']['fact'], 23.0769, places=4)
        self.assertAlmostEqual(google_stats['cpo']['relative_variance_percent'], -23.08, places=2)

        meta_stats = results['source']['Meta']
        self.assertAlmostEqual(meta_stats['spend']['plan'], 150)
        self.assertAlmostEqual(meta_stats['spend']['fact'], 150)
        self.assertAlmostEqual(meta_stats['cpo']['plan'], 10) # 150 / 15
        self.assertAlmostEqual(meta_stats['cpo']['fact'], 8.3333, places=4) # 150 / 18

    def test_analyze_by_multiple_dimensions(self):
        """Тест анализа по нескольким разрезам."""
        results = self.analyzer.analyze(self.comparison_data, ['country', 'campaign_type'])
        
        self.assertIn('country', results)
        self.assertIn('campaign_type', results)
        
        # Все данные по стране KZ
        kz_stats = results['country']['KZ']
        self.assertAlmostEqual(kz_stats['spend']['plan'], 450) # 100 + 200 + 150
        self.assertAlmostEqual(kz_stats['spend']['fact'], 450) # 120 + 180 + 150

        # Данные по типу кампании Performance
        perf_stats = results['campaign_type']['Performance']
        self.assertAlmostEqual(perf_stats['spend']['plan'], 250) # 100 + 150
        self.assertAlmostEqual(perf_stats['spend']['fact'], 270) # 120 + 150
        self.assertAlmostEqual(perf_stats['spend']['relative_variance_percent'], 8.0)

    def test_safe_divide(self):
        """Тест безопасного деления."""
        self.assertEqual(self.analyzer._safe_divide(10, 2), 5)
        self.assertEqual(self.analyzer._safe_divide(10, 0), 0.0)
        self.assertEqual(self.analyzer._safe_divide(0, 5), 0)

if __name__ == '__main__':
    unittest.main()
