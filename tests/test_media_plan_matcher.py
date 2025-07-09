"""
Тесты для MediaPlanMatcher.
"""
import unittest
from datetime import datetime
from src.media_plan_matcher import MediaPlanMatcher

class TestMediaPlanMatcher(unittest.TestCase):

    def setUp(self):
        """Настройка тестовых данных."""
        self.media_plan_data = [
            {
                'id': 1,
                'month': '2025-07-01',
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'goal': 'Заказы',
                'source': 'Google search',
                'planned_budget': 1000.0,
                'planned_impressions': 50000,
                'planned_clicks': 2000,
                'planned_orders': 100,
                'planned_revenue': 20000.0
            },
            {
                'id': 2,
                'month': '2025-07-01',
                'restaurant': 'Белла',
                'country': 'Казахстан',
                'campaign_type': 'Awareness',
                'goal': 'Охват/Узнаваемость',
                'source': 'Мета',
                'planned_budget': 1500.0,
                'planned_impressions': 200000,
                'planned_clicks': None, # Может быть None
                'planned_orders': 0,
                'planned_revenue': None
            }
        ]
        self.matcher = MediaPlanMatcher(self.media_plan_data)

    def test_exact_match_and_variance(self):
        """Тест на точное совпадение и корректный расчет отклонений."""
        campaign_data = {
            'date': datetime(2025, 7, 15),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы',
            'source': 'Google search',
            'spend': 1200.0,
            'impressions': 55000,
            'clicks': 1800,
            'orders': 110,
            'revenue': 25000.0
        }
        
        result = self.matcher.match_campaign_to_media_plan(campaign_data)
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 1)
        
        variances = result['variances']
        
        # Проверка отклонения по бюджету (spend)
        self.assertAlmostEqual(variances['spend']['fact'], 1200.0)
        self.assertAlmostEqual(variances['spend']['plan'], 1000.0)
        self.assertAlmostEqual(variances['spend']['absolute_variance'], 200.0)
        self.assertAlmostEqual(variances['spend']['relative_variance_percent'], 20.0)
        
        # Проверка отклонения по показам (impressions)
        self.assertAlmostEqual(variances['impressions']['absolute_variance'], 5000)
        self.assertAlmostEqual(variances['impressions']['relative_variance_percent'], 10.0)

        # Проверка отклонения по кликам (clicks) - недовыполнение
        self.assertAlmostEqual(variances['clicks']['absolute_variance'], -200)
        self.assertAlmostEqual(variances['clicks']['relative_variance_percent'], -10.0)

        # Проверка отклонения по заказам (orders)
        self.assertAlmostEqual(variances['orders']['absolute_variance'], 10)
        self.assertAlmostEqual(variances['orders']['relative_variance_percent'], 10.0)

        # Проверка отклонения по доходу (revenue)
        self.assertAlmostEqual(variances['revenue']['absolute_variance'], 5000)
        self.assertAlmostEqual(variances['revenue']['relative_variance_percent'], 25.0)

    def test_no_match(self):
        """Тест на отсутствие совпадения."""
        campaign_data = {
            'date': datetime(2025, 8, 10), # Другой месяц
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы',
            'source': 'Google search',
            'spend': 100.0
        }
        result = self.matcher.match_campaign_to_media_plan(campaign_data)
        self.assertIsNone(result)

    def test_match_with_zero_and_none_plan(self):
        """Тест на совпадение, когда в плане есть 0 и None."""
        campaign_data = {
            'date': datetime(2025, 7, 20),
            'restaurant': 'Белла',
            'country': 'Казахстан',
            'campaign_type': 'Awareness',
            'campaign_goal': 'Охват/Узнаваемость',
            'source': 'Мета',
            'spend': 1400.0,
            'impressions': 250000,
            'clicks': 500, # Факт есть, плана нет
            'orders': 5,    # Факт есть, план 0
            'revenue': 1000.0 # Факт есть, плана нет
        }
        
        result = self.matcher.match_campaign_to_media_plan(campaign_data)
        self.assertIsNotNone(result)
        self.assertEqual(result['media_plan_id'], 2)
        
        variances = result['variances']
        
        # Бюджет (недовыполнение)
        self.assertAlmostEqual(variances['spend']['absolute_variance'], -100.0)
        self.assertAlmostEqual(variances['spend']['relative_variance_percent'], -6.67, places=2)
        
        # Клики (план None, факт есть)
        self.assertEqual(variances['clicks']['plan'], 0)
        self.assertEqual(variances['clicks']['fact'], 500)
        self.assertEqual(variances['clicks']['relative_variance_percent'], 100.0)

        # Заказы (план 0, факт есть)
        self.assertEqual(variances['orders']['plan'], 0)
        self.assertEqual(variances['orders']['fact'], 5)
        self.assertEqual(variances['orders']['relative_variance_percent'], 100.0)

        # Доход (план None, факт есть)
        self.assertEqual(variances['revenue']['plan'], 0)
        self.assertEqual(variances['revenue']['fact'], 1000)
        self.assertEqual(variances['revenue']['relative_variance_percent'], 100.0)

    def test_fuzzy_match_successful(self):
        """Тест на успешное нечеткое совпадение (fuzzy match)."""
        campaign_data = {
            'date': datetime(2025, 7, 10),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы', # Совпадает
            'source': 'Google Ads', # Отличается от 'Google search'
            'spend': 950.0,
            'impressions': 48000,
            'clicks': 2100,
            'orders': 95,
            'revenue': 19000.0
        }
        
        result = self.matcher.match_campaign_to_media_plan(campaign_data)
        self.assertIsNotNone(result)
        self.assertTrue(result.get('is_fuzzy'))
        self.assertEqual(result['media_plan_id'], 1)
        self.assertGreaterEqual(result['match_score'], 3)

    def test_fuzzy_match_fail_low_score(self):
        """Тест на неудачное нечеткое совпадение из-за низкого балла."""
        campaign_data = {
            'date': datetime(2025, 7, 15),
            'restaurant': 'Якитория', # Отличается
            'country': 'Россия',      # Отличается
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы',
            'source': 'Yandex',       # Отличается
            'spend': 100.0
        }
        result = self.matcher.match_campaign_to_media_plan(campaign_data)
        self.assertIsNone(result)

    def test_variance_calculation_for_derived_metrics(self):
        """Тест на корректный расчет отклонений для производных метрик."""
        campaign_data = {
            'date': datetime(2025, 7, 15),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы',
            'source': 'Google search',
            'spend': 1200.0,      # План: 1000
            'impressions': 60000, # План: 50000
            'clicks': 1500,       # План: 2000
            'orders': 120,        # План: 100
            'revenue': 24000.0    # План: 20000
        }
        
        result = self.matcher.match_campaign_to_media_plan(campaign_data)
        self.assertIsNotNone(result)
        variances = result['variances']

        # План: CPM=20, CPC=0.5, CPA=10, CPO=10, ДРР=5%
        # Факт: CPM=20, CPC=0.8, CPA=10, CPO=10, ДРР=5%
        
        # CPM (Cost Per Mille)
        self.assertAlmostEqual(variances['cpm']['plan'], 20.0) # 1000 * 1000 / 50000
        self.assertAlmostEqual(variances['cpm']['fact'], 20.0) # 1200 * 1000 / 60000
        self.assertAlmostEqual(variances['cpm']['absolute_variance'], 0.0)
        self.assertAlmostEqual(variances['cpm']['relative_variance_percent'], 0.0)

        # CPC (Cost Per Click)
        self.assertAlmostEqual(variances['cpc']['plan'], 0.5) # 1000 / 2000
        self.assertAlmostEqual(variances['cpc']['fact'], 0.8) # 1200 / 1500
        self.assertAlmostEqual(variances['cpc']['absolute_variance'], 0.3)
        self.assertAlmostEqual(variances['cpc']['relative_variance_percent'], 60.0) # Рост CPC - это плохо

        # CPO (Cost Per Order)
        self.assertAlmostEqual(variances['cpo']['plan'], 10.0) # 1000 / 100
        self.assertAlmostEqual(variances['cpo']['fact'], 10.0) # 1200 / 120
        self.assertAlmostEqual(variances['cpo']['absolute_variance'], 0.0)
        self.assertAlmostEqual(variances['cpo']['relative_variance_percent'], 0.0)

        # ДРР (Доля Рекламных Расходов)
        self.assertAlmostEqual(variances['drr']['plan'], 5.0) # 1000 / 20000 * 100
        self.assertAlmostEqual(variances['drr']['fact'], 5.0) # 1200 / 24000 * 100
        self.assertAlmostEqual(variances['drr']['absolute_variance'], 0.0)
        self.assertAlmostEqual(variances['drr']['relative_variance_percent'], 0.0)

    def test_manual_mapping_override(self):
        """Тест на то, что ручное сопоставление имеет приоритет."""
        # Эта кампания не совпадает по 'source', но должна быть сопоставлена вручную
        campaign_data = {
            'date': datetime(2025, 7, 10),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Заказы',
            'source': 'Yandex Direct', # Не совпадает с планом
            'campaign_name': 'kz_performance_tanuki_yandex_promo_123',
            'spend': 800.0
        }
        
        campaign_identifier = (
            '2025-07-01', 'Тануки', 'Казахстан', 'Performance', 
            'Заказы', 'Yandex Direct', 'kz_performance_tanuki_yandex_promo_123'
        )
        
        manual_mappings = {campaign_identifier: 1} # Ручное сопоставление с планом ID=1
        
        matcher_with_manual = MediaPlanMatcher(self.media_plan_data, manual_mappings)
        result = matcher_with_manual.match_campaign_to_media_plan(campaign_data)
        
        self.assertIsNotNone(result)
        self.assertTrue(result.get('is_manual'))
        self.assertFalse(result.get('is_fuzzy'))
        self.assertEqual(result['media_plan_id'], 1)
        self.assertAlmostEqual(result['variances']['spend']['absolute_variance'], -200.0)

    def test_ambiguous_fuzzy_match(self):
        """Тест на обнаружение неоднозначного нечеткого совпадения."""
        # Добавляем еще один похожий план
        ambiguous_plan = self.media_plan_data + [{
            'id': 3,
            'month': '2025-07-01',
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'goal': 'Лиды', # Отличается от "Заказы"
            'source': 'Google search',
            'planned_budget': 500.0,
            'planned_impressions': 25000,
            'planned_clicks': 1000,
            'planned_orders': 50,
            'planned_revenue': 10000.0
        }]
        
        matcher_with_ambiguous = MediaPlanMatcher(ambiguous_plan)
        
        # Эта кампания может подойти под оба плана с одинаковым счетом
        campaign_data = {
            'date': datetime(2025, 7, 15),
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'campaign_type': 'Performance',
            'campaign_goal': 'Неизвестно', # Не совпадает ни с одним
            'source': 'Google search',
            'spend': 100.0
        }
        
        result = matcher_with_ambiguous.match_campaign_to_media_plan(campaign_data)
        
        self.assertIsNotNone(result)
        self.assertTrue(result.get('is_fuzzy'))
        self.assertTrue(result.get('is_ambiguous'))
        # Убедимся, что он выбрал один из них (например, первый по порядку)
        self.assertIn(result['media_plan_id'], [1, 3])


if __name__ == '__main__':
    unittest.main()
