"""
Тесты для системы дедупликации заказов.
"""

import unittest
from datetime import date, datetime
from typing import Dict, List, Any
from unittest.mock import MagicMock, patch

from src.deduplication import OrderDeduplicator, DeduplicationStrategy
from src.attribution import AttributionSourceAssigner, AttributionSource, AttributionModel


class TestOrderDeduplicator(unittest.TestCase):
    """
    Тесты для класса OrderDeduplicator.
    """
    
    def setUp(self):
        """
        Настраивает тестовые данные перед каждым тестом.
        """
        # Создаем мок для атрибуции
        self.mock_attribution_assigner = MagicMock(spec=AttributionSourceAssigner)
        
        # Настраиваем мок, чтобы он добавлял атрибуцию источника
        def assign_attribution_side_effect(transaction):
            if transaction.get('is_promo_order', False):
                transaction['attribution_source'] = AttributionSource.PROMO_CODE
                transaction['attribution_details'] = {
                    'source': AttributionSource.PROMO_CODE,
                    'model_used': AttributionModel.LAST_CLICK
                }
            else:
                source = 'google_ads' if 'google' in transaction.get('utm_source', '') else AttributionSource.UTM_SOURCE
                transaction['attribution_source'] = source
                transaction['attribution_details'] = {
                    'source': source,
                    'model_used': AttributionModel.LAST_CLICK
                }
            return transaction
        
        self.mock_attribution_assigner.assign_attribution_source.side_effect = assign_attribution_side_effect
        self.mock_attribution_assigner.get_stats.return_value = {'processed': 5, 'sources': {}}
        
        self.deduplicator = OrderDeduplicator(
            attribution_assigner=self.mock_attribution_assigner
        )
        
        # Тестовые данные из GA4
        self.ga4_data = [
            {
                'transaction_id': 'TXN123456',
                'date': '2025-01-01',
                'utm_source': 'facebook',
                'utm_medium': 'cpc',
                'utm_campaign': 'summer_promo',
                'purchase_revenue': 1500.0
            },
            {
                'transaction_id': 'TXN789012',
                'date': '2025-01-02',
                'utm_source': 'google',
                'utm_medium': 'cpc',
                'utm_campaign': 'winter_sale',
                'purchase_revenue': 2000.5
            },
            {
                'transaction_id': 'TXN345678',
                'date': '2025-01-03',
                'utm_source': 'direct',
                'utm_medium': 'none',
                'utm_campaign': 'none',
                'purchase_revenue': 1750.75
            },
            {
                'transaction_id': 'TXN901234-GA4',  # Небольшое отличие для теста нечеткого сопоставления
                'date': '2025-01-04',
                'utm_source': 'instagram',
                'utm_medium': 'cpc',
                'utm_campaign': 'food_delivery',
                'purchase_revenue': 1250.0
            },
            {
                'transaction_id': 'TXN567890',
                'date': '2025-01-05',
                'utm_source': 'email',
                'utm_medium': 'email',
                'utm_campaign': 'newsletter',
                'purchase_revenue': 500.0
            }
        ]
        
        # Тестовые данные о промокодах
        self.promo_data = [
            {
                'promo_code': 'SUMMER20',
                'order_id': 'ORD-123',
                'transaction_id': 'TXN123456',
                'order_date': date(2025, 1, 1),
                'order_amount': 1500.0,
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'promo_source': 'facebook_ads'
            },
            {
                'promo_code': 'WINTER15',
                'order_id': 'ORD-456',
                'transaction_id': 'TXN901234',  # Для теста нечеткого сопоставления
                'order_date': date(2025, 1, 4),
                'order_amount': 1250.0,
                'restaurant': 'Белла',
                'country': 'Узбекистан',
                'promo_source': 'instagram_ads'
            },
            {
                'promo_code': 'DELIVERY10',
                'order_id': 'ORD-789',
                'transaction_id': 'TXN567890',
                'order_date': date(2025, 1, 5),
                'order_amount': 500.0,
                'restaurant': 'Каспийка',
                'country': 'Казахстан',
                'promo_source': 'email_campaign'
            },
            # Добавляем дубликаты для тестирования разрешения конфликтов
            {
                'promo_code': 'WINTER15_FACEBOOK',
                'order_id': 'ORD-456',
                'transaction_id': 'TXN901234-V2',  # Дубликат для TXN901234-GA4
                'order_date': date(2025, 1, 4),
                'order_amount': 1250.0,
                'restaurant': 'Белла',
                'country': 'Узбекистан',
                'promo_source': 'facebook_ads'
            },
            {
                'promo_code': 'WINTER15_GOOGLE',
                'order_id': 'ORD-456',
                'transaction_id': 'TXN901234-V3',  # Еще один дубликат для TXN901234-GA4
                'order_date': date(2025, 1, 4),
                'order_amount': 1250.0,
                'restaurant': 'Белла',
                'country': 'Узбекистан',
                'promo_source': 'google_ads'
            }
        ]
        
        # Данные для теста улучшенного сопоставления
        self.ga4_data_enhanced = [
            {
                'transaction_id': 'ORDER123',
                'order_id': '123456',
                'date': '2025-01-10',
                'utm_source': 'facebook',
                'utm_medium': 'cpc',
                'purchase_revenue': 1850.0,
                'customer_phone': '+7 (701) 123-4567'
            },
            {
                'transaction_id': 'ORDER987',
                'order_id': '987654',
                'date': '2025-01-11',
                'utm_source': 'email',
                'utm_medium': 'email',
                'purchase_revenue': 2200.0,
                'customer_phone': '+7 (702) 987-6543'
            }
        ]
        
        self.promo_data_enhanced = [
            {
                'promo_code': 'FB20',
                'order_id': 'A123456',  # Немного отличается от GA4, но должно сопоставиться
                'transaction_id': 'TXN-ORDER123',  # Другой формат ID, но с общей частью
                'order_date': date(2025, 1, 10),
                'order_amount': 1850.0,  # Та же сумма, поможет в сопоставлении
                'promo_source': 'facebook_ads',
                'customer_phone': '7-701-123-4567'  # Тот же телефон, но в другом формате
            },
            {
                'promo_code': 'EMAIL15',
                'order_id': '987654',  # Точное совпадение order_id
                'transaction_id': 'DIFF-ID',  # Полностью другой ID
                'order_date': date(2025, 1, 11),
                'order_amount': 2200.0,
                'promo_source': 'email_campaign',
                'customer_phone': '+7 702 987 6543'  # Тот же телефон, но в другом формате
            }
        ]
    
    def test_exact_match_deduplication(self):
        """
        Тестирует точное совпадение при дедупликации.
        """
        # Выполняем дедупликацию
        result = self.deduplicator.deduplicate_orders(self.ga4_data, self.promo_data)
        
        # Проверяем, что результат содержит все транзакции
        self.assertEqual(len(result), len(self.ga4_data))
        
        # Проверяем, что транзакции с точным совпадением отмечены правильно
        for transaction in result:
            if transaction['transaction_id'] == 'TXN123456':
                self.assertTrue(transaction['is_promo_order'])
                self.assertEqual(transaction['attribution_source'], AttributionSource.PROMO_CODE)
                self.assertEqual(transaction['match_type'], 'exact')
                self.assertEqual(transaction['match_confidence'], 1.0)
                self.assertEqual(transaction['promo_code'], 'SUMMER20')
                self.assertEqual(transaction['promo_source'], 'facebook_ads')
            
            if transaction['transaction_id'] == 'TXN567890':
                self.assertTrue(transaction['is_promo_order'])
                self.assertEqual(transaction['attribution_source'], AttributionSource.PROMO_CODE)
                self.assertEqual(transaction['match_type'], 'exact')
                self.assertEqual(transaction['promo_code'], 'DELIVERY10')
    
    def test_fuzzy_match_deduplication(self):
        """
        Тестирует нечеткое совпадение при дедупликации.
        """
        # Выполняем дедупликацию
        result = self.deduplicator.deduplicate_orders(self.ga4_data, self.promo_data)
        
        # Проверяем, что транзакция с нечетким совпадением отмечена правильно
        for transaction in result:
            if transaction['transaction_id'] == 'TXN901234-GA4':
                self.assertTrue(transaction['is_promo_order'])
                self.assertEqual(transaction['attribution_source'], AttributionSource.PROMO_CODE)
                self.assertIn(transaction['match_type'], ['fuzzy', 'fuzzy_resolved'])
                self.assertGreaterEqual(transaction['match_confidence'], self.deduplicator.fuzzy_matching_threshold)
                self.assertIn(transaction['promo_code'], ['WINTER15', 'WINTER15_FACEBOOK', 'WINTER15_GOOGLE'])
                self.assertIn(transaction['promo_source'], ['instagram_ads', 'facebook_ads', 'google_ads'])
                self.assertIn(transaction['fuzzy_matched_id'], ['TXN901234', 'TXN901234-V2', 'TXN901234-V3'])
    
    def test_unmatched_transactions(self):
        """
        Тестирует транзакции без совпадений.
        """
        # Выполняем дедупликацию
        result = self.deduplicator.deduplicate_orders(self.ga4_data, self.promo_data)
        
        # Проверяем, что транзакции без совпадений отмечены правильно
        for transaction in result:
            if transaction['transaction_id'] in ['TXN789012', 'TXN345678']:
                self.assertFalse(transaction['is_promo_order'])
                # Атрибуция по UTM-источнику может отличаться в зависимости от правил
                self.assertIn(transaction['attribution_source'], ['google_ads', 'utm_attribution', AttributionSource.UTM_SOURCE])
                self.assertEqual(transaction['match_type'], 'none')
                self.assertEqual(transaction['match_confidence'], 0.0)
    
    def test_empty_input(self):
        """
        Тестирует дедупликацию с пустыми входными данными.
        """
        # Тестируем пустые GA4 данные
        result1 = self.deduplicator.deduplicate_orders([], self.promo_data)
        self.assertEqual(len(result1), 0)
        
        # Тестируем пустые данные о промокодах
        result2 = self.deduplicator.deduplicate_orders(self.ga4_data, [])
        self.assertEqual(len(result2), len(self.ga4_data))
        
        # Проверяем, что все транзакции отмечены как не совпадающие
        for transaction in result2:
            self.assertFalse(transaction['is_promo_order'])
            # Атрибуция может отличаться в зависимости от правил
            self.assertIn(transaction['attribution_source'], 
                          ['utm_attribution', 'google_ads', AttributionSource.UTM_SOURCE])
    
    def test_stats_generation(self):
        """
        Тестирует генерацию статистики дедупликации.
        """
        # Выполняем дедупликацию
        self.deduplicator.deduplicate_orders(self.ga4_data, self.promo_data)
        
        # Получаем статистику
        stats = self.deduplicator.get_stats()
        
        # Проверяем основные метрики
        self.assertEqual(stats['total_ga4_transactions'], len(self.ga4_data))
        self.assertEqual(stats['total_promo_transactions'], len(self.promo_data))
        self.assertEqual(stats['exact_matches'], 2)  # TXN123456 и TXN567890
        self.assertEqual(stats['fuzzy_matches'], 1)  # TXN901234-GA4
        self.assertEqual(stats['unmatched'], 2)      # TXN789012 и TXN345678
        
        # Проверяем итоговые коэффициенты
        self.assertAlmostEqual(stats['match_rate'], 3/5)
        self.assertAlmostEqual(stats['exact_match_rate'], 2/5)
        self.assertAlmostEqual(stats['fuzzy_match_rate'], 1/5)
        
        # Уникальных транзакций в промо-данных, которые могут быть сопоставлены - 3
        unique_promo_transactions = len(set(p['transaction_id'] for p in self.promo_data if 'V2' not in p['transaction_id'] and 'V3' not in p['transaction_id']))
        self.assertAlmostEqual(stats['promo_coverage'], 3 / unique_promo_transactions)
    
    def test_custom_fuzzy_threshold(self):
        """
        Тестирует настраиваемый порог для нечеткого сопоставления.
        """
        # Создаем дедупликатор с более высоким порогом
        strict_deduplicator = OrderDeduplicator(fuzzy_matching_threshold=0.99)
        
        # Выполняем дедупликацию
        result = strict_deduplicator.deduplicate_orders(self.ga4_data, self.promo_data)
        
        # С высоким порогом нечеткое сопоставление не должно сработать
        for transaction in result:
            if transaction['transaction_id'] == 'TXN901234-GA4':
                self.assertFalse(transaction['is_promo_order'])
                self.assertEqual(transaction['attribution_source'], AttributionSource.UTM_SOURCE)
                
        # Создаем дедупликатор с более низким порогом
        lenient_deduplicator = OrderDeduplicator(fuzzy_matching_threshold=0.5)
        
        # Выполняем дедупликацию
        result = lenient_deduplicator.deduplicate_orders(self.ga4_data, self.promo_data)
        
        # С низким порогом нечеткое сопоставление должно сработать
        for transaction in result:
            if transaction['transaction_id'] == 'TXN901234-GA4':
                self.assertTrue(transaction['is_promo_order'])
                self.assertEqual(transaction['attribution_source'], AttributionSource.PROMO_CODE)
    
    def test_conflict_resolution_strategies(self):
        """Тестирует различные стратегии разрешения конфликтов."""
        transactions = [
            {'order_id': 'ORD100', 'order_date': datetime(2023, 1, 1, 12, 5), 'order_amount': 150.0, 'customer_id': 'CUST1'},
        ]
        promo_orders = [
            {'promo_code': 'CONF_PROMO1', 'order_id': 'ORD100', 'order_date': datetime(2023, 1, 1, 12, 0), 'order_amount': 150.0},
            {'promo_code': 'CONF_PROMO2', 'order_id': 'ORD100', 'order_date': datetime(2023, 1, 1, 12, 2), 'order_amount': 150.0},
        ]
        
        # Стратегия 'first'
        deduplicator = OrderDeduplicator(promo_orders, time_window_minutes=10, conflict_strategy='first')
        result = deduplicator.deduplicate(transactions)
        self.assertEqual(result[0]['promo_code'], 'CONF_PROMO1')

        # Стратегия 'last'
        deduplicator = OrderDeduplicator(promo_orders, time_window_minutes=10, conflict_strategy='last')
        result = deduplicator.deduplicate(transactions)
        self.assertEqual(result[0]['promo_code'], 'CONF_PROMO2')

        # Стратегия 'error'
        deduplicator = OrderDeduplicator(promo_orders, time_window_minutes=10, conflict_strategy='error')
        with self.assertRaises(ValueError):
            deduplicator.deduplicate(transactions)

    def test_stats_generation(self):
        """Тестирует генерацию статистики дедупликации."""
        transactions = [
            {'order_id': 'ORD100', 'order_date': datetime(2023, 1, 1, 12, 5), 'order_amount': 150.0, 'customer_id': 'CUST1'},
            {'order_id': 'ORD101', 'order_date': datetime(2023, 1, 1, 12, 10), 'order_amount': 200.0, 'customer_id': 'CUST2'},
            {'order_id': 'ORD102', 'order_date': datetime(2023, 1, 1, 12, 15), 'order_amount': 250.0, 'customer_id': 'CUST1'},
            {'order_id': 'ORD103', 'order_date': datetime(2023, 1, 1, 12, 20), 'order_amount': 300.0, 'customer_id': 'CUST3'},
            {'order_id': 'ORD104', 'order_date': datetime(2023, 1, 1, 12, 25), 'order_amount': 350.0, 'customer_id': 'CUST2'},
        ]
        promo_orders = [
            {'promo_code': 'PROMO1', 'order_id': 'ORD100', 'order_date': datetime(2023, 1, 1, 12, 0), 'order_amount': 150.0},
            {'promo_code': 'PROMO2', 'order_id': 'ORD101', 'order_date': datetime(2023, 1, 1, 12, 10), 'order_amount': 200.0},
            {'promo_code': 'PROMO3', 'order_id': 'ORD102', 'order_date': datetime(2023, 1, 1, 12, 15), 'order_amount': 250.0},
            {'promo_code': 'PROMO4', 'order_id': 'ORD103', 'order_date': datetime(2023, 1, 1, 12, 20), 'order_amount': 300.0},
            {'promo_code': 'PROMO5', 'order_id': 'ORD104', 'order_date': datetime(2023, 1, 1, 12, 25), 'order_amount': 350.0},
        ]

        unique_promo_transactions = 5
        
        deduplicator = OrderDeduplicator(promo_orders, time_window_minutes=10)
        deduplicator.deduplicate(transactions)
        stats = deduplicator.get_stats()

        self.assertEqual(stats['total_transactions'], len(transactions))
        self.assertEqual(stats['matched_transactions'], 3)
        self.assertEqual(stats['unmatched_transactions'], 2)
        self.assertEqual(stats['total_promo_orders'], len(promo_orders))
        self.assertEqual(stats['unique_promo_transactions'], unique_promo_transactions)
        self.assertAlmostEqual(stats['promo_coverage'], 3 / unique_promo_transactions)

    def test_additional_match_criteria(self):
        """Тестирует дополнительные критерии сопоставления."""
        transactions = [
            {'order_id': 'ORD200', 'order_date': datetime(2023, 1, 1, 12, 0), 'order_amount': 200.0, 'customer_id': 'CUST2', 'payment_method': 'card'},
            {'order_id': 'ORD201', 'order_date': datetime(2023, 1, 1, 13, 0), 'order_amount': 250.0, 'customer_id': 'CUST3', 'payment_method': 'cash'},
        ]
        promo_orders = [
            {'promo_code': 'PROMO_CARD', 'order_id': 'ORD200', 'order_date': datetime(2023, 1, 1, 12, 0), 'order_amount': 200.0, 'payment_method': 'card'},
            {'promo_code': 'PROMO_CASH', 'order_id': 'ORD201', 'order_date': datetime(2023, 1, 1, 13, 0), 'order_amount': 250.0, 'payment_method': 'card'}, # Не совпадает метод оплаты
        ]
        
        deduplicator = OrderDeduplicator(promo_orders, time_window_minutes=5, match_criteria=['order_id', 'order_amount', 'payment_method'])
        result = deduplicator.deduplicate(transactions)

        self.assertTrue(result[0]['is_promo_order'])
        self.assertFalse(result[1]['is_promo_order'])

    def test_time_window_configuration(self):
        """Тестирует настройку временного окна для сопоставления транзакций."""
        transactions = [
            {'order_id': 'ORD300', 'order_date': datetime(2023, 1, 1, 12, 10), 'order_amount': 300.0},
        ]
        promo_orders = [
            {'promo_code': 'PROMO_TIME', 'order_id': 'ORD300', 'order_date': datetime(2023, 1, 1, 12, 0), 'order_amount': 300.0},
        ]

        # Окно 5 минут - не должно быть совпадения
        deduplicator_5_min = OrderDeduplicator(promo_orders, time_window_minutes=5)
        result_5_min = deduplicator_5_min.deduplicate(transactions.copy())
        self.assertFalse(result_5_min[0]['is_promo_order'])

        # Окно 15 минут - должно быть совпадение
        deduplicator_15_min = OrderDeduplicator(promo_orders, time_window_minutes=15)
        result_15_min = deduplicator_15_min.deduplicate(transactions.copy())
        self.assertTrue(result_15_min[0]['is_promo_order'])
    
    def test_configure_method(self):
        """
        Тестирует метод configure для комплексной настройки дедупликатора.
        """
        # Создаем дедупликатор с базовыми настройками
        configurable_deduplicator = OrderDeduplicator(attribution_assigner=self.mock_attribution_assigner)
        
        # Настраиваем с помощью метода configure
        configurable_deduplicator.configure({
            'fuzzy_matching_threshold': 0.7,
            'conflict_strategy': 'highest_value',
            'time_window_hours': 10 * 24,
            'additional_match_criteria': ['user_id', 'email'],
            'use_transactional_attrs': True
        })
        
        # Проверяем, что настройки были применены
        self.assertEqual(configurable_deduplicator.fuzzy_matching_threshold, 0.7)
        self.assertEqual(configurable_deduplicator.conflict_strategy, 'highest_value')
        self.assertEqual(configurable_deduplicator.time_window_hours, 10 * 24)
        self.assertEqual(configurable_deduplicator.additional_match_criteria, ['user_id', 'email'])
        self.assertTrue(configurable_deduplicator.use_transactional_attrs)
        
        # Проверяем частичное обновление конфигурации
        configurable_deduplicator.configure({
            'conflict_strategy': 'first_touch',
            'time_window_hours': 5
        })
        
        self.assertEqual(configurable_deduplicator.fuzzy_matching_threshold, 0.7)  # Остается прежним
        self.assertEqual(configurable_deduplicator.conflict_strategy, 'first_touch')  # Обновлено
        self.assertEqual(configurable_deduplicator.time_window_hours, 5)  # Обновлено
        self.assertEqual(configurable_deduplicator.additional_match_criteria, ['user_id', 'email'])  # Остается прежним


if __name__ == '__main__':
    unittest.main()
