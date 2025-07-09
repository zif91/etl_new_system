"""
Тесты для системы атрибуции источников заказов.
"""

import unittest
from typing import Dict, List, Any

from src.attribution import (
    AttributionModel, AttributionSource, AttributionRules, AttributionSourceAssigner
)


class TestAttributionRules(unittest.TestCase):
    """
    Тесты для класса AttributionRules.
    """
    
    def setUp(self):
        """
        Настраивает тестовые данные перед каждым тестом.
        """
        self.rules = AttributionRules()
    
    def test_priority_list_default(self):
        """
        Проверяет, что список приоритетов по умолчанию корректный.
        """
        expected_priority_list = [
            AttributionSource.PROMO_CODE,
            AttributionSource.UTM_SOURCE,
            AttributionSource.REFERRAL,
            AttributionSource.DIRECT,
            AttributionSource.ORGANIC
        ]
        
        self.assertEqual(self.rules.priority_list, expected_priority_list)
    
    def test_get_priority(self):
        """
        Проверяет корректность определения приоритетов источников.
        """
        # Проверяем приоритеты из списка
        self.assertEqual(self.rules.get_priority(AttributionSource.PROMO_CODE), 0)
        self.assertEqual(self.rules.get_priority(AttributionSource.UTM_SOURCE), 1)
        self.assertEqual(self.rules.get_priority(AttributionSource.ORGANIC), 4)
        
        # Проверяем приоритет несуществующего источника
        self.assertEqual(self.rules.get_priority('unknown_source'), 5)
    
    def test_standardize_source(self):
        """
        Проверяет корректность стандартизации источников.
        """
        # Проверяем маппинг известных источников
        self.assertEqual(
            self.rules.standardize_source('google', 'cpc'),
            AttributionSource.GOOGLE_ADS
        )
        self.assertEqual(
            self.rules.standardize_source('facebook', 'paid'),
            AttributionSource.FACEBOOK
        )
        self.assertEqual(
            self.rules.standardize_source('email', 'email'),
            AttributionSource.EMAIL
        )
        
        # Проверяем маппинг без учета регистра
        self.assertEqual(
            self.rules.standardize_source('Google', 'CPC'),
            AttributionSource.GOOGLE_ADS
        )
        
        # Проверяем источник, которого нет в маппинге
        self.assertEqual(
            self.rules.standardize_source('tiktok', 'paid'),
            'tiktok'
        )
        
        # Проверяем пустые источники
        self.assertEqual(
            self.rules.standardize_source(None, None),
            AttributionSource.UTM_SOURCE  # Дефолтный источник
        )


class TestAttributionSourceAssigner(unittest.TestCase):
    """
    Тесты для класса AttributionSourceAssigner.
    """
    
    def setUp(self):
        """
        Настраивает тестовые данные перед каждым тестом.
        """
        self.assigner = AttributionSourceAssigner()
        
        # Тестовые транзакции
        self.transactions = [
            # Транзакция с промокодом
            {
                'transaction_id': 'TXN001',
                'is_promo_order': True,
                'promo_code': 'SUMMER20',
                'promo_source': 'facebook_ads',
                'sourceMedium': 'facebook / paid',
                'campaign': 'summer_campaign',
                'purchase_revenue': 1000.0
            },
            # Транзакция с UTM из Google Ads
            {
                'transaction_id': 'TXN002',
                'is_promo_order': False,
                'sourceMedium': 'google / cpc',
                'campaign': 'search_brand',
                'purchase_revenue': 1500.0
            },
            # Органическая транзакция из Google
            {
                'transaction_id': 'TXN003',
                'is_promo_order': False,
                'sourceMedium': 'google / organic',
                'campaign': '(not set)',
                'purchase_revenue': 750.0
            },
            # Прямой переход
            {
                'transaction_id': 'TXN004',
                'is_promo_order': False,
                'sourceMedium': '(direct) / (none)',
                'campaign': '(not set)',
                'purchase_revenue': 500.0
            },
            # Email
            {
                'transaction_id': 'TXN005',
                'is_promo_order': False,
                'sourceMedium': 'email / email',
                'campaign': 'newsletter',
                'purchase_revenue': 1200.0
            }
        ]
        
        # Транзакции с конфликтами атрибуции (по order_id)
        self.conflict_transactions = [
            # Прямой переход (низкий приоритет)
            {
                'transaction_id': 'TXN006',
                'order_id': 'ORD001',
                'is_promo_order': False,
                'sourceMedium': '(direct) / (none)',
                'campaign': '(not set)',
                'purchase_revenue': 1000.0
            },
            # Google Ads (средний приоритет)
            {
                'transaction_id': 'TXN007',
                'order_id': 'ORD001',
                'is_promo_order': False,
                'sourceMedium': 'google / cpc',
                'campaign': 'search_brand',
                'purchase_revenue': 1000.0
            },
            # Промокод (высокий приоритет)
            {
                'transaction_id': 'TXN008',
                'order_id': 'ORD001',
                'is_promo_order': True,
                'promo_code': 'SUMMER20',
                'promo_source': 'facebook_ads',
                'sourceMedium': 'facebook / paid',
                'campaign': 'summer_campaign',
                'purchase_revenue': 1000.0
            }
        ]
    
    def test_assign_attribution_source(self):
        """
        Проверяет корректность назначения источников атрибуции для отдельных транзакций.
        """
        # Проверяем атрибуцию для транзакции с промокодом
        result = self.assigner.assign_attribution_source(self.transactions[0])
        self.assertEqual(result['attribution_source'], AttributionSource.PROMO_CODE)
        self.assertEqual(result['attribution_details']['promo_code'], 'SUMMER20')
        self.assertEqual(result['attribution_details']['model_used'], AttributionModel.LAST_CLICK)
        
        # Проверяем атрибуцию для транзакции из Google Ads
        result = self.assigner.assign_attribution_source(self.transactions[1])
        self.assertEqual(result['attribution_source'], AttributionSource.GOOGLE_ADS)
        self.assertEqual(result['attribution_details']['utm_campaign'], 'search_brand')
        self.assertTrue(result['attribution_details']['is_paid'])
        
        # Проверяем атрибуцию для органической транзакции
        result = self.assigner.assign_attribution_source(self.transactions[2])
        self.assertEqual(result['attribution_source'], AttributionSource.GOOGLE_ORGANIC)
        self.assertFalse(result['attribution_details']['is_paid'])
    
    def test_assign_attribution_to_transactions(self):
        """
        Проверяет корректность назначения источников атрибуции для списка транзакций.
        """
        results = self.assigner.assign_attribution_to_transactions(self.transactions)
        
        # Проверяем, что все транзакции получили атрибуцию
        self.assertEqual(len(results), len(self.transactions))
        
        # Проверяем статистику атрибуции
        stats = self.assigner.get_stats()
        self.assertEqual(stats['processed'], len(self.transactions))
        self.assertEqual(stats['sources'][AttributionSource.PROMO_CODE], 1)
        self.assertEqual(stats['sources'][AttributionSource.GOOGLE_ADS], 1)
        self.assertEqual(stats['sources'][AttributionSource.GOOGLE_ORGANIC], 1)
        self.assertEqual(stats['sources'][AttributionSource.DIRECT], 1)
        self.assertEqual(stats['sources'][AttributionSource.EMAIL], 1)
        self.assertEqual(stats['models_used'][AttributionModel.LAST_CLICK], 5)
    
    def test_resolve_attribution_conflict(self):
        """
        Проверяет разрешение конфликтов атрибуции между источниками.
        """
        # Создаем тестовый набор транзакций с атрибуцией
        test_transactions = [
            {
                'transaction_id': 'TXN101',
                'order_id': 'ORD101',
                'attribution_source': AttributionSource.DIRECT,
                'attribution_details': {
                    'source_priority': 4  # Низший приоритет
                }
            },
            {
                'transaction_id': 'TXN102',
                'order_id': 'ORD101',
                'attribution_source': AttributionSource.UTM_SOURCE,
                'attribution_details': {
                    'source_priority': 1  # Средний приоритет
                }
            },
            {
                'transaction_id': 'TXN103',
                'order_id': 'ORD101',
                'attribution_source': AttributionSource.PROMO_CODE,
                'attribution_details': {
                    'source_priority': 0  # Высший приоритет
                }
            }
        ]
        
        # Разрешаем конфликты
        resolved = self.assigner.resolve_attribution_conflict(
            test_transactions, 'order_id'
        )
        
        # Проверяем, что осталась только одна транзакция с order_id=ORD101
        self.assertEqual(len(resolved), 1)
        
        # Проверяем, что выбрана транзакция с промокодом (высший приоритет)
        self.assertEqual(resolved[0]['transaction_id'], 'TXN103')
        self.assertEqual(resolved[0]['attribution_source'], AttributionSource.PROMO_CODE)
    
    def test_empty_input(self):
        """
        Проверяет обработку пустого списка транзакций.
        """
        results = self.assigner.assign_attribution_to_transactions([])
        self.assertEqual(len(results), 0)
        
        resolved = self.assigner.resolve_attribution_conflict([], 'order_id')
        self.assertEqual(len(resolved), 0)


if __name__ == '__main__':
    unittest.main()
