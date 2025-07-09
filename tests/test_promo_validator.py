"""
Тесты для валидатора и трансформатора промокодов.
"""

import unittest
import os
import sys
from datetime import date

# Добавляем путь проекта в sys.path, чтобы корректно импортировать модули
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.promo_validator import PromoCodeValidator, PromoCodeTransformer


class TestPromoCodeValidator(unittest.TestCase):
    """
    Тесты для PromoCodeValidator
    """
    
    def setUp(self):
        """
        Настройка тестового окружения
        """
        self.validator = PromoCodeValidator()
    
    def test_validate_promo_code(self):
        """
        Тест валидации промокода
        """
        # Валидные промокоды
        self.assertTrue(self.validator.validate_promo_code('TEST20'))
        self.assertTrue(self.validator.validate_promo_code('PROMO_50'))
        self.assertTrue(self.validator.validate_promo_code('SALE-2023'))
        
        # Невалидные промокоды
        self.assertFalse(self.validator.validate_promo_code(''))
        self.assertFalse(self.validator.validate_promo_code(None))
        self.assertFalse(self.validator.validate_promo_code('AB'))  # Слишком короткий
        self.assertFalse(self.validator.validate_promo_code('PROMO!CODE'))  # Недопустимые символы
        self.assertFalse(self.validator.validate_promo_code('PROMO CODE'))  # Пробелы не допускаются
    
    def test_validate_order_id(self):
        """
        Тест валидации ID заказа
        """
        # Валидные ID заказов
        self.assertTrue(self.validator.validate_order_id('ORD123'))
        self.assertTrue(self.validator.validate_order_id('ORDER_123'))
        self.assertTrue(self.validator.validate_order_id('ORDER-123'))
        
        # Невалидные ID заказов
        self.assertFalse(self.validator.validate_order_id(''))
        self.assertFalse(self.validator.validate_order_id(None))
        self.assertFalse(self.validator.validate_order_id('ORDER#123'))  # Недопустимые символы
    
    def test_validate_transaction_id(self):
        """
        Тест валидации ID транзакции
        """
        # Валидные ID транзакций
        self.assertTrue(self.validator.validate_transaction_id('TXN001'))
        self.assertTrue(self.validator.validate_transaction_id('TRANSACTION_123'))
        self.assertTrue(self.validator.validate_transaction_id('TX-123-456'))
        
        # Невалидные ID транзакций
        self.assertFalse(self.validator.validate_transaction_id(''))
        self.assertFalse(self.validator.validate_transaction_id(None))
        self.assertFalse(self.validator.validate_transaction_id('TXN#123'))  # Недопустимые символы
    
    def test_validate_restaurant(self):
        """
        Тест валидации ресторана
        """
        # Валидные рестораны без списка разрешенных
        self.assertTrue(self.validator.validate_restaurant('Тануки'))
        self.assertTrue(self.validator.validate_restaurant('Каспийка'))
        self.assertTrue(self.validator.validate_restaurant('Белла'))
        
        # Валидные рестораны со списком разрешенных
        self.assertTrue(self.validator.validate_restaurant('Тануки', ['Тануки', 'Каспийка', 'Белла']))
        self.assertTrue(self.validator.validate_restaurant('Каспийка', ['Тануки', 'Каспийка', 'Белла']))
        
        # Невалидные рестораны
        self.assertFalse(self.validator.validate_restaurant(''))
        self.assertFalse(self.validator.validate_restaurant(None))
        self.assertFalse(self.validator.validate_restaurant('Другой', ['Тануки', 'Каспийка', 'Белла']))
    
    def test_validate_country(self):
        """
        Тест валидации страны
        """
        # Валидные страны без списка разрешенных
        self.assertTrue(self.validator.validate_country('Казахстан'))
        self.assertTrue(self.validator.validate_country('Узбекистан'))
        
        # Валидные страны со списком разрешенных
        self.assertTrue(self.validator.validate_country('Казахстан', ['Казахстан', 'Узбекистан']))
        self.assertTrue(self.validator.validate_country('Узбекистан', ['Казахстан', 'Узбекистан']))
        
        # Невалидные страны
        self.assertFalse(self.validator.validate_country(''))
        self.assertFalse(self.validator.validate_country(None))
        self.assertFalse(self.validator.validate_country('Россия', ['Казахстан', 'Узбекистан']))
    
    def test_parse_date(self):
        """
        Тест парсинга даты
        """
        # Валидные даты в разных форматах
        self.assertEqual(self.validator.parse_date('2023-01-01'), date(2023, 1, 1))
        self.assertEqual(self.validator.parse_date('01.01.2023'), date(2023, 1, 1))
        self.assertEqual(self.validator.parse_date('01/01/2023'), date(2023, 1, 1))
        self.assertEqual(self.validator.parse_date('01/01/2023'), date(2023, 1, 1))
        
        # Невалидные даты
        self.assertIsNone(self.validator.parse_date(''))
        self.assertIsNone(self.validator.parse_date(None))
        self.assertIsNone(self.validator.parse_date('invalid_date'))
        self.assertIsNone(self.validator.parse_date('2023.01.01'))  # Неподдерживаемый формат
    
    def test_parse_amount(self):
        """
        Тест парсинга суммы
        """
        # Валидные суммы в разных форматах
        self.assertEqual(self.validator.parse_amount('1500'), 1500.0)
        self.assertEqual(self.validator.parse_amount('1500.00'), 1500.0)
        self.assertEqual(self.validator.parse_amount('1,500.00'), 1500.0)
        self.assertEqual(self.validator.parse_amount('1 500,00'), 1500.0)
        self.assertEqual(self.validator.parse_amount(1500), 1500.0)
        self.assertEqual(self.validator.parse_amount(1500.0), 1500.0)
        
        # Невалидные суммы
        self.assertIsNone(self.validator.parse_amount(''))
        self.assertIsNone(self.validator.parse_amount(None))
        self.assertIsNone(self.validator.parse_amount('invalid_amount'))
        self.assertIsNone(self.validator.parse_amount('-1500'))  # Отрицательная сумма


class TestPromoCodeTransformer(unittest.TestCase):
    """
    Тесты для PromoCodeTransformer
    """
    
    def setUp(self):
        """
        Настройка тестового окружения
        """
        self.transformer = PromoCodeTransformer()
    
    def test_transform_record_valid(self):
        """
        Тест трансформации валидной записи
        """
        # Валидная запись
        record = {
            'promo_code': 'TEST20',
            'order_id': 'ORD123',
            'transaction_id': 'TXN001',
            'order_date': '2023-01-01',
            'order_amount': '1500.00',
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'promo_source': 'facebook_ads'
        }
        
        expected_transformed = {
            'promo_code': 'TEST20',
            'order_id': 'ORD123',
            'transaction_id': 'TXN001',
            'order_date': date(2023, 1, 1),
            'order_amount': 1500.0,
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'promo_source': 'facebook_ads'
        }
        
        transformed_record, errors = self.transformer.transform_record(record)
        
        self.assertIsNotNone(transformed_record)
        self.assertEqual(len(errors), 0)
        
        # Проверяем каждое поле
        for field, expected_value in expected_transformed.items():
            self.assertEqual(transformed_record[field], expected_value)
    
    def test_transform_record_without_transaction_id(self):
        """
        Тест трансформации записи без ID транзакции
        """
        # Запись без ID транзакции
        record = {
            'promo_code': 'TEST20',
            'order_id': 'ORD123',
            'order_date': '2023-01-01',
            'order_amount': '1500.00',
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'promo_source': 'facebook_ads'
        }
        
        transformed_record, errors = self.transformer.transform_record(record)
        
        self.assertIsNotNone(transformed_record)
        self.assertEqual(len(errors), 0)
        self.assertEqual(transformed_record['transaction_id'], 'ORD123')  # Должен использовать order_id как transaction_id
    
    def test_transform_record_invalid(self):
        """
        Тест трансформации невалидной записи
        """
        # Невалидная запись (отсутствует order_id)
        record = {
            'promo_code': 'TEST20',
            'order_date': '2023-01-01',
            'order_amount': '1500.00',
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'promo_source': 'facebook_ads'
        }
        
        transformed_record, errors = self.transformer.transform_record(record)
        
        self.assertIsNone(transformed_record)
        self.assertGreater(len(errors), 0)
        self.assertIn('Missing required field: order_id', errors)
    
    def test_transform_records(self):
        """
        Тест трансформации списка записей
        """
        # Список записей (валидные и невалидные)
        records = [
            {
                'promo_code': 'TEST20',
                'order_id': 'ORD123',
                'transaction_id': 'TXN001',
                'order_date': '2023-01-01',
                'order_amount': '1500.00',
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'promo_source': 'facebook_ads'
            },
            {
                'promo_code': 'TEST30',
                'order_id': 'ORD124',
                'order_date': '01.01.2023',
                'order_amount': '2000,50',
                'restaurant': 'Белла',
                'country': 'Узбекистан'
            },
            {
                'promo_code': 'TEST40',
                # Отсутствует order_id (обязательное поле)
                'transaction_id': 'TXN003',
                'order_date': '2023-01-03',
                'order_amount': '2500.00',
                'restaurant': 'Каспийка',
                'country': 'Казахстан'
            },
            {
                'promo_code': 'TEST50',
                'order_id': 'ORD126',
                'order_date': 'invalid_date',  # Невалидная дата
                'order_amount': '3000.00',
                'restaurant': 'Тануки',
                'country': 'Казахстан'
            }
        ]
        
        transformed_records, errors_by_index = self.transformer.transform_records(records)
        
        # Должно быть 2 валидных записи и 2 записи с ошибками
        self.assertEqual(len(transformed_records), 2)
        self.assertEqual(len(errors_by_index), 2)
        
        # Проверяем, что ошибки есть для индексов 2 и 3
        self.assertIn(2, errors_by_index)
        self.assertIn(3, errors_by_index)


if __name__ == '__main__':
    unittest.main()
