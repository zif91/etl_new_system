"""
Тесты для менеджера базы данных промокодов.
"""

import unittest
from unittest.mock import patch, MagicMock
import os
import sys
from datetime import date, datetime

# Добавляем путь проекта в sys.path, чтобы корректно импортировать модули
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.promo_db_manager import PromoDBManager


class TestPromoDBManager(unittest.TestCase):
    """
    Тесты для PromoDBManager
    """
    
    def setUp(self):
        """
        Настройка тестового окружения
        """
        # Создаем менеджер базы данных для тестирования
        self.db_manager = PromoDBManager()
    
    @patch('src.promo_db_manager.get_connection')
    def test_insert_promo_order(self, mock_get_connection):
        """Тест вставки одной записи о промокоде"""
        mock_conn = MagicMock()
        mock_get_connection.return_value.__enter__.return_value = mock_conn
        
        self.mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        # Тестовые данные
        self.test_order = {
            'promo_code': 'TEST20',
            'order_id': 'ORD123',
            'transaction_id': 'TXN001',
            'order_date': date(2023, 1, 1),
            'order_amount': 1500.0,
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'promo_source': 'facebook_ads'
        }
        
        # Настраиваем моки
        self.mock_cursor.fetchone.return_value = (1,) # Возвращаем кортеж, как это делает курсор
        self.mock_cursor.lastrowid = 1 # Устанавливаем ID последней вставленной строки

        result = self.db_manager.insert_promo_order(self.test_order)
        
        self.assertEqual(result, 1)
        self.mock_cursor.execute.assert_called_once()


    @patch('src.promo_db_manager.get_connection')
    def test_bulk_insert_promo_orders(self, mock_get_connection):
        """Тест массовой вставки записей о промокодах"""
        mock_conn = MagicMock()
        mock_get_connection.return_value.__enter__.return_value = mock_conn
        
        self.mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        test_orders = [
            {'promo_code': 'BULK1', 'order_id': 'B1', 'order_date': '2023-01-01', 'order_amount': 100, 'restaurant': 'A', 'country': 'KZ'},
            {'promo_code': 'BULK2', 'order_id': 'B2', 'order_date': '2023-01-02', 'order_amount': 200, 'restaurant': 'B', 'country': 'UZ'},
        ]
        
        # Мокируем executemany для успешной вставки
        self.mock_cursor.executemany.return_value = None
        self.mock_cursor.rowcount = 2 # executemany не возвращает lastrowid, но обновляет rowcount

        result = self.db_manager.bulk_insert_promo_orders(test_orders)
        
        self.assertEqual(result['inserted'], 2)
        self.assertEqual(result['failed'], 0)
        self.mock_cursor.executemany.assert_called_once()


    @patch('src.promo_db_manager.get_connection')
    def test_get_promo_orders(self, mock_get_connection):
        """Тест получения списка заказов с промокодами"""
        mock_conn = MagicMock()
        mock_get_connection.return_value.__enter__.return_value = mock_conn
        
        self.mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        expected_orders = [
            (1, 'TEST20', 'ORD123', datetime(2023, 1, 10), 150.0, 'Тануки', 'Казахстан', datetime(2023, 1, 11)),
            (2, 'TEST30', 'ORD124', datetime(2023, 1, 11), 250.0, 'Каспийка', 'Узбекистан', datetime(2023, 1, 12))
        ]
        # fetchall возвращает список кортежей
        self.mock_cursor.fetchall.return_value = expected_orders
        
        # Описываем колонки, которые вернет курсор
        self.mock_cursor.description = [
            ('id',), ('promo_code',), ('order_id',), ('order_date',), ('order_amount',), 
            ('restaurant',), ('country',), ('processing_time',)
        ]

        result = self.db_manager.get_promo_orders(start_date='2023-01-01', end_date='2023-01-31')
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['promo_code'], 'TEST20')
        self.assertEqual(result[1]['country'], 'Узбекистан')
        self.mock_cursor.execute.assert_called_once()


    @patch('src.promo_db_manager.get_connection')
    def test_get_promo_stats(self, mock_get_connection):
        """Тест получения статистики по промокодам"""
        mock_conn = MagicMock()
        mock_get_connection.return_value.__enter__.return_value = mock_conn
        
        self.mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        # fetchone возвращает один кортеж
        self.mock_cursor.fetchone.side_effect = [
            (10,), # total_orders
            (5000.0,), # total_amount
            (2,) # unique_promo_codes
        ]

        result = self.db_manager.get_promo_stats(start_date='2023-01-01', end_date='2023-01-31')

        self.assertEqual(result['total_orders'], 10)
        self.assertEqual(result['total_amount'], 5000.0)
        self.assertEqual(result['unique_promo_codes'], 2)
        self.assertEqual(self.mock_cursor.execute.call_count, 3)
    
    @patch('src.promo_db_manager.get_connection')
    def test_delete_promo_orders(self, mock_get_connection):
        """
        Тест удаления записей о промокодах
        """
        # Настраиваем моки для подключения и курсоров
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Настраиваем количество затронутых строк
        mock_cursor.rowcount = 2
        
        # Запускаем метод
        transaction_ids = ['TXN001', 'TXN002']
        result = self.db_manager.delete_promo_orders(transaction_ids)
        
        # Проверяем результаты
        self.assertEqual(result, 2)
        
        # Проверяем вызовы методов
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('src.promo_db_manager.get_connection')
    def test_delete_promo_orders_empty(self, mock_get_connection):
        """
        Тест удаления записей с пустым списком ID
        """
        # Запускаем метод с пустым списком
        result = self.db_manager.delete_promo_orders([])
        
        # Проверяем результаты
        self.assertEqual(result, 0)
        
        # Проверяем, что подключение не создавалось
        mock_get_connection.assert_not_called()


if __name__ == '__main__':
    unittest.main()
