"""
Тесты для Google Sheets клиента.
"""

import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Добавляем путь проекта в sys.path, чтобы корректно импортировать модули
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.google_sheets_client import GoogleSheetsClient


class TestGoogleSheetsClient(unittest.TestCase):
    """
    Тесты для GoogleSheetsClient
    """
    
    @patch('src.google_sheets_client.Credentials')
    @patch('src.google_sheets_client.gspread')
    def setUp(self, mock_gspread, mock_credentials):
        """
        Настройка тестового окружения
        """
        # Устанавливаем моки для тестирования
        self.mock_gspread = mock_gspread
        self.mock_credentials = mock_credentials
        
        # Создаем моки для объектов gspread
        self.mock_client = MagicMock()
        self.mock_spreadsheet = MagicMock()
        self.mock_worksheet = MagicMock()
        
        # Настраиваем поведение моков
        mock_gspread.authorize.return_value = self.mock_client
        self.mock_client.open_by_key.return_value = self.mock_spreadsheet
        self.mock_spreadsheet.worksheet.return_value = self.mock_worksheet
        
        # Устанавливаем тестовые параметры окружения
        os.environ['GOOGLE_SHEETS_CREDENTIALS_JSON'] = '/fake/path/credentials.json'
        os.environ['PROMO_SHEET_ID'] = 'test_sheet_id'
        
        # Создаем клиент Google Sheets для тестирования
        self.sheets_client = GoogleSheetsClient()
    
    def test_init(self):
        """
        Тест инициализации клиента
        """
        self.assertEqual(self.sheets_client.credentials_path, '/fake/path/credentials.json')
        self.assertEqual(self.sheets_client.sheet_id, 'test_sheet_id')
        self.mock_gspread.authorize.assert_called_once()
    
    def test_open_spreadsheet(self):
        """
        Тест открытия таблицы
        """
        spreadsheet = self.sheets_client.open_spreadsheet()
        self.mock_client.open_by_key.assert_called_with('test_sheet_id')
        self.assertEqual(spreadsheet, self.mock_spreadsheet)
    
    def test_get_worksheet(self):
        """
        Тест получения листа
        """
        worksheet = self.sheets_client.get_worksheet('Test Sheet')
        self.mock_spreadsheet.worksheet.assert_called_with('Test Sheet')
        self.assertEqual(worksheet, self.mock_worksheet)
    
    def test_get_all_records(self):
        """
        Тест получения всех записей
        """
        test_records = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
        self.mock_worksheet.get_all_records.return_value = test_records
        
        records = self.sheets_client.get_all_records('Test Sheet')
        
        self.mock_spreadsheet.worksheet.assert_called_with('Test Sheet')
        self.mock_worksheet.get_all_records.assert_called_once()
        self.assertEqual(records, test_records)
    
    def test_get_range_values(self):
        """
        Тест получения значений из диапазона
        """
        test_values = [['a', 'b'], [1, 2], [3, 4]]
        self.mock_worksheet.get.return_value = test_values
        
        values = self.sheets_client.get_range_values('Test Sheet', 'A1:B3')
        
        self.mock_spreadsheet.worksheet.assert_called_with('Test Sheet')
        self.mock_worksheet.get.assert_called_with('A1:B3')
        self.assertEqual(values, test_values)


if __name__ == '__main__':
    unittest.main()
