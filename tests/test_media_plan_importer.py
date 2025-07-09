"""
Тесты для импортера медиаплана.
"""
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.media_plan_importer import MediaPlanImporter

class TestMediaPlanImporter(unittest.TestCase):
    
    def setUp(self):
        self.mock_sheets_client = MagicMock()
        self.importer = MediaPlanImporter(
            spreadsheet_id="test_spreadsheet_id",
            sheets_client=self.mock_sheets_client
        )
    
    def test_import_media_plan(self):
        """Тестирует импорт медиаплана."""
        # Мок данные из Google Sheets
        mock_data = [
            {
                'month': '2025-07-01',
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'goal': 'Заказы',
                'source': 'Google search',
                'planned_budget': '1000.0',
                'planned_impressions': '50000',
                'planned_clicks': '2000',
                'planned_orders': '100',
                'planned_revenue': '20000.0'
            },
            {
                'month': '2025-07',
                'restaurant': 'Белла',
                'country': 'Казахстан',
                'campaign_type': 'Awareness',
                'goal': 'Охват/Узнаваемость',
                'source': 'Мета',
                'planned_budget': '1500.0',
                'planned_impressions': '200000',
                'planned_clicks': '',
                'planned_orders': '0',
                'planned_revenue': ''
            }
        ]
        
        self.mock_sheets_client.get_all_records.return_value = mock_data
        
        # Вызываем метод
        result = self.importer.import_media_plan(month="2025-07")
        
        # Проверяем вызов метода
        self.mock_sheets_client.get_all_records.assert_called_once()
        
        # Проверяем, что данные были правильно преобразованы
        self.assertEqual(len(result), 2)
        
        # Проверяем первую запись
        self.assertEqual(result[0]['restaurant'], 'Тануки')
        self.assertEqual(result[0]['planned_budget'], 1000.0)
        self.assertEqual(result[0]['planned_impressions'], 50000.0)
        self.assertEqual(result[0]['month'], '2025-07-01')
        
        # Проверяем вторую запись с пустыми значениями
        self.assertEqual(result[1]['restaurant'], 'Белла')
        self.assertEqual(result[1]['planned_budget'], 1500.0)
        self.assertEqual(result[1]['planned_clicks'], None)
        self.assertEqual(result[1]['month'], '2025-07-01')
    
    def test_transform_media_plan_data(self):
        """Тестирует преобразование данных медиаплана."""
        raw_data = [
            {
                'month': '07/2025',  # Формат MM/YYYY
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'goal': 'Заказы',
                'source': 'Google search',
                'planned_budget': '1 000,50',  # Пробелы и запятая
                'planned_impressions': '50000',
                'planned_clicks': '2000',
                'planned_orders': '100',
                'planned_revenue': '20000.0'
            },
            {
                'month': '01/07/2025',  # Формат DD/MM/YYYY
                'restaurant': 'Белла',
                'country': 'Казахстан',
                'campaign_type': 'Awareness',
                'goal': 'Охват/Узнаваемость',
                'source': 'Мета',
                'planned_budget': '1500,75',  # Запятая вместо точки
                'planned_impressions': '200 000',  # Пробелы в числе
                'planned_clicks': '',  # Пустое значение
                'planned_orders': '0',
                'planned_revenue': ''  # Пустое значение
            }
        ]
        
        # Вызываем метод
        transformed_data = self.importer._transform_media_plan_data(raw_data)
        
        # Проверяем результаты преобразования
        self.assertEqual(len(transformed_data), 2)
        
        # Проверяем первую запись
        self.assertEqual(transformed_data[0]['month'], '2025-07-01')
        self.assertEqual(transformed_data[0]['planned_budget'], 1000.5)
        
        # Проверяем вторую запись
        self.assertEqual(transformed_data[1]['month'], '2025-07-01')
        self.assertEqual(transformed_data[1]['planned_budget'], 1500.75)
        self.assertEqual(transformed_data[1]['planned_impressions'], 200000)
        self.assertEqual(transformed_data[1]['planned_clicks'], None)
    
    @patch('os.makedirs')
    @patch('json.dump')
    @patch('builtins.open')
    def test_save_media_plan_to_file(self, mock_open, mock_json_dump, mock_makedirs):
        """Тестирует сохранение медиаплана в файл."""
        media_plan_data = [
            {
                'id': 1,
                'month': '2025-07-01',
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'goal': 'Заказы',
                'source': 'Google search',
                'planned_budget': 1000.0
            }
        ]
        
        # Мок datetime.now() для предсказуемого имени файла
        with patch('src.media_plan_importer.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2025, 7, 1)
            mock_dt.strftime = datetime.strftime
            
            output_file = self.importer.save_media_plan_to_file(media_plan_data)
        
        # Проверяем, что директория была создана
        mock_makedirs.assert_called_once()
        
        # Проверяем, что файл был открыт для записи
        mock_open.assert_called_once()
        
        # Проверяем, что json.dump был вызван с правильными аргументами
        mock_json_dump.assert_called_once()
        
        # Проверяем возвращаемое значение
        self.assertIn('media_plan_2025-07-01.json', output_file)

if __name__ == '__main__':
    unittest.main()
