"""
Тесты для интегратора сравнения с медиапланом.
"""
import unittest
from unittest.mock import MagicMock, patch, mock_open, call
import json
import os
from datetime import datetime
from src.media_plan_integrator import compare_with_media_plan_task

class TestMediaPlanIntegrator(unittest.TestCase):
    """Тесты для модуля интеграции сравнения с медиапланом."""
    
    @patch('src.media_plan_integrator.MediaPlanMatcher')
    @patch('src.media_plan_integrator.get_connection')
    @patch('src.media_plan_integrator.os.makedirs')
    @patch('src.media_plan_integrator.json.dump')
    @patch('src.media_plan_integrator.logger')
    def test_with_file_path(self, mock_logger, mock_json_dump, mock_makedirs, mock_get_conn, mock_matcher_class):
        """Тестирование сценария с указанным путем к файлу медиаплана."""
        # Подготавливаем данные для теста
        media_plan_data = [
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
            }
        ]
        
        # Настраиваем мок для открытия файла медиаплана
        read_file_mock = mock_open(read_data=json.dumps(media_plan_data))
        
        # Настраиваем моки для базы данных
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Настраиваем данные кампаний
        campaign_data = [
            {
                'date': datetime(2025, 7, 1),
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'campaign_goal': 'Заказы',
                'source': 'Google search',
                'campaign_name': 'Test Campaign',
                'spend': 1200.0,
                'impressions': 55000,
                'clicks': 1800,
                'orders': 110,
                'revenue': 25000.0
            }
        ]
        
        # Модифицируем мок для _get_campaign_metrics_from_db
        with patch('src.media_plan_integrator._get_campaign_metrics_from_db', return_value=campaign_data):
            # Настраиваем моки для матчера
            mock_matcher = MagicMock()
            mock_matcher_class.return_value = mock_matcher
            
            # Настраиваем результат сопоставления
            mock_matcher.match_campaign_to_media_plan.return_value = {
                'media_plan_id': 1,
                'variances': {
                    'spend': {'fact': 1200.0, 'plan': 1000.0, 'absolute_variance': 200.0},
                    'impressions': {'fact': 55000, 'plan': 50000, 'absolute_variance': 5000},
                    'clicks': {'fact': 1800, 'plan': 2000, 'absolute_variance': -200},
                    'orders': {'fact': 110, 'plan': 100, 'absolute_variance': 10},
                    'revenue': {'fact': 25000.0, 'plan': 20000.0, 'absolute_variance': 5000.0}
                }
            }
            
            # Выполняем тестируемую функцию с моком для чтения и записи файлов
            with patch('builtins.open', read_file_mock):
                result = compare_with_media_plan_task(
                    media_plan_path='/path/to/media_plan.json',
                    execution_date='2025-07-15'
                )
        
        # Проверяем, что матчер был создан с правильными данными
        mock_matcher_class.assert_called_once()
        
        # Проверяем, что матчер был использован для сопоставления
        mock_matcher.match_campaign_to_media_plan.assert_called_once()
        
        # Проверяем, что директория для результатов была создана
        mock_makedirs.assert_called_once_with('data/comparisons', exist_ok=True)
        
        # Проверяем, что результаты были сохранены в JSON
        mock_json_dump.assert_called_once()
        
        # Проверяем, что было логирование
        mock_logger.info.assert_any_call(f"Выполняем сравнение с медиапланом для даты: 2025-07-15")
        
        # Проверяем, что результат не пустой
        self.assertIsNotNone(result)
    
    @patch('src.media_plan_integrator.MediaPlanImporter')
    @patch('src.media_plan_integrator.MediaPlanMatcher')
    @patch('src.media_plan_integrator.get_connection')
    @patch('builtins.open', new_callable=mock_open)
    @patch('src.media_plan_integrator.os.makedirs')
    @patch('src.media_plan_integrator.json.dump')
    @patch('src.media_plan_integrator.logger')
    def test_without_file_path(
        self, mock_logger, mock_json_dump, mock_makedirs, mock_file_open, 
        mock_get_conn, mock_matcher_class, mock_importer_class
    ):
        """Тестирование сценария без указания пути к файлу медиаплана."""
        # Настраиваем моки для импортера
        mock_importer = MagicMock()
        mock_importer_class.return_value = mock_importer
        
        media_plan_data = [
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
            }
        ]
        mock_importer.import_media_plan.return_value = media_plan_data
        
        # Настраиваем моки для базы данных
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Настраиваем данные кампаний
        campaign_data = [
            {
                'date': datetime(2025, 7, 1),
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'campaign_type': 'Performance',
                'campaign_goal': 'Заказы',
                'source': 'Google search',
                'campaign_name': 'Test Campaign',
                'spend': 1200.0,
                'impressions': 55000,
                'clicks': 1800,
                'orders': 110,
                'revenue': 25000.0
            }
        ]
        
        # Модифицируем мок для _get_campaign_metrics_from_db
        with patch('src.media_plan_integrator._get_campaign_metrics_from_db', return_value=campaign_data):
            # Настраиваем моки для матчера
            mock_matcher = MagicMock()
            mock_matcher_class.return_value = mock_matcher
            
            # Настраиваем результат сопоставления без совпадения
            mock_matcher.match_campaign_to_media_plan.return_value = None
            
            # Выполняем тестируемую функцию
            result = compare_with_media_plan_task(
                media_plan_path=None,
                execution_date='2025-07-15'
            )
        
        # Проверяем, что импортер был вызван с правильным месяцем
        mock_importer.import_media_plan.assert_called_once_with('2025-07')
        
        # Проверяем, что матчер был создан с правильными данными
        mock_matcher_class.assert_called_once()
        
        # Проверяем, что матчер был использован для сопоставления
        # Для этого проверим, что у него был вызван метод, а не проверяем конкретные аргументы
        mock_matcher.match_campaign_to_media_plan.assert_called_once()
        
        # Проверяем, что директория для результатов была создана
        mock_makedirs.assert_called_once_with('data/comparisons', exist_ok=True)
        
        # Проверяем, что результаты были сохранены в JSON
        mock_json_dump.assert_called_once()
        
        # Проверяем, что было логирование
        mock_logger.info.assert_any_call(f"Выполняем сравнение с медиапланом для даты: 2025-07-15")
        
        # Проверяем, что результат не пустой
        self.assertIsNotNone(result)
    
    @patch('src.media_plan_integrator.MediaPlanImporter')
    @patch('src.media_plan_integrator.MediaPlanMatcher')
    @patch('src.media_plan_integrator.get_connection')
    @patch('src.media_plan_integrator.logger')
    def test_empty_media_plan(self, mock_logger, mock_get_conn, mock_matcher_class, mock_importer_class):
        """Тестирование сценария с пустым медиапланом."""
        # Настраиваем моки для импортера, возвращающего пустой медиаплан
        mock_importer = MagicMock()
        mock_importer_class.return_value = mock_importer
        mock_importer.import_media_plan.return_value = []
        
        # Выполняем тестируемую функцию
        result = compare_with_media_plan_task(
            media_plan_path=None,
            execution_date='2025-07-15'
        )
        
        # Проверяем, что импортер был вызван
        mock_importer.import_media_plan.assert_called_once_with('2025-07')
        
        # Проверяем, что матчер не был создан
        mock_matcher_class.assert_not_called()
        
        # Проверяем, что было логирование предупреждения
        mock_logger.warning.assert_any_call("Данные медиаплана отсутствуют, задача завершается.")
        
        # Проверяем, что результат пустой
        self.assertIsNone(result)
    
    @patch('src.media_plan_integrator.MediaPlanMatcher')
    @patch('src.media_plan_integrator.get_connection')
    @patch('src.media_plan_integrator.logger')
    def test_database_error(self, mock_logger, mock_get_conn, mock_matcher_class):
        """Тестирование сценария с ошибкой базы данных."""
        # Настраиваем моки для чтения файла с медиапланом
        with patch('builtins.open', mock_open(read_data=json.dumps([{'id': 1}]))):
            # Настраиваем моки для базы данных с ошибкой
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Симулируем ошибку базы данных
            mock_cursor.execute.side_effect = Exception("Database connection error")
            
            # Выполняем тестируемую функцию
            result = compare_with_media_plan_task(
                media_plan_path='/path/to/media_plan.json',
                execution_date='2025-07-15'
            )
            
            # Проверяем, что была попытка выполнить запрос, которая вызвала ошибку
            mock_cursor.execute.assert_called_once()
            
            # Проверяем, что было логирование ошибки
            mock_logger.error.assert_any_call("Ошибка при получении данных из БД: Database connection error")
            
            # Проверяем, что результат пустой из-за ошибки
            self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
