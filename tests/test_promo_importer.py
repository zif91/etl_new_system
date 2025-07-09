"""
Тесты для импортера промокодов.
"""

import unittest
from unittest.mock import patch, MagicMock, ANY
import os
import sys
from datetime import datetime, date

# Добавляем путь проекта в sys.path, чтобы корректно импортировать модули
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.promo_importer import PromoCodeImporter


class TestPromoCodeImporter(unittest.TestCase):
    def setUp(self):
        self.mock_extractor = MagicMock(spec=GoogleSheetExtractor)
        self.mock_transformer = MagicMock(spec=PromoCodeTransformer)
        self.mock_db_manager = MagicMock(spec=PromoDBManager)
        
        self.importer = PromoCodeImporter(
            extractor=self.mock_extractor,
            transformer=self.mock_transformer,
            db_manager=self.mock_db_manager
        )

    def test_transform_promo_data(self):
        """Тест трансформации данных с использованием PromoCodeTransformer"""
        test_raw_records = [{'Промокод': 'TEST1', 'Заказ': '123'}]
        transformed_records = [{'promo_code': 'TEST1', 'order_id': '123'}]
        
        self.mock_transformer.transform_records.return_value = (transformed_records, [])
        
        result, errors = self.importer._transform_data(test_raw_records)

        self.mock_transformer.transform_records.assert_called_once_with(test_raw_records)
        self.assertEqual(result, transformed_records)
        self.assertEqual(errors, [])

    def test_transform_promo_data_with_errors(self):
        """Тест трансформации данных с ошибками"""
        test_raw_records = [{'Промокод': 'BAD'}]
        transformation_errors = [{'record': test_raw_records[0], 'errors': ['Missing required field: order_id']}]
        
        self.mock_transformer.transform_records.return_value = ([], transformation_errors)

        result, errors = self.importer._transform_data(test_raw_records)

        self.mock_transformer.transform_records.assert_called_once_with(test_raw_records)
        self.assertEqual(result, [])
        self.assertEqual(errors, transformation_errors)


    @patch('src.promo_importer.logging')
    def test_import_promo_codes(self, mock_logging):
        """Тест импорта промокодов"""
        test_raw_records = [{'Промокод': 'TEST1', 'Заказ': '123'}]
        transformed_records = [{'promo_code': 'TEST1', 'order_id': '123'}]
        
        self.mock_extractor.extract_data.return_value = test_raw_records
        self.mock_transformer.transform_records.return_value = (transformed_records, [])
        self.mock_db_manager.bulk_insert_promo_orders.return_value = {'inserted': 1, 'failed': 0}

        self.importer.run()

        self.mock_extractor.extract_data.assert_called_once()
        self.mock_transformer.transform_records.assert_called_once_with(test_raw_records)
        self.mock_db_manager.bulk_insert_promo_orders.assert_called_once_with(transformed_records)
        mock_logging.info.assert_any_call("Процесс импорта промокодов завершен. Вставлено: 1, Ошибки: 0")

    @patch('src.promo_importer.logging')
    def test_import_promo_codes_empty(self, mock_logging):
        """Тест импорта промокодов с пустым результатом"""
        self.mock_extractor.extract_data.return_value = []
        # transform_records не должен вызываться, если нет данных
        self.mock_transformer.transform_records.return_value = ([], []) 

        self.importer.run()

        self.mock_extractor.extract_data.assert_called_once()
        self.mock_transformer.transform_records.assert_not_called()
        self.mock_db_manager.bulk_insert_promo_orders.assert_not_called()
        mock_logging.info.assert_any_call("Не найдено записей для импорта.")

    @patch('src.promo_importer.logging')
    def test_import_promo_codes_with_errors(self, mock_logging):
        """Тест импорта промокодов с ошибками при сохранении"""
        test_raw_records = [{'Промокод': 'TEST1', 'Заказ': '123'}]
        transformed_records = [{'promo_code': 'TEST1', 'order_id': '123'}]
        transformation_errors = [{'record': {'Промокод': 'BAD'}, 'errors': ['error']}]

        self.mock_extractor.extract_data.return_value = test_raw_records
        self.mock_transformer.transform_records.return_value = (transformed_records, transformation_errors)
        self.mock_db_manager.bulk_insert_promo_orders.return_value = {'inserted': 0, 'failed': 1}

        self.importer.run()

        self.mock_extractor.extract_data.assert_called_once()
        self.mock_transformer.transform_records.assert_called_once_with(test_raw_records)
        self.mock_db_manager.bulk_insert_promo_orders.assert_called_once_with(transformed_records)
        
        # Проверяем логирование ошибок трансформации и сохранения
        mock_logging.warning.assert_any_call("Обнаружены ошибки при трансформации данных: %s", transformation_errors)
        mock_logging.info.assert_any_call("Процесс импорта промокодов завершен. Вставлено: 0, Ошибки: 1")


if __name__ == '__main__':
    unittest.main()
