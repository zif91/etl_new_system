#!/usr/bin/env python
"""
Интеграционное тестирование для импорта промокодов из Google Sheets в БД.
Тестирует весь процесс от аутентификации до сохранения данных в БД.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Добавляем корневую директорию проекта в sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.google_sheets_client import GoogleSheetsClient
from src.promo_importer import PromoCodeImporter
from src.promo_db_manager import PromoDBManager
from src.promo_validator import PromoCodeValidator, PromoCodeTransformer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/promo_import_test.log')
    ]
)

logger = logging.getLogger('promo_import_test')


def test_google_sheets_client(sheet_id=None):
    """Проверяет подключение к Google Sheets и получение данных"""
    try:
        client = GoogleSheetsClient(sheet_id=sheet_id)
        
        # Проверяем, что клиент создан и аутентификация прошла успешно
        assert client.client is not None, "Client initialization failed"
        
        # Открываем таблицу
        spreadsheet = client.open_spreadsheet()
        logger.info(f"Successfully opened spreadsheet: {spreadsheet.title}")
        
        # Получаем список листов
        worksheets = spreadsheet.worksheets()
        logger.info(f"Available worksheets: {', '.join([ws.title for ws in worksheets])}")
        
        # Пробуем получить данные с первого листа
        if worksheets:
            records = client.get_all_records(worksheets[0].title)
            logger.info(f"Retrieved {len(records)} records from {worksheets[0].title}")
            
            # Проверка структуры данных первой записи
            if records:
                logger.info(f"First record keys: {list(records[0].keys())}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing Google Sheets client: {e}")
        return False


def test_promo_validator():
    """Проверяет работу валидатора и трансформера промокодов"""
    validator = PromoCodeValidator()
    transformer = PromoCodeTransformer()
    
    # Тестовые данные
    test_data = [
        {
            'promo_code': 'TANUKI20',
            'order_id': 'ORD123456',
            'order_date': '2025-01-01',
            'order_amount': '1500',
            'restaurant': 'Тануки',
            'country': 'Казахстан',
            'promo_source': 'facebook_ads'
        },
        {
            'promo_code': 'INVALID-PROMO-!@#',  # Невалидный промокод
            'order_id': 'ORD789012',
            'order_date': '01/02/2025',  # Другой формат даты
            'order_amount': '2000.50',
            'restaurant': 'Белла',
            'country': 'Узбекистан',
            'promo_source': 'instagram'
        },
        {
            'promo_code': 'BELLA15',
            'order_id': 'ORD345678',
            'order_date': '2025-03-01',
            'order_amount': 'not-a-number',  # Невалидная сумма
            'restaurant': 'Неизвестный',  # Не из списка разрешенных
            'country': 'Казахстан',
            'promo_source': 'google_ads'
        }
    ]
    
    # Трансформируем данные
    transformed, errors = transformer.transform_records(test_data)
    
    # Проверка результатов
    logger.info(f"Transformed {len(transformed)} records out of {len(test_data)}")
    logger.info(f"Found {len(errors)} records with errors")
    
    for i, record in enumerate(test_data):
        if i in errors:
            logger.info(f"Record {i+1} has errors: {errors[i]}")
            logger.info(f"Record data: {record}")
    
    return transformed, errors


def test_database_operations(test_data=None):
    """Проверяет операции с базой данных для промокодов"""
    db_manager = PromoDBManager()
    
    if not test_data:
        # Генерируем тестовые данные, если не предоставлены
        test_data = []
        for i in range(5):
            test_data.append({
                'promo_code': f'TEST{i:02d}',
                'order_id': f'TEST-ORDER-{i:02d}',
                'transaction_id': f'TXN-TEST-{i:02d}-{int(datetime.now().timestamp())}',
                'order_date': datetime.now().date() - timedelta(days=i),
                'order_amount': 1000 + i * 100,
                'restaurant': 'Тануки',
                'country': 'Казахстан',
                'promo_source': 'integration_test'
            })
    
    # Вставляем данные
    insert_result = db_manager.bulk_insert_promo_orders(test_data)
    logger.info(f"Insert result: {insert_result}")
    
    # Получаем данные обратно для проверки
    filters = {'promo_source': 'integration_test'}
    retrieved = db_manager.get_promo_orders(filters)
    logger.info(f"Retrieved {len(retrieved)} records with filter {filters}")
    
    # Получаем статистику
    stats = db_manager.get_promo_stats(filters)
    logger.info(f"Statistics: {stats['total_orders']} orders, total amount: {stats['total_amount']}")
    
    # Если есть данные, попробуем удалить первую запись
    if retrieved:
        transaction_id = retrieved[0]['transaction_id']
        deleted = db_manager.delete_promo_orders([transaction_id])
        logger.info(f"Deleted {deleted} records with transaction_id {transaction_id}")
    
    return insert_result, retrieved, stats


def test_end_to_end_import(worksheet_name='Promo Codes'):
    """Тестирует полный процесс импорта от Google Sheets до БД"""
    try:
        # Инициализируем импортер
        importer = PromoCodeImporter()
        
        # Запускаем импорт
        import_result = importer.import_promo_codes(worksheet_name=worksheet_name, generate_report=True)
        
        logger.info(f"End-to-end import test completed. Result: {import_result}")
        return import_result
    except Exception as e:
        logger.error(f"Error during end-to-end import test: {e}")
        return {'error': str(e)}


def main():
    parser = argparse.ArgumentParser(description='Test promo code import integration')
    parser.add_argument('--sheet-id', help='Google Sheet ID (optional, otherwise uses PROMO_SHEET_ID from .env)')
    parser.add_argument('--worksheet', default='Promo Codes', help='Worksheet name (default: Promo Codes)')
    parser.add_argument('--test-type', choices=['sheets', 'validator', 'db', 'all', 'e2e'], 
                        default='all', help='Type of test to run (default: all)')
    
    args = parser.parse_args()
    
    logger.info("Starting promo import integration tests")
    
    # Создаем директорию для логов, если не существует
    os.makedirs('logs', exist_ok=True)
    
    if args.test_type in ('sheets', 'all'):
        logger.info("Testing Google Sheets client...")
        sheets_result = test_google_sheets_client(args.sheet_id)
        logger.info(f"Google Sheets client test {'passed' if sheets_result else 'failed'}")
    
    if args.test_type in ('validator', 'all'):
        logger.info("Testing promo code validator...")
        transformed, errors = test_promo_validator()
        logger.info(f"Validator test completed. Transformed {len(transformed)} records, {len(errors)} records with errors")
    
    if args.test_type in ('db', 'all'):
        logger.info("Testing database operations...")
        insert_result, retrieved, stats = test_database_operations()
        logger.info(f"Database test completed. Inserted: {insert_result['inserted']}, Updated: {insert_result['updated']}")
    
    if args.test_type in ('e2e', 'all'):
        logger.info("Testing end-to-end import...")
        e2e_result = test_end_to_end_import(args.worksheet)
        total_records = e2e_result.get('total_records', 0)
        processed = e2e_result.get('processed_records', 0)
        logger.info(f"End-to-end import test completed. Processed {processed} out of {total_records} records")
    
    logger.info("All tests completed")


if __name__ == "__main__":
    main()
