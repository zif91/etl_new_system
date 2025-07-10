"""
Импортер промокодов из Google Sheets.
"""

import os
import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

from src.google_sheets_client import GoogleSheetsClient
from src.db import insert_promo_order
from src.promo_validator import PromoCodeTransformer
from src.promo_db_manager import PromoDBManager

logger = logging.getLogger(__name__)

class PromoCodeImporter:
    """
    Класс для импорта данных о промокодах из Google Sheets
    """
    
    def __init__(self, credentials_path: Optional[str] = None, sheet_id: Optional[str] = None):
        """
        Инициализирует импортер промокодов.
        
        Args:
            credentials_path: Путь к JSON-файлу с учетными данными сервисного аккаунта.
                              Если None, используется GOOGLE_SHEETS_CREDENTIALS_JSON из .env
            sheet_id: ID Google-таблицы. Если None, используется PROMO_SHEET_ID из .env
        """
        self.sheets_client = GoogleSheetsClient(credentials_path, sheet_id)
    
    def transform_promo_data(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Трансформирует данные о промокодах из сырого формата Google Sheets
        в формат, подходящий для сохранения в БД.
        
        Args:
            raw_records: Список словарей с данными из Google Sheets
            
        Returns:
            Список словарей с преобразованными данными
        """
        # Используем PromoCodeTransformer для валидации и трансформации данных
        transformer = PromoCodeTransformer()
        transformed_records, errors_by_index = transformer.transform_records(raw_records)
        
        # Логируем ошибки
        for index, errors in errors_by_index.items():
            logger.warning(f"Record at index {index} has errors: {', '.join(errors)}")
            logger.warning(f"Record data: {raw_records[index]}")
        
        logger.info(f"Transformed {len(transformed_records)} records out of {len(raw_records)} total records")
        logger.info(f"Found {len(errors_by_index)} records with errors")
        
        return transformed_records
    
    def import_promo_codes(self, worksheet_name: str = 'Promo Codes', 
                        generate_report: bool = True, link_to_campaign: Optional[str] = None) -> Dict[str, Any]:
        """
        Импортирует данные о промокодах из указанного листа Google Sheets,
        трансформирует их и сохраняет в БД.
        
        Args:
            worksheet_name: Название листа с данными о промокодах
            generate_report: Генерировать ли подробный отчёт по импорту
            link_to_campaign: ID кампании для связывания с промокодами (опционально)
            
        Returns:
            Словарь с результатами импорта и отчетом
        """
        logger.info(f"Starting import of promo codes from worksheet: {worksheet_name}")
        import_start_time = time.time()
        
        stats = {
            'total_records': 0,
            'processed_records': 0,
            'skipped_records': 0,
            'failed_records': 0,
            'import_date': datetime.now().isoformat(),
            'sheet_name': worksheet_name
        }
        
        try:
            # Получаем все записи с листа
            raw_records = self.sheets_client.get_all_records(worksheet_name)
            stats['total_records'] = len(raw_records)
            
            if not raw_records:
                logger.warning(f"No records found in worksheet: {worksheet_name}")
                return stats
            
            # Трансформируем данные с валидацией
            transformed_records, errors_by_index = self.transform_promo_data(raw_records)
            stats['skipped_records'] = stats['total_records'] - len(transformed_records)
            stats['validation_errors'] = len(errors_by_index)
            
            # Определяем диапазон дат для отчета
            min_date = None
            max_date = None
            if transformed_records:
                try:
                    dates = [record['order_date'] for record in transformed_records if record.get('order_date')]
                    if dates:
                        min_date = min(dates)
                        max_date = max(dates)
                        stats['date_range'] = {
                            'start': min_date.isoformat() if min_date else None,
                            'end': max_date.isoformat() if max_date else None
                        }
                except Exception as e:
                    logger.error(f"Error calculating date range: {e}")
            
            # Если указан ID кампании, добавляем его к каждой записи
            if link_to_campaign:
                for record in transformed_records:
                    record['campaign_id'] = link_to_campaign
            
            # Сохраняем в БД, используя массовую вставку с улучшенным отчетом
            db_manager = PromoDBManager()
            try:
                db_results = db_manager.bulk_insert_promo_orders(transformed_records)
                
                stats['processed_records'] = db_results['inserted'] + db_results['updated']
                stats['inserted_records'] = db_results['inserted']
                stats['updated_records'] = db_results['updated']
                stats['failed_records'] = db_results['failed']
                stats['processing_time'] = db_results['processing_time']
                
                # Добавляем подробную статистику об ошибках
                if db_results.get('failed_records'):
                    stats['failed_details'] = [{
                        'promo_code': record.get('promo_code'),
                        'order_id': record.get('order_id'),
                        'transaction_id': record.get('transaction_id'),
                        'error': record.get('error')
                    } for record in db_results['failed_records']]
                
                logger.info(f"Database operation stats: {db_results}")
                
                # Генерируем подробный отчет по импорту
                if generate_report:
                    import_report = db_manager.generate_import_report(db_results, min_date, max_date)
                    stats['report'] = import_report
                    
                    # Логируем основные метрики из отчета
                    summary = import_report.get('summary', {})
                    logger.info(f"Import summary: {summary.get('total_orders')} orders, "
                                f"total amount: {summary.get('total_amount')}, "
                                f"avg order: {summary.get('avg_order_amount')}")
                
            except Exception as e:
                logger.error(f"Error saving promo orders to database: {e}")
                stats['failed_records'] = len(transformed_records)
                stats['error'] = str(e)
            
            total_time = time.time() - import_start_time
            stats['total_time'] = total_time
            logger.info(f"Promo codes import completed in {total_time:.2f}s. Stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error importing promo codes: {e}")
            raise

def import_promo_codes(worksheet_name: str = 'Promo Codes') -> Dict[str, Any]:
    """
    Global function to import promo codes for use in Airflow DAG.
    
    Args:
        worksheet_name: Name of the worksheet to import from
        
    Returns:
        Dictionary with import statistics
    """
    importer = PromoCodeImporter()
    return importer.import_promo_codes(worksheet_name)
