"""
Модуль для импорта и обработки данных медиаплана.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os
import json

from .google_sheets_client import GoogleSheetsClient

logger = logging.getLogger(__name__)

class MediaPlanImporter:
    """
    Класс для импорта данных медиаплана из Google Sheets.
    """
    
    def __init__(self, spreadsheet_id: str = None, worksheet_name: str = 'MediaPlan', sheets_client=None):
        """
        Инициализирует импортер медиаплана.
        
        Args:
            spreadsheet_id: ID Google таблицы с медиапланом
            worksheet_name: Название листа с медиапланом
            sheets_client: Клиент для работы с Google Sheets (для тестов)
        """
        self.sheets_client = sheets_client if sheets_client is not None else GoogleSheetsClient()
        self.spreadsheet_id = spreadsheet_id or os.environ.get('MEDIA_PLAN_SPREADSHEET_ID')
        self.worksheet_name = worksheet_name
    
    def import_media_plan(self, month: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Импортирует данные медиаплана из Google Sheets.
        
        Args:
            month: Месяц в формате 'YYYY-MM' (если None, импортирует все месяцы)
            
        Returns:
            Список словарей с данными медиаплана
        """
        if not self.spreadsheet_id:
            logger.error("Не задан ID таблицы с медиапланом")
            return []
        
        try:
            # Получаем данные из Google Sheets
            records = self.sheets_client.get_all_records(self.worksheet_name, spreadsheet_id=self.spreadsheet_id)
            
            if not records:
                logger.warning(f"Нет данных в медиаплане: {self.worksheet_name}")
                return []
            
            # Преобразуем данные
            media_plan_data = self._transform_media_plan_data(records)
            
            # Фильтруем по месяцу, если указан
            if month:
                media_plan_data = [
                    item for item in media_plan_data 
                    if item.get('month', '').startswith(month)
                ]
            
            logger.info(f"Импортировано {len(media_plan_data)} записей медиаплана")
            return media_plan_data
            
        except Exception as e:
            logger.error(f"Ошибка при импорте медиаплана: {str(e)}")
            return []
    
    def _transform_media_plan_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Преобразует сырые данные из Google Sheets в структурированный формат.
        
        Args:
            records: Список записей из Google Sheets
            
        Returns:
            Структурированные данные медиаплана
        """
        transformed_data = []
        
        for idx, record in enumerate(records):
            try:
                # Проверяем обязательные поля
                required_fields = ['month', 'restaurant', 'country', 'campaign_type', 'goal', 'source']
                missing_fields = [field for field in required_fields if not record.get(field)]
                
                if missing_fields:
                    logger.warning(f"Запись {idx} не содержит обязательных полей: {', '.join(missing_fields)}")
                    continue
                
                # Преобразуем формат месяца
                try:
                    month_value = record['month']
                    # Если дата в формате MM/YYYY или DD/MM/YYYY, преобразуем её
                    if '/' in month_value:
                        parts = month_value.split('/')
                        if len(parts) == 2:  # MM/YYYY
                            month, year = parts
                            month_date = f"{year}-{month.zfill(2)}-01"
                        elif len(parts) == 3:  # DD/MM/YYYY
                            day, month, year = parts
                            month_date = f"{year}-{month.zfill(2)}-01"
                        else:
                            month_date = f"{datetime.now().year}-{datetime.now().month:02d}-01"
                    else:
                        # Пытаемся разобрать как YYYY-MM или YYYY-MM-DD
                        if len(month_value.split('-')) >= 2:
                            year_month = '-'.join(month_value.split('-')[:2])
                            month_date = f"{year_month}-01"
                        else:
                            month_date = f"{datetime.now().year}-{datetime.now().month:02d}-01"
                except Exception as e:
                    logger.warning(f"Ошибка при разборе даты для записи {idx}: {str(e)}")
                    month_date = f"{datetime.now().year}-{datetime.now().month:02d}-01"
                
                # Преобразуем числовые значения
                numeric_fields = ['planned_budget', 'planned_impressions', 'planned_clicks', 
                                 'planned_orders', 'planned_revenue']
                
                transformed_record = {
                    'id': idx + 1,  # Генерируем ID на основе индекса
                    'month': month_date,
                    'restaurant': record['restaurant'],
                    'country': record['country'],
                    'campaign_type': record['campaign_type'],
                    'goal': record['goal'],
                    'source': record['source']
                }
                
                # Добавляем числовые поля
                for field in numeric_fields:
                    value = record.get(field, None)
                    if value is not None and value != '':
                        try:
                            # Удаляем пробелы и заменяем запятые на точки
                            if isinstance(value, str):
                                value = value.replace(' ', '').replace(',', '.')
                            transformed_record[field] = float(value)
                        except (ValueError, TypeError):
                            logger.warning(f"Неверный формат числа для поля {field} в записи {idx}")
                            transformed_record[field] = None
                    else:
                        transformed_record[field] = None
                
                transformed_data.append(transformed_record)
            
            except Exception as e:
                logger.error(f"Ошибка при обработке записи {idx}: {str(e)}")
                continue
        
        return transformed_data
    
    def save_media_plan_to_file(self, media_plan_data: List[Dict[str, Any]], 
                               output_dir: str = 'data/media_plan') -> str:
        """
        Сохраняет данные медиаплана в JSON файл.
        
        Args:
            media_plan_data: Данные медиаплана
            output_dir: Директория для сохранения
            
        Returns:
            Путь к сохраненному файлу
        """
        if not media_plan_data:
            return ""
        
        try:
            # Создаем директорию, если её нет
            os.makedirs(output_dir, exist_ok=True)
            
            # Генерируем имя файла с текущей датой
            current_date = datetime.now().strftime('%Y-%m-%d')
            output_file = os.path.join(output_dir, f"media_plan_{current_date}.json")
            
            # Сохраняем данные в JSON файл
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(media_plan_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Медиаплан сохранен в {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении медиаплана: {str(e)}")
            return ""

def import_media_plan_task(execution_date=None, **kwargs):
    """
    Функция для импорта медиаплана в рамках Airflow DAG.
    
    Args:
        execution_date: Дата выполнения в формате 'YYYY-MM-DD'
        kwargs: Дополнительные параметры
        
    Returns:
        Путь к сохраненному файлу с медиапланом
    """
    if execution_date is None:
        execution_date = datetime.now().strftime('%Y-%m-%d')
    
    if isinstance(execution_date, str):
        # Если дата передана как строка, преобразуем её в объект datetime
        dt = datetime.strptime(execution_date, '%Y-%m-%d')
    else:
        dt = execution_date
    
    # Извлекаем месяц из даты выполнения
    month = dt.strftime('%Y-%m')
    
    logger.info(f"Импортируем медиаплан для месяца: {month}")
    
    # Создаем импортер и получаем данные
    importer = MediaPlanImporter()
    media_plan_data = importer.import_media_plan(month)
    
    if not media_plan_data:
        logger.warning("Не удалось импортировать данные медиаплана")
        return None
    
    # Сохраняем данные в файл
    output_file = importer.save_media_plan_to_file(media_plan_data)
    
    # Возвращаем путь к файлу для использования в следующих задачах
    return output_file
