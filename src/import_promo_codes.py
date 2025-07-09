"""
Скрипт для импорта промокодов из Google Sheets.
"""

import os
import sys
import logging
from datetime import datetime

# Добавляем путь проекта в sys.path, чтобы корректно импортировать модули
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.promo_importer import PromoCodeImporter

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/promo_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

logger = logging.getLogger(__name__)

def import_promo_codes():
    """
    Основная функция для импорта промокодов из Google Sheets.
    """
    logger.info("Starting promo codes import process")
    
    try:
        # Создаем импортер промокодов
        importer = PromoCodeImporter()
        
        # Импортируем данные из листа (по умолчанию используется лист "Promo Codes")
        results = importer.import_promo_codes()
        
        logger.info(f"Promo codes import completed. Stats: {results}")
        
        # Возвращаем результаты для использования в Airflow или других местах
        return results
        
    except Exception as e:
        logger.error(f"Error during promo codes import: {e}")
        raise


if __name__ == "__main__":
    # Создаем директорию для логов, если она не существует
    os.makedirs("logs", exist_ok=True)
    
    # Запускаем импорт
    import_promo_codes()
