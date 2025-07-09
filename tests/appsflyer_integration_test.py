"""
Интеграционный тест AppsFlyer API.

Этот скрипт выполняет интеграционное тестирование всей цепочки AppsFlyer API:
1. Аутентификация в API
2. Получение данных
3. Трансформация данных
4. Сохранение в базу данных

Для запуска требуются настроенные переменные окружения:
- APPSFLYER_API_TOKEN
- APPSFLYER_APP_ID
- DATABASE_URL
"""

import os
import logging
import argparse
from datetime import datetime, timedelta

from src.appsflyer_importer import import_and_store_appsflyer_data

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("appsflyer_integration_test")


def run_integration_test(days_ago=7, include_retention=True, include_ltv=True, media_source=None):
    """
    Запускает полную интеграционную проверку AppsFlyer ETL цепочки.
    
    Args:
        days_ago: Количество дней назад для импорта (по умолчанию 7)
        include_retention: Включать ли данные об удержании
        include_ltv: Включать ли данные о LTV
        media_source: Опциональный фильтр по источнику
    """
    # Проверяем наличие переменных окружения
    required_env_vars = ["APPSFLYER_API_TOKEN", "APPSFLYER_APP_ID", "DATABASE_URL"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables before running the test")
        return False
        
    # Определяем даты для тестового импорта
    end_date = datetime.now().date() - timedelta(days=1)  # До вчера
    start_date = end_date - timedelta(days=days_ago - 1)  # За указанное количество дней
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    logger.info(f"Starting AppsFlyer integration test for period {start_date_str} to {end_date_str}")
    logger.info(f"Include retention: {include_retention}, Include LTV: {include_ltv}")
    
    if media_source:
        logger.info(f"Filtering by media source: {media_source}")
    
    try:
        # Запускаем полный цикл импорта
        data = import_and_store_appsflyer_data(
            start_date=start_date_str,
            end_date=end_date_str,
            include_retention=include_retention,
            include_ltv=include_ltv,
            media_source=media_source
        )
        
        # Проверяем результаты
        if data:
            logger.info(f"Successfully imported and stored {len(data)} AppsFlyer records")
            
            # Выводим статистику
            date_counts = {}
            campaign_counts = {}
            total_installs = 0
            total_purchases = 0
            total_revenue = 0
            
            for item in data:
                # Статистика по датам
                date = item.get('date')
                if date in date_counts:
                    date_counts[date] += 1
                else:
                    date_counts[date] = 1
                
                # Статистика по кампаниям
                campaign = item.get('campaign')
                if campaign in campaign_counts:
                    campaign_counts[campaign] += 1
                else:
                    campaign_counts[campaign] = 1
                
                # Суммарная статистика
                total_installs += item.get('installs', 0)
                total_purchases += item.get('purchases', 0)
                total_revenue += item.get('revenue', 0)
            
            logger.info(f"Date distribution: {date_counts}")
            logger.info(f"Campaign distribution: {campaign_counts}")
            logger.info(f"Total metrics - Installs: {total_installs}, Purchases: {total_purchases}, Revenue: {total_revenue:.2f}")
            
            return True
        else:
            logger.warning("No data was imported from AppsFlyer")
            return False
            
    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run AppsFlyer API integration test")
    parser.add_argument("--days", type=int, default=7, help="Number of days to import (default: 7)")
    parser.add_argument("--skip-retention", action="store_true", help="Skip retention data")
    parser.add_argument("--skip-ltv", action="store_true", help="Skip LTV data")
    parser.add_argument("--media-source", help="Filter by media source")
    
    args = parser.parse_args()
    
    success = run_integration_test(
        days_ago=args.days,
        include_retention=not args.skip_retention,
        include_ltv=not args.skip_ltv,
        media_source=args.media_source
    )
    
    if success:
        logger.info("AppsFlyer integration test completed successfully")
        exit(0)
    else:
        logger.error("AppsFlyer integration test failed")
        exit(1)
