"""
Модуль для импорта данных из AppsFlyer API.
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from src.appsflyer_client import init_appsflyer_client, AppsFlyerClient
from src.appsflyer_transformer import (
    transform_appsflyer_installs,
    transform_appsflyer_events,
    transform_appsflyer_retention,
    transform_appsflyer_ltv,
    merge_appsflyer_data
)
from src.db import insert_appsflyer_metrics, get_connection

logger = logging.getLogger(__name__)


def import_appsflyer_data(
    start_date: str, 
    end_date: str, 
    include_retention: bool = True,
    include_ltv: bool = True,
    media_source: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Импортирует данные из AppsFlyer API и трансформирует их в стандартный формат.
    
    Args:
        start_date: Начальная дата в формате 'YYYY-MM-DD'
        end_date: Конечная дата в формате 'YYYY-MM-DD'
        include_retention: Включать ли данные об удержании пользователей
        include_ltv: Включать ли данные о LTV
        media_source: Опциональный фильтр по источнику трафика
        
    Returns:
        Список объединенных данных в стандартном формате
    """
    # Инициализируем клиент AppsFlyer
    client = init_appsflyer_client()
    
    # 1. Получаем данные об установках
    logger.info(f"Fetching AppsFlyer installs data from {start_date} to {end_date}")
    installs_raw = client.get_installs_report(start_date, end_date, media_source)
    installs_data = transform_appsflyer_installs(installs_raw)
    logger.info(f"Fetched {len(installs_data)} installs records")
    
    # 2. Получаем данные о событиях покупки
    logger.info(f"Fetching AppsFlyer in-app events data from {start_date} to {end_date}")
    events_raw = client.get_in_app_events_report(start_date, end_date, None, media_source)
    events_data = transform_appsflyer_events(events_raw)
    logger.info(f"Fetched {len(events_data)} in-app events records")
    
    # 3. Получаем данные об удержании, если требуется
    retention_data = None
    if include_retention:
        logger.info(f"Fetching AppsFlyer retention data from {start_date} to {end_date}")
        retention_raw = client.get_retention_report(start_date, end_date, media_source)
        retention_data = transform_appsflyer_retention(retention_raw)
        logger.info(f"Fetched {len(retention_data)} retention records")
    
    # 4. Получаем данные о LTV, если требуется
    ltv_data = None
    if include_ltv:
        logger.info(f"Fetching AppsFlyer LTV data from {start_date} to {end_date}")
        ltv_raw = client.get_ltv_report(start_date, end_date, 7, media_source)
        ltv_data = transform_appsflyer_ltv(ltv_raw)
        logger.info(f"Fetched {len(ltv_data)} LTV records")
    
    # 5. Объединяем все данные
    merged_data = merge_appsflyer_data(
        installs_data=installs_data,
        events_data=events_data,
        retention_data=retention_data,
        ltv_data=ltv_data
    )
    
    logger.info(f"Transformed and merged {len(merged_data)} AppsFlyer data records")
    
    return merged_data


def import_and_store_appsflyer_data(
    start_date: str, 
    end_date: str,
    include_retention: bool = True,
    include_ltv: bool = True,
    media_source: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Импортирует данные из AppsFlyer API и сохраняет их в базу данных.
    
    Args:
        start_date: Начальная дата в формате 'YYYY-MM-DD'
        end_date: Конечная дата в формате 'YYYY-MM-DD'
        include_retention: Включать ли данные об удержании пользователей
        include_ltv: Включать ли данные о LTV
        media_source: Опциональный фильтр по источнику трафика
        
    Returns:
        Список импортированных данных
    """
    try:
        # Импортируем данные из AppsFlyer
        data = import_appsflyer_data(
            start_date=start_date,
            end_date=end_date,
            include_retention=include_retention,
            include_ltv=include_ltv,
            media_source=media_source
        )
        
        if not data:
            logger.warning("No AppsFlyer data to store")
            return []
        
        # Сохраняем данные в базу
        insert_count = insert_appsflyer_metrics(data)
        
        logger.info(f"Successfully stored {insert_count} AppsFlyer data records to database")
        
        return data
        
    except Exception as e:
        logger.error(f"Error importing AppsFlyer data: {e}")
        raise


def main():
    """
    Основная функция для запуска импорта данных из AppsFlyer.
    """
    import argparse
    from datetime import datetime, timedelta
    
    parser = argparse.ArgumentParser(description="Import data from AppsFlyer API")
    parser.add_argument("--start-date", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", help="End date in YYYY-MM-DD format")
    parser.add_argument("--days", type=int, default=7, help="Number of days to import (default: 7)")
    parser.add_argument("--media-source", help="Filter by media source")
    parser.add_argument("--skip-retention", action="store_true", help="Skip retention data")
    parser.add_argument("--skip-ltv", action="store_true", help="Skip LTV data")
    
    args = parser.parse_args()
    
    # Определяем даты
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = datetime.now().date() - timedelta(days=1)  # По умолчанию до вчера
        
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = end_date - timedelta(days=args.days - 1)
        
    # Преобразуем даты в строки
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    logger.info(f"Starting AppsFlyer data import for period {start_date_str} to {end_date_str}")
    
    # Импортируем и сохраняем данные
    try:
        data = import_and_store_appsflyer_data(
            start_date=start_date_str,
            end_date=end_date_str,
            include_retention=not args.skip_retention,
            include_ltv=not args.skip_ltv,
            media_source=args.media_source
        )
        
        logger.info(f"Successfully imported {len(data)} AppsFlyer data records")
        
    except Exception as e:
        logger.error(f"Failed to import AppsFlyer data: {e}")
        exit(1)
        
    logger.info("AppsFlyer data import completed successfully")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    main()
