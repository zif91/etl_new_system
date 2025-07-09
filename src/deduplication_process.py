"""
Пример использования системы дедупликации заказов в ETL-процессе.
"""

import os
import logging
import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from src.deduplication import OrderDeduplicator, DeduplicationStrategy
from src.attribution import AttributionSourceAssigner, AttributionRules
from src.ga4_client import GA4Client
from src.ga4_transformer import transform_ga4_report
from src.promo_db_manager import PromoDBManager
from src.db import get_connection, insert_deduplicated_metrics

# Импортируем DeduplicationLogger для расширенного логирования
try:
    from src.deduplication_logger import DeduplicationLogger
    DEDUPLICATION_LOGGER_AVAILABLE = True
except ImportError:
    DEDUPLICATION_LOGGER_AVAILABLE = False

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/deduplication_process.log')
    ]
)

logger = logging.getLogger('deduplication_process')


def get_ga4_transactions(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """
    Получает данные о транзакциях из Google Analytics 4.
    
    Args:
        start_date: Начальная дата для выборки
        end_date: Конечная дата для выборки
        
    Returns:
        Список транзакций из GA4
    """
    try:
        # Инициализируем клиент GA4
        ga4_client = GA4Client()
        
        # Создаем запрос для получения данных о транзакциях
        request = {
            'dateRanges': [{'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')}],
            'dimensions': [
                {'name': 'date'},
                {'name': 'transactionId'},
                {'name': 'sourceMedium'},
                {'name': 'campaign'},
                {'name': 'sessionSource'},
                {'name': 'sessionMedium'}
            ],
            'metrics': [
                {'name': 'transactions'},
                {'name': 'purchaseRevenue'},
                {'name': 'itemsPerPurchase'}
            ]
        }
        
        # Выполняем запрос к API GA4
        report = ga4_client.run_report(request)
        
        # Трансформируем результат в удобный формат
        transactions = transform_ga4_report(report)
        
        logger.info(f"Retrieved {len(transactions)} transactions from GA4")
        return transactions
    
    except Exception as e:
        logger.error(f"Error retrieving GA4 transactions: {e}")
        return []


def get_promo_transactions(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """
    Получает данные о заказах с промокодами из базы данных.
    
    Args:
        start_date: Начальная дата для выборки
        end_date: Конечная дата для выборки
        
    Returns:
        Список заказов с промокодами
    """
    try:
        # Инициализируем менеджер базы данных для промокодов
        db_manager = PromoDBManager()
        
        # Создаем фильтры для запроса
        filters = {
            'date_from': start_date.date(),
            'date_to': end_date.date()
        }
        
        # Получаем заказы с промокодами
        promo_orders = db_manager.get_promo_orders(filters)
        
        logger.info(f"Retrieved {len(promo_orders)} promo orders from database")
        return promo_orders
    
    except Exception as e:
        logger.error(f"Error retrieving promo orders: {e}")
        return []


def load_deduplication_config() -> Dict[str, Any]:
    """
    Загружает конфигурацию для дедупликации из файла или переменных окружения.
    
    Returns:
        Словарь с конфигурацией
    """
    config = {
        'fuzzy_matching_threshold': 0.9,
        'time_window_hours': 24,
        'conflict_strategy': DeduplicationStrategy.SOURCE_PRIORITY,
        'use_transactional_attrs': True,
        'additional_match_criteria': ['purchase_revenue', 'order_amount'],
        'enhanced_logging': True  # Включаем расширенное логирование по умолчанию
    }
    
    # Загружаем конфигурацию из файла, если он существует
    config_path = os.environ.get('DEDUPLICATION_CONFIG_PATH', 'config/deduplication.json')
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)
    except Exception as e:
        logger.warning(f"Error loading config from {config_path}: {e}")
    
    # Переопределяем из переменных окружения, если они есть
    if 'DEDUP_FUZZY_THRESHOLD' in os.environ:
        try:
            config['fuzzy_matching_threshold'] = float(os.environ['DEDUP_FUZZY_THRESHOLD'])
        except ValueError:
            pass
            
    if 'DEDUP_TIME_WINDOW' in os.environ:
        try:
            config['time_window_hours'] = int(os.environ['DEDUP_TIME_WINDOW'])
        except ValueError:
            pass
            
    if 'DEDUP_CONFLICT_STRATEGY' in os.environ:
        strategy = os.environ['DEDUP_CONFLICT_STRATEGY']
        if strategy in DeduplicationStrategy.get_available_strategies():
            config['conflict_strategy'] = strategy
    
    logger.info(f"Loaded deduplication config: {config}")
    return config


def save_deduplicated_data(deduplicated_data: List[Dict[str, Any]]) -> bool:
    """
    Сохраняет дедуплицированные данные в базу данных.
    
    Args:
        deduplicated_data: Список дедуплицированных транзакций
        
    Returns:
        True, если данные успешно сохранены, иначе False
    """
    try:
        # Сохраняем дедуплицированные данные в базу
        insert_deduplicated_metrics(deduplicated_data)
        
        logger.info(f"Saved {len(deduplicated_data)} deduplicated records to database")
        return True
    
    except Exception as e:
        logger.error(f"Error saving deduplicated data: {e}")
        return False


def save_deduplication_stats(stats: Dict[str, Any], run_id: str) -> bool:
    """
    Сохраняет статистику дедупликации в базу данных.
    
    Args:
        stats: Словарь со статистикой дедупликации
        run_id: Идентификатор запуска ETL-процесса
        
    Returns:
        True, если статистика успешно сохранена, иначе False
    """
    try:
        # Преобразуем статистику в JSON
        stats_json = json.dumps(stats)
        
        # Добавляем идентификатор запуска и временную метку
        stats_record = {
            'run_id': run_id,
            'timestamp': datetime.now().isoformat(),
            'stats': stats_json
        }
        
        # Получаем соединение с базой данных
        conn = get_connection()
        cursor = conn.cursor()
        
        # Вставляем запись в таблицу статистики
        cursor.execute(
            """
            INSERT INTO etl_stats (run_id, process_name, timestamp, stats_data)
            VALUES (%s, %s, %s, %s)
            """,
            (run_id, 'order_deduplication', datetime.now(), stats_json)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Saved deduplication stats for run {run_id}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving deduplication stats: {e}")
        return False


def generate_deduplication_reports(stats: Dict[str, Any], run_id: str) -> Dict[str, str]:
    """
    Генерирует и сохраняет отчеты о дедупликации.
    
    Args:
        stats: Статистика дедупликации
        run_id: Идентификатор запуска
        
    Returns:
        Словарь с путями к созданным файлам отчетов
    """
    reports = {}
    
    # Проверяем наличие модуля matplotlib для визуализации
    try:
        import matplotlib.pyplot as plt
        
        # Создаем директорию для отчетов, если её нет
        reports_dir = f"logs/reports/{run_id}"
        os.makedirs(reports_dir, exist_ok=True)
        
        # 1. График распределения типов совпадений
        plt.figure(figsize=(10, 6))
        match_types = ['exact_matches', 'fuzzy_matches', 'unmatched']
        match_counts = [stats.get(t, 0) for t in match_types]
        plt.bar(match_types, match_counts, color=['green', 'orange', 'red'])
        plt.title('Distribution of Match Types')
        plt.ylabel('Count')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        chart_path = f"{reports_dir}/match_types.png"
        plt.savefig(chart_path)
        plt.close()
        reports['match_types_chart'] = chart_path
        
        # 2. График источников атрибуции
        if 'attribution_sources' in stats:
            plt.figure(figsize=(10, 6))
            sources = list(stats['attribution_sources'].keys())
            counts = list(stats['attribution_sources'].values())
            plt.bar(sources, counts, color=['blue', 'purple'])
            plt.title('Attribution Sources')
            plt.ylabel('Count')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            chart_path = f"{reports_dir}/attribution_sources.png"
            plt.savefig(chart_path)
            plt.close()
            reports['attribution_sources_chart'] = chart_path
        
        # 3. График критериев сопоставления
        if 'match_by_criteria' in stats:
            plt.figure(figsize=(12, 6))
            criteria = list(stats['match_by_criteria'].keys())
            counts = list(stats['match_by_criteria'].values())
            plt.bar(criteria, counts, color='teal')
            plt.title('Match by Criteria')
            plt.ylabel('Count')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            chart_path = f"{reports_dir}/match_criteria.png"
            plt.savefig(chart_path)
            plt.close()
            reports['match_criteria_chart'] = chart_path
        
        logger.info(f"Generated deduplication reports in {reports_dir}")
    
    except ImportError:
        logger.warning("Matplotlib not available, skipping report visualization")
    
    # Сохраняем статистику в JSON-файл
    stats_path = f"logs/reports/{run_id}/deduplication_stats.json"
    os.makedirs(os.path.dirname(stats_path), exist_ok=True)
    
    try:
        with open(stats_path, 'w') as f:
            json.dump(stats, f, indent=2)
        reports['stats_json'] = stats_path
    except Exception as e:
        logger.error(f"Error saving stats to JSON: {e}")
    
    return reports


def run_deduplication_process(start_date: Optional[datetime] = None, 
                              end_date: Optional[datetime] = None,
                              run_id: Optional[str] = None,
                              config_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Запускает процесс дедупликации заказов.
    
    Args:
        start_date: Начальная дата для выборки
        end_date: Конечная дата для выборки
        run_id: Идентификатор запуска ETL-процесса
        config_override: Переопределение конфигурации
        
    Returns:
        Словарь с результатами дедупликации
    """
    # Если даты не указаны, используем последние 7 дней
    if not start_date:
        start_date = datetime.now() - timedelta(days=7)
    if not end_date:
        end_date = datetime.now()
    
    # Если run_id не указан, генерируем его
    if not run_id:
        run_id = f"dedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    result = {
        'run_id': run_id,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'status': 'success',
        'error': None,
        'stats': None,
        'config_used': None
    }
    
    try:
        logger.info(f"Starting deduplication process for period {start_date.date()} - {end_date.date()}")
        
        # Загружаем базовую конфигурацию
        config = load_deduplication_config()
        
        # Применяем переопределения конфигурации, если они есть
        if config_override:
            config.update(config_override)
            
        # Сохраняем использованную конфигурацию в результате
        result['config_used'] = config
        
        # Получаем данные из GA4
        ga4_data = get_ga4_transactions(start_date, end_date)
        
        # Получаем данные о промокодах
        promo_data = get_promo_transactions(start_date, end_date)
        
        # Если нет данных, завершаем процесс
        if not ga4_data:
            logger.warning("No GA4 data available for deduplication")
            result['status'] = 'warning'
            result['error'] = 'No GA4 data available'
            return result
        
        # Создаем настроенный экземпляр ассайнера атрибуции
        attribution_rules = AttributionRules()
        attribution_assigner = AttributionSourceAssigner(rules=attribution_rules)
        
        # Создаем экземпляр логгера дедупликации, если включено расширенное логирование
        deduplication_logger = None
        if config.get('enhanced_logging', True) and DEDUPLICATION_LOGGER_AVAILABLE:
            # Создаем директории для логов, если их нет
            os.makedirs('logs/deduplication', exist_ok=True)
            os.makedirs('logs/stats', exist_ok=True)
            
            deduplication_logger = DeduplicationLogger(
                log_level="INFO",
                log_dir="logs/deduplication",
                stats_dir="logs/stats",
                enable_console=True,
                enable_file_logging=True
            )
            logger.info("Включено расширенное логирование дедупликации")
        
        # Создаем экземпляр дедупликатора с настроенной атрибуцией и конфигурацией
        deduplicator = OrderDeduplicator(
            fuzzy_matching_threshold=config['fuzzy_matching_threshold'],
            time_window_hours=config['time_window_hours'],
            attribution_assigner=attribution_assigner,
            conflict_strategy=config['conflict_strategy'],
            use_transactional_attrs=config['use_transactional_attrs'],
            additional_match_criteria=config['additional_match_criteria'],
            enhanced_logging=config.get('enhanced_logging', True),
            logger_instance=deduplication_logger
        )
        
        # Выполняем дедупликацию
        deduplicated_data = deduplicator.deduplicate_orders(ga4_data, promo_data)
        
        # Получаем статистику дедупликации
        deduplication_stats = deduplicator.get_stats()
        
        # Добавляем статистику по атрибуции
        attribution_stats = attribution_assigner.get_stats()
        deduplication_stats['attribution'] = attribution_stats
        
        result['stats'] = deduplication_stats
        
        # Сохраняем дедуплицированные данные
        if deduplicated_data:
            save_deduplicated_data(deduplicated_data)
        
        # Сохраняем статистику дедупликации
        save_deduplication_stats(deduplication_stats, run_id)
        
        # Генерируем отчеты о дедупликации
        if config.get('enhanced_logging', True):
            reports = generate_deduplication_reports(deduplication_stats, run_id)
            result['reports'] = reports
        
        logger.info(f"Deduplication process completed successfully. Stats: {deduplication_stats}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error during deduplication process: {e}")
        result['status'] = 'error'
        result['error'] = str(e)
        return result


def main():
    """
    Основная функция для запуска процесса дедупликации из командной строки.
    """
    parser = argparse.ArgumentParser(description='Run order deduplication process')
    parser.add_argument('--start-date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--run-id', help='Run ID for ETL process')
    parser.add_argument('--config', help='Path to configuration file (JSON)')
    parser.add_argument('--threshold', type=float, help='Fuzzy matching threshold (0.0-1.0)')
    parser.add_argument('--time-window', type=int, help='Time window in hours')
    parser.add_argument('--strategy', choices=DeduplicationStrategy.get_available_strategies(),
                       help='Conflict resolution strategy')
    
    args = parser.parse_args()
    
    # Обрабатываем аргументы
    start_date = None
    end_date = None
    config_override = {}
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    # Загружаем конфигурацию из файла, если указан
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config_override = json.load(f)
        except Exception as e:
            logger.error(f"Error loading config from {args.config}: {e}")
    
    # Применяем параметры командной строки, если они указаны
    if args.threshold is not None:
        config_override['fuzzy_matching_threshold'] = args.threshold
        
    if args.time_window is not None:
        config_override['time_window_hours'] = args.time_window
        
    if args.strategy:
        config_override['conflict_strategy'] = args.strategy
    
    # Запускаем процесс дедупликации
    result = run_deduplication_process(
        start_date=start_date, 
        end_date=end_date, 
        run_id=args.run_id,
        config_override=config_override
    )
    
    # Выводим результат
    if result['status'] == 'success':
        logger.info("Deduplication process completed successfully")
        print(f"Successfully processed {result['stats']['total_ga4_transactions']} transactions")
        print(f"Found {result['stats']['exact_matches'] + result['stats']['fuzzy_matches']} duplicates")
        print(f"Conflicts resolved: {result['stats']['conflicts_resolved']}")
        print(f"Match rate: {result['stats']['match_rate']:.2f}")
    else:
        logger.error(f"Deduplication process failed: {result['error']}")
        print(f"Error: {result['error']}")
