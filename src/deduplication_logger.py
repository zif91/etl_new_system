"""
Система логирования и отслеживания статистики для процесса дедупликации.
"""

import logging
import json
import os
import csv
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Tuple, Set
from pathlib import Path

# Необязательный импорт для визуализации
# Если matplotlib не установлен, визуализация будет отключена
try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

class DeduplicationLogger:
    """
    Класс для логирования и отслеживания статистики процесса дедупликации.
    Обеспечивает детальное логирование решений, сохранение статистики
    и формирование отчетов.
    """
    
    def __init__(self, log_level: str = "INFO", 
                 log_dir: str = "logs/deduplication",
                 stats_dir: str = "logs/stats",
                 enable_console: bool = True,
                 enable_file_logging: bool = True,
                 enable_db_logging: bool = False):
        """
        Инициализирует логгер для дедупликации.
        
        Args:
            log_level: Уровень логирования (INFO, DEBUG, WARNING, ERROR)
            log_dir: Директория для хранения логов
            stats_dir: Директория для хранения статистики
            enable_console: Включить вывод логов в консоль
            enable_file_logging: Включить запись логов в файл
            enable_db_logging: Включить запись логов в БД (требует настройки подключения)
        """
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.log_dir = log_dir
        self.stats_dir = stats_dir
        self.enable_console = enable_console
        self.enable_file_logging = enable_file_logging
        self.enable_db_logging = enable_db_logging
        
        # Создаем директории для логов и статистики, если их нет
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(stats_dir, exist_ok=True)
        
        # Генерируем имена файлов с текущей датой
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.log_filename = f"{log_dir}/deduplication_{current_date}.log"
        self.stats_filename = f"{stats_dir}/deduplication_stats_{current_date}.json"
        self.csv_stats_filename = f"{stats_dir}/deduplication_stats_{current_date}.csv"
        
        # Настраиваем логгер
        self.logger = logging.getLogger("deduplication")
        self.logger.setLevel(self.log_level)
        self.logger.handlers = []  # Очищаем обработчики, чтобы избежать дублирования
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        if enable_file_logging:
            file_handler = logging.FileHandler(self.log_filename)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # История операций для отслеживания
        self.operations_history = []
        
        # Сохраняем успешные и неудачные сопоставления для анализа
        self.successful_matches = []
        self.failed_matches = []
        
        # Храним статистику из разных запусков
        self.historical_stats = []
    
    def log_start(self, ga4_transactions: int, promo_transactions: int, 
                  config: Dict[str, Any] = None) -> None:
        """
        Логирует начало процесса дедупликации.
        
        Args:
            ga4_transactions: Количество транзакций из GA4
            promo_transactions: Количество транзакций промокодов
            config: Конфигурация дедупликатора
        """
        self.start_time = datetime.now()
        config_str = json.dumps(config) if config else "default"
        
        self.logger.info(f"=== Начало процесса дедупликации ===")
        self.logger.info(f"Время запуска: {self.start_time}")
        self.logger.info(f"Входные данные: {ga4_transactions} транзакций GA4, "
                         f"{promo_transactions} промокодов")
        self.logger.info(f"Конфигурация: {config_str}")
        
        self.operations_history.append({
            "operation": "start",
            "timestamp": self.start_time,
            "ga4_transactions": ga4_transactions,
            "promo_transactions": promo_transactions,
            "config": config
        })
    
    def log_exact_match(self, transaction_id: str, ga4_data: Dict[str, Any],
                        promo_data: Dict[str, Any]) -> None:
        """
        Логирует точное совпадение транзакций.
        
        Args:
            transaction_id: ID транзакции
            ga4_data: Данные транзакции из GA4
            promo_data: Данные промокода
        """
        self.logger.debug(f"Точное совпадение для {transaction_id}: "
                         f"GA4 (сумма: {ga4_data.get('purchase_revenue', 'N/A')}) = "
                         f"Промо (код: {promo_data.get('promo_code', 'N/A')}, "
                         f"сумма: {promo_data.get('order_amount', 'N/A')})")
        
        self.successful_matches.append({
            "type": "exact",
            "transaction_id": transaction_id,
            "ga4_data": {k: v for k, v in ga4_data.items() if k in ['purchase_revenue', 'date', 'utm_source']},
            "promo_data": {k: v for k, v in promo_data.items() if k in ['promo_code', 'order_amount', 'promo_source']},
            "timestamp": datetime.now()
        })
    
    def log_fuzzy_match(self, ga4_transaction_id: str, promo_transaction_id: str,
                         confidence: float, ga4_data: Dict[str, Any],
                         promo_data: Dict[str, Any]) -> None:
        """
        Логирует нечеткое совпадение транзакций.
        
        Args:
            ga4_transaction_id: ID транзакции из GA4
            promo_transaction_id: ID транзакции из промо
            confidence: Степень уверенности в совпадении
            ga4_data: Данные транзакции из GA4
            promo_data: Данные промокода
        """
        self.logger.debug(f"Нечеткое совпадение {ga4_transaction_id} ≈ {promo_transaction_id} "
                         f"(уверенность: {confidence:.2f}): "
                         f"GA4 (сумма: {ga4_data.get('purchase_revenue', 'N/A')}) = "
                         f"Промо (код: {promo_data.get('promo_code', 'N/A')}, "
                         f"сумма: {promo_data.get('order_amount', 'N/A')})")
        
        self.successful_matches.append({
            "type": "fuzzy",
            "ga4_transaction_id": ga4_transaction_id,
            "promo_transaction_id": promo_transaction_id,
            "confidence": confidence,
            "ga4_data": {k: v for k, v in ga4_data.items() if k in ['purchase_revenue', 'date', 'utm_source']},
            "promo_data": {k: v for k, v in promo_data.items() if k in ['promo_code', 'order_amount', 'promo_source']},
            "timestamp": datetime.now()
        })
    
    def log_criteria_match(self, ga4_transaction_id: str, promo_transaction_id: str,
                           criteria: str, ga4_data: Dict[str, Any],
                           promo_data: Dict[str, Any]) -> None:
        """
        Логирует совпадение по дополнительным критериям.
        
        Args:
            ga4_transaction_id: ID транзакции из GA4
            promo_transaction_id: ID транзакции из промо
            criteria: Критерий, по которому произошло совпадение
            ga4_data: Данные транзакции из GA4
            promo_data: Данные промокода
        """
        self.logger.debug(f"Совпадение по критерию '{criteria}': {ga4_transaction_id} -> {promo_transaction_id}")
        
        self.successful_matches.append({
            "type": "criteria",
            "criteria": criteria,
            "ga4_transaction_id": ga4_transaction_id,
            "promo_transaction_id": promo_transaction_id,
            "ga4_data": {k: v for k, v in ga4_data.items() if k in ['purchase_revenue', 'date', 'utm_source']},
            "promo_data": {k: v for k, v in promo_data.items() if k in ['promo_code', 'order_amount', 'promo_source']},
            "timestamp": datetime.now()
        })
    
    def log_no_match(self, transaction_id: str, ga4_data: Dict[str, Any]) -> None:
        """
        Логирует отсутствие совпадения для транзакции.
        
        Args:
            transaction_id: ID транзакции
            ga4_data: Данные транзакции из GA4
        """
        self.logger.debug(f"Нет совпадения для {transaction_id}")
        
        self.failed_matches.append({
            "transaction_id": transaction_id,
            "ga4_data": {k: v for k, v in ga4_data.items() if k in ['purchase_revenue', 'date', 'utm_source']},
            "timestamp": datetime.now()
        })
    
    def log_conflict_resolution(self, transaction_id: str, matches: List[Dict[str, Any]],
                                selected_match: Dict[str, Any], strategy: str) -> None:
        """
        Логирует разрешение конфликта при множественных совпадениях.
        
        Args:
            transaction_id: ID транзакции
            matches: Список потенциальных совпадений
            selected_match: Выбранное совпадение
            strategy: Стратегия разрешения конфликта
        """
        self.logger.info(f"Разрешен конфликт для {transaction_id} "
                        f"(стратегия: {strategy}): выбран {selected_match.get('promo_code', 'N/A')}")
        
        self.operations_history.append({
            "operation": "conflict_resolution",
            "timestamp": datetime.now(),
            "transaction_id": transaction_id,
            "strategy": strategy,
            "num_matches": len(matches),
            "selected_match": selected_match.get('promo_code', 'unknown')
        })
    
    def log_end(self, stats: Dict[str, Any]) -> None:
        """
        Логирует завершение процесса дедупликации.
        
        Args:
            stats: Статистика дедупликации
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        self.logger.info(f"=== Завершение процесса дедупликации ===")
        self.logger.info(f"Время завершения: {end_time}")
        self.logger.info(f"Продолжительность: {duration:.2f} секунд")
        self.logger.info(f"Статистика: {json.dumps(stats, default=self._json_serialize)}")
        
        # Сохраняем статистику с добавлением временных меток
        stats_with_time = {
            "timestamp": datetime.now().isoformat(),
            "duration": duration,
            **stats
        }
        self.historical_stats.append(stats_with_time)
        
        self.operations_history.append({
            "operation": "end",
            "timestamp": end_time,
            "duration": duration,
            "stats": stats
        })
        
        # Сохраняем статистику в файл
        if self.enable_file_logging:
            self._save_stats_to_file(stats_with_time)
    
    def _save_stats_to_file(self, stats: Dict[str, Any]) -> None:
        """
        Сохраняет статистику в JSON и CSV файлы.
        
        Args:
            stats: Статистика для сохранения
        """
        # Сохраняем в JSON
        try:
            # Проверяем, существует ли файл и есть ли в нем данные
            if os.path.exists(self.stats_filename) and os.path.getsize(self.stats_filename) > 0:
                with open(self.stats_filename, 'r', encoding='utf-8') as f:
                    try:
                        existing_stats = json.load(f)
                    except json.JSONDecodeError:
                        existing_stats = []
                
                if not isinstance(existing_stats, list):
                    existing_stats = [existing_stats]
            else:
                existing_stats = []
            
            # Добавляем текущую статистику
            existing_stats.append(stats)
            
            # Записываем обратно в файл
            with open(self.stats_filename, 'w', encoding='utf-8') as f:
                json.dump(existing_stats, f, indent=2, default=self._json_serialize)
                
            self.logger.info(f"Статистика сохранена в {self.stats_filename}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении статистики в JSON: {str(e)}")
        
        # Сохраняем в CSV
        try:
            # Преобразуем вложенные словари в плоскую структуру
            flat_stats = self._flatten_dict(stats)
            
            # Проверяем существование файла
            file_exists = os.path.exists(self.csv_stats_filename)
            
            with open(self.csv_stats_filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=flat_stats.keys())
                
                # Записываем заголовок только если файл новый
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(flat_stats)
                
            self.logger.info(f"Статистика сохранена в {self.csv_stats_filename}")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении статистики в CSV: {str(e)}")
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Преобразует вложенный словарь в плоскую структуру.
        
        Args:
            d: Вложенный словарь
            parent_key: Родительский ключ для вложенных значений
            sep: Разделитель для ключей
            
        Returns:
            Плоский словарь
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Для списков просто записываем длину или конвертируем в строку
                items.append((new_key, len(v) if all(isinstance(x, dict) for x in v) else str(v)))
            else:
                items.append((new_key, v))
                
        return dict(items)
    
    def _json_serialize(self, obj):
        """
        Кастомный сериализатор для JSON.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, set):
            return list(obj)
        return str(obj)
    
    def generate_report(self, output_file: str = None) -> str:
        """
        Генерирует текстовый отчет о результатах дедупликации.
        
        Args:
            output_file: Файл для сохранения отчета (опционально)
            
        Returns:
            Текстовый отчет
        """
        if not self.historical_stats:
            return "Нет данных для формирования отчета"
        
        # Берем последнюю статистику
        stats = self.historical_stats[-1]
        
        report_lines = [
            "=" * 80,
            "ОТЧЕТ О ДЕДУПЛИКАЦИИ ЗАКАЗОВ",
            "=" * 80,
            f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-" * 80,
            "СТАТИСТИКА СОПОСТАВЛЕНИЯ:",
            f"Всего обработано транзакций GA4: {stats.get('total_ga4_transactions', 0)}",
            f"Всего промокодов: {stats.get('total_promo_transactions', 0)}",
            f"Точных совпадений: {stats.get('exact_matches', 0)} "
            f"({stats.get('exact_match_rate', 0)*100:.2f}%)",
            f"Нечетких совпадений: {stats.get('fuzzy_matches', 0)} "
            f"({stats.get('fuzzy_match_rate', 0)*100:.2f}%)",
            f"Без совпадений: {stats.get('unmatched', 0)}",
            "-" * 80,
            "КОНФЛИКТЫ И РАЗРЕШЕНИЕ:",
            f"Всего конфликтов: {stats.get('conflicts_resolved', 0)}",
        ]
        
        # Добавляем информацию о стратегиях разрешения конфликтов
        if 'conflicts_by_strategy' in stats:
            for strategy, count in stats['conflicts_by_strategy'].items():
                if count > 0:
                    report_lines.append(f"- {strategy}: {count}")
        
        report_lines.extend([
            "-" * 80,
            "ИСТОЧНИКИ АТРИБУЦИИ:",
        ])
        
        # Добавляем информацию об источниках атрибуции
        if 'attribution_sources' in stats:
            for source, count in stats['attribution_sources'].items():
                if count > 0:
                    report_lines.append(f"- {source}: {count}")
        
        report_lines.extend([
            "-" * 80,
            "ПРОИЗВОДИТЕЛЬНОСТЬ:",
            f"Время выполнения: {stats.get('duration', 0):.2f} сек.",
            "=" * 80
        ])
        
        report_text = "\n".join(report_lines)
        
        # Сохраняем отчет в файл, если указан
        if output_file:
            try:
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_text)
                self.logger.info(f"Отчет сохранен в {output_file}")
            except Exception as e:
                self.logger.error(f"Ошибка при сохранении отчета: {str(e)}")
        
        return report_text
    
    def generate_charts(self, output_dir: str = None) -> List[str]:
        """
        Генерирует графики на основе собранной статистики.
        
        Args:
            output_dir: Директория для сохранения графиков (опционально)
            
        Returns:
            Список путей к сохраненным графикам
        """
        # Проверяем доступность matplotlib
        if not MATPLOTLIB_AVAILABLE:
            self.logger.warning("Matplotlib не установлен. Визуализация отключена.")
            return []
        
        if not self.historical_stats:
            return []
        
        stats = self.historical_stats[-1]
        
        # Если не указана директория, используем директорию статистики
        if not output_dir:
            output_dir = os.path.join(self.stats_dir, 'charts')
        
        # Создаем директорию если не существует
        os.makedirs(output_dir, exist_ok=True)
        
        saved_charts = []
        
        # График 1: Соотношение типов совпадений
        try:
            plt.figure(figsize=(10, 6))
            match_types = ['exact_matches', 'fuzzy_matches', 'unmatched']
            match_values = [stats.get(type_name, 0) for type_name in match_types]
            match_labels = ['Точные совпадения', 'Нечеткие совпадения', 'Без совпадений']
            
            plt.pie(match_values, labels=match_labels, autopct='%1.1f%%', startangle=90)
            plt.axis('equal')
            plt.title('Распределение типов совпадений транзакций')
            
            chart_path = os.path.join(output_dir, 'match_types_pie.png')
            plt.savefig(chart_path)
            plt.close()
            
            saved_charts.append(chart_path)
            self.logger.info(f"Сохранен график: {chart_path}")
        except Exception as e:
            self.logger.error(f"Ошибка при создании графика распределения типов совпадений: {str(e)}")
        
        # График 2: Источники атрибуции
        if 'attribution_sources' in stats:
            try:
                plt.figure(figsize=(10, 6))
                sources = list(stats['attribution_sources'].keys())
                values = list(stats['attribution_sources'].values())
                
                plt.bar(sources, values)
                plt.title('Распределение источников атрибуции')
                plt.xlabel('Источник')
                plt.ylabel('Количество транзакций')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                
                chart_path = os.path.join(output_dir, 'attribution_sources_bar.png')
                plt.savefig(chart_path)
                plt.close()
                
                saved_charts.append(chart_path)
                self.logger.info(f"Сохранен график: {chart_path}")
            except Exception as e:
                self.logger.error(f"Ошибка при создании графика источников атрибуции: {str(e)}")
        
        return saved_charts
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """
        Возвращает сводку статистики за все запуски.
        
        Returns:
            Сводная статистика
        """
        if not self.historical_stats:
            return {}
        
        # Вычисляем средние значения для основных метрик
        total_runs = len(self.historical_stats)
        
        summary = {
            "total_runs": total_runs,
            "avg_exact_matches": sum(s.get('exact_matches', 0) for s in self.historical_stats) / total_runs,
            "avg_fuzzy_matches": sum(s.get('fuzzy_matches', 0) for s in self.historical_stats) / total_runs,
            "avg_unmatched": sum(s.get('unmatched', 0) for s in self.historical_stats) / total_runs,
            "avg_match_rate": sum(s.get('match_rate', 0) for s in self.historical_stats) / total_runs,
            "avg_duration": sum(s.get('duration', 0) for s in self.historical_stats) / total_runs,
            "last_run": self.historical_stats[-1].get('timestamp', datetime.now().isoformat()),
            "first_run": self.historical_stats[0].get('timestamp', datetime.now().isoformat())
        }
        
        return summary
