"""
Модуль для анализа и сравнения эффективности рекламных кампаний по нескольким измерениям.
Предоставляет функции для многомерного анализа и сравнения периодов (месяц к месяцу).
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple
import json
import os
from scipy import stats

from .db import get_connection

logger = logging.getLogger(__name__)


class PerformanceAnalyzer:
    """
    Класс для многомерного анализа производительности рекламных кампаний.
    Поддерживает сравнение по периодам, каналам, аудиториям и креативам.
    """

    def __init__(self):
        """
        Инициализирует анализатор производительности.
        """
        self.key_metrics = [
            'spend',
            'impressions',
            'clicks', 
            'orders',
            'revenue',
            'cpm', 
            'cpc',
            'cpa',
            'cpo',
            'drr'
        ]

    def compare_month_to_month(
        self, 
        current_month: str,
        previous_month: Optional[str] = None,
        dimensions: List[str] = ['restaurant', 'country', 'campaign_type', 'source'],
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Выполняет сравнение производительности месяц к месяцу.

        Args:
            current_month: Месяц в формате 'YYYY-MM' для анализа
            previous_month: Предыдущий месяц для сравнения (если None, берется предыдущий календарный месяц)
            dimensions: Список измерений для группировки (например, ['restaurant', 'country'])
            metrics: Список метрик для сравнения (если None, используются все ключевые метрики)

        Returns:
            Словарь с результатами сравнения
        """
        # Если не указаны метрики, используем все ключевые
        if metrics is None:
            metrics = self.key_metrics

        # Если не указан предыдущий месяц, вычисляем его
        if previous_month is None:
            current_date = datetime.strptime(f"{current_month}-01", "%Y-%m-%d")
            prev_date = current_date - relativedelta(months=1)
            previous_month = prev_date.strftime("%Y-%m")

        logger.info(f"Comparing performance: {current_month} vs {previous_month}")

        # Получаем данные для текущего и предыдущего месяцев
        current_data = self._get_monthly_metrics(current_month)
        previous_data = self._get_monthly_metrics(previous_month)

        if not current_data or not previous_data:
            logger.warning("No data available for comparison")
            return {
                "status": "error",
                "message": "No data available for comparison",
                "current_month": current_month,
                "previous_month": previous_month
            }

        # Преобразуем в pandas DataFrame для удобства анализа
        current_df = pd.DataFrame(current_data)
        previous_df = pd.DataFrame(previous_data)

        # Группируем по указанным измерениям
        grouped_current = self._group_by_dimensions(current_df, dimensions, metrics)
        grouped_previous = self._group_by_dimensions(previous_df, dimensions, metrics)

        # Выполняем сравнение и расчет отклонений
        comparison_results = self._calculate_variances(grouped_current, grouped_previous, metrics)

        # Добавляем статистическую значимость
        comparison_results = self._add_statistical_significance(comparison_results, metrics)

        return {
            "status": "success",
            "current_month": current_month,
            "previous_month": previous_month,
            "dimensions": dimensions,
            "metrics": metrics,
            "results": comparison_results,
            "summary": self._generate_summary(comparison_results, metrics)
        }

    def _get_monthly_metrics(self, month: str) -> List[Dict[str, Any]]:
        """
        Получает метрики для указанного месяца из базы данных.

        Args:
            month: Месяц в формате 'YYYY-MM'

        Returns:
            Список словарей с данными метрик
        """
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # Формируем даты начала и конца месяца
                    start_date = f"{month}-01"
                    next_month = (datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(months=1)).strftime("%Y-%m-%d")
                    
                    # SQL запрос для получения агрегированных метрик
                    cur.execute("""
                        SELECT 
                            DATE_TRUNC('month', dm.date) AS month,
                            c.restaurant,
                            c.country,
                            c.type AS campaign_type,
                            c.goal,
                            c.source,
                            SUM(dm.impressions) AS impressions,
                            SUM(dm.clicks) AS clicks,
                            SUM(dm.spend) AS spend,
                            SUM(dm.orders) AS orders,
                            SUM(dm.revenue) AS revenue,
                            AVG(dm.cpm) AS cpm,
                            AVG(dm.cpc) AS cpc,
                            AVG(dm.cpo) AS cpo,
                            AVG(dm.cpa) AS cpa,
                            AVG(dm.drr) AS drr
                        FROM 
                            daily_metrics dm
                        JOIN 
                            campaigns c ON dm.campaign_id = c.id
                        WHERE 
                            dm.date >= %s AND dm.date < %s
                        GROUP BY 
                            DATE_TRUNC('month', dm.date),
                            c.restaurant,
                            c.country,
                            c.type,
                            c.goal,
                            c.source
                    """, (start_date, next_month))
                    
                    columns = [desc[0] for desc in cur.description]
                    results = [dict(zip(columns, row)) for row in cur.fetchall()]
                    
                    logger.info(f"Retrieved {len(results)} metrics records for {month}")
                    return results
        except Exception as e:
            logger.error(f"Error retrieving metrics data for {month}: {e}")
            return []

    def _group_by_dimensions(
        self, 
        df: pd.DataFrame,
        dimensions: List[str],
        metrics: List[str]
    ) -> pd.DataFrame:
        """
        Группирует данные по указанным измерениям.

        Args:
            df: DataFrame с данными метрик
            dimensions: Список измерений для группировки
            metrics: Список метрик для агрегации

        Returns:
            Сгруппированный DataFrame
        """
        # Проверяем наличие всех измерений в данных
        valid_dimensions = [d for d in dimensions if d in df.columns]
        
        if not valid_dimensions:
            logger.warning(f"No valid dimensions found among {dimensions}")
            return df
        
        # Определяем функции агрегации для разных метрик
        agg_functions = {}
        for metric in metrics:
            if metric in df.columns:
                if metric in ['cpm', 'cpc', 'cpa', 'cpo', 'drr']:
                    # Для относительных показателей используем среднее
                    agg_functions[metric] = 'mean'
                else:
                    # Для абсолютных показателей используем сумму
                    agg_functions[metric] = 'sum'
        
        # Группируем данные
        if agg_functions:
            grouped = df.groupby(valid_dimensions).agg(agg_functions).reset_index()
            return grouped
        else:
            logger.warning(f"No valid metrics found among {metrics}")
            return df

    def _calculate_variances(
        self, 
        current_data: pd.DataFrame,
        previous_data: pd.DataFrame,
        metrics: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Рассчитывает отклонения между текущим и предыдущим периодами.

        Args:
            current_data: DataFrame с текущими данными
            previous_data: DataFrame с предыдущими данными
            metrics: Список метрик для сравнения

        Returns:
            Список словарей с результатами сравнения
        """
        # Проверяем, что оба DataFrame имеют данные
        if current_data.empty or previous_data.empty:
            logger.warning("Cannot calculate variances with empty data")
            return []
        
        # Определяем измерения (все столбцы, кроме метрик)
        dimensions = [col for col in current_data.columns if col not in self.key_metrics]
        
        # Объединяем данные для сравнения
        merged_data = pd.merge(
            current_data, 
            previous_data, 
            on=dimensions, 
            how='outer', 
            suffixes=('_current', '_previous')
        )
        
        # Заполняем отсутствующие значения нулями
        merged_data = merged_data.fillna(0)
        
        # Рассчитываем отклонения для каждой метрики
        results = []
        for _, row in merged_data.iterrows():
            result = {dim: row[dim] for dim in dimensions if dim in row}
            
            # Расчет отклонений для каждой метрики
            result['metrics'] = {}
            for metric in metrics:
                current_metric = f"{metric}_current"
                previous_metric = f"{metric}_previous"
                
                if current_metric in row and previous_metric in row:
                    current_value = row[current_metric]
                    previous_value = row[previous_metric]
                    
                    # Рассчитываем абсолютное отклонение
                    absolute_variance = current_value - previous_value
                    
                    # Рассчитываем относительное отклонение (в процентах)
                    if previous_value != 0:
                        relative_variance = (absolute_variance / previous_value) * 100
                    elif current_value != 0:
                        relative_variance = 100  # Если было 0, а стало что-то - считаем как 100% рост
                    else:
                        relative_variance = 0  # Если было 0 и осталось 0
                    
                    result['metrics'][metric] = {
                        'current': current_value,
                        'previous': previous_value,
                        'absolute_variance': absolute_variance,
                        'relative_variance_percent': relative_variance
                    }
            
            results.append(result)
        
        return results

    def _add_statistical_significance(
        self, 
        comparison_results: List[Dict[str, Any]],
        metrics: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Добавляет анализ статистической значимости изменений.

        Args:
            comparison_results: Результаты сравнения
            metrics: Список анализируемых метрик

        Returns:
            Обновленные результаты сравнения с информацией о статистической значимости
        """
        # Собираем данные для каждой метрики
        metrics_data = {metric: {'current': [], 'previous': []} for metric in metrics}
        
        for result in comparison_results:
            for metric in metrics:
                if metric in result.get('metrics', {}):
                    metrics_data[metric]['current'].append(result['metrics'][metric]['current'])
                    metrics_data[metric]['previous'].append(result['metrics'][metric]['previous'])
        
        # Рассчитываем статистическую значимость
        significance = {}
        for metric in metrics:
            current_data = metrics_data[metric]['current']
            previous_data = metrics_data[metric]['previous']
            
            if len(current_data) > 1 and len(previous_data) > 1:
                try:
                    # Проводим t-тест для зависимых выборок
                    t_stat, p_value = stats.ttest_rel(current_data, previous_data)
                    
                    significance[metric] = {
                        'p_value': p_value,
                        'significant': p_value < 0.05,  # Стандартный порог значимости 5%
                        't_statistic': t_stat
                    }
                except Exception as e:
                    logger.warning(f"Cannot calculate significance for {metric}: {e}")
                    significance[metric] = {
                        'p_value': None,
                        'significant': False,
                        'error': str(e)
                    }
        
        # Добавляем информацию о значимости к результатам
        for result in comparison_results:
            result['statistical_significance'] = {
                metric: significance.get(metric, {'significant': False})
                for metric in metrics if metric in result.get('metrics', {})
            }
        
        return comparison_results

    def _generate_summary(
        self, 
        comparison_results: List[Dict[str, Any]],
        metrics: List[str]
    ) -> Dict[str, Any]:
        """
        Генерирует сводную информацию о сравнении.

        Args:
            comparison_results: Результаты сравнения
            metrics: Список метрик для сравнения

        Returns:
            Словарь со сводной информацией
        """
        summary = {
            'overall': {},
            'by_metric': {}
        }
        
        # Для каждой метрики рассчитываем средние значения и изменения
        for metric in metrics:
            current_values = []
            previous_values = []
            absolute_variances = []
            relative_variances = []
            
            for result in comparison_results:
                if metric in result.get('metrics', {}):
                    current_values.append(result['metrics'][metric]['current'])
                    previous_values.append(result['metrics'][metric]['previous'])
                    absolute_variances.append(result['metrics'][metric]['absolute_variance'])
                    relative_variances.append(result['metrics'][metric]['relative_variance_percent'])
            
            if current_values and previous_values:
                summary['by_metric'][metric] = {
                    'current_average': sum(current_values) / len(current_values),
                    'previous_average': sum(previous_values) / len(previous_values),
                    'absolute_variance_average': sum(absolute_variances) / len(absolute_variances),
                    'relative_variance_percent_average': sum(relative_variances) / len(relative_variances),
                    'significant_changes': any(
                        result.get('statistical_significance', {}).get(metric, {}).get('significant', False)
                        for result in comparison_results
                    )
                }
        
        # Общая информация о сравнении
        summary['overall'] = {
            'total_categories': len(comparison_results),
            'improved_metrics_count': sum(
                1 for result in comparison_results
                for metric in result.get('metrics', {})
                if metric in metrics and result['metrics'][metric]['relative_variance_percent'] > 0
            ),
            'worsened_metrics_count': sum(
                1 for result in comparison_results
                for metric in result.get('metrics', {})
                if metric in metrics and result['metrics'][metric]['relative_variance_percent'] < 0
            )
        }
        
        return summary

    def save_comparison_results(self, comparison_results: Dict[str, Any], output_path: str) -> str:
        """
        Сохраняет результаты сравнения в JSON-файл.

        Args:
            comparison_results: Результаты сравнения
            output_path: Путь для сохранения файла (или директория)

        Returns:
            Полный путь к сохраненному файлу
        """
        # Проверяем, указана ли директория или полный путь
        if not output_path.endswith('.json'):
            # Создаем имя файла с текущей датой и информацией о сравнении
            current_month = comparison_results.get('current_month', 'unknown')
            previous_month = comparison_results.get('previous_month', 'unknown')
            filename = f"performance_comparison_{current_month}_vs_{previous_month}.json"
            output_path = os.path.join(output_path, filename)
        
        # Создаем директорию, если она не существует
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        # Сохраняем результаты
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(comparison_results, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Comparison results saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving comparison results: {e}")
            return None


def compare_month_to_month_task(current_month=None, previous_month=None, dimensions=None, metrics=None, output_path=None, **kwargs):
    """
    Задача Airflow для сравнения эффективности месяц к месяцу.

    Args:
        current_month: Текущий месяц в формате 'YYYY-MM' (если None, используется текущий месяц)
        previous_month: Предыдущий месяц для сравнения (если None, берется предыдущий календарный месяц)
        dimensions: Список измерений для группировки
        metrics: Список метрик для сравнения
        output_path: Путь для сохранения результатов

    Returns:
        Путь к файлу с результатами сравнения
    """
    # Определяем текущий месяц, если не указан
    if current_month is None:
        today = datetime.now()
        # Берем предыдущий месяц для анализа, так как текущий месяц может быть неполным
        current_date = today - relativedelta(months=1)
        current_month = current_date.strftime("%Y-%m")
    
    # Устанавливаем значения по умолчанию
    if dimensions is None:
        dimensions = ['restaurant', 'country', 'campaign_type', 'source']
    
    if output_path is None:
        output_path = "data/comparisons"
    
    logger.info(f"Starting month-to-month comparison for {current_month}")
    
    try:
        # Создаем анализатор производительности
        analyzer = PerformanceAnalyzer()
        
        # Выполняем сравнение месяц к месяцу
        results = analyzer.compare_month_to_month(
            current_month=current_month,
            previous_month=previous_month,
            dimensions=dimensions,
            metrics=metrics
        )
        
        # Сохраняем результаты
        output_file = analyzer.save_comparison_results(results, output_path)
        
        logger.info(f"Month-to-month comparison completed for {current_month}")
        return output_file
        
    except Exception as e:
        logger.error(f"Error performing month-to-month comparison: {e}")
        raise
