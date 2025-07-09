"""
Модуль для выполнения многомерного анализа результатов сравнения с медиапланом.
"""
import logging
import json
from typing import List, Dict, Any
from collections import defaultdict
from .db import get_connection

logger = logging.getLogger(__name__)

class MultiDimensionalAnalyzer:
    """
    Анализирует результаты сравнения с медиапланом в различных разрезах.
    """

    def _safe_divide(self, numerator: float, denominator: float) -> float:
        """Безопасное деление."""
        if denominator == 0:
            return 0.0
        return numerator / denominator

    def _calculate_group_variance(self, group_data: Dict[str, float]) -> Dict[str, Any]:
        """Рассчитывает отклонения для сгруппированных данных."""
        variances = {}
        
        # 1. Отклонения для базовых метрик
        base_metrics = ['spend', 'impressions', 'clicks', 'orders', 'revenue']
        for metric in base_metrics:
            fact = group_data.get(f'fact_{metric}', 0)
            plan = group_data.get(f'plan_{metric}', 0)
            absolute_variance = fact - plan
            relative_variance = self._safe_divide(absolute_variance, plan) if plan else (1.0 if fact > 0 else 0.0)
            variances[metric] = {
                'fact': fact,
                'plan': plan,
                'absolute_variance': absolute_variance,
                'relative_variance_percent': relative_variance * 100
            }

        # 2. Расчет производных метрик и их отклонений
        # Fact derived
        fact_spend = group_data.get('fact_spend', 0)
        fact_impressions = group_data.get('fact_impressions', 0)
        fact_clicks = group_data.get('fact_clicks', 0)
        fact_orders = group_data.get('fact_orders', 0)
        fact_revenue = group_data.get('fact_revenue', 0)
        
        fact_cpm = self._safe_divide(fact_spend * 1000, fact_impressions)
        fact_cpc = self._safe_divide(fact_spend, fact_clicks)
        fact_cpo = self._safe_divide(fact_spend, fact_orders)
        fact_drr = self._safe_divide(fact_spend * 100, fact_revenue)

        # Plan derived
        plan_spend = group_data.get('plan_spend', 0)
        plan_impressions = group_data.get('plan_impressions', 0)
        plan_clicks = group_data.get('plan_clicks', 0)
        plan_orders = group_data.get('plan_orders', 0)
        plan_revenue = group_data.get('plan_revenue', 0)

        plan_cpm = self._safe_divide(plan_spend * 1000, plan_impressions)
        plan_cpc = self._safe_divide(plan_spend, plan_clicks)
        plan_cpo = self._safe_divide(plan_spend, plan_orders)
        plan_drr = self._safe_divide(plan_spend * 100, plan_revenue)

        derived_values = {
            'cpm': {'fact': fact_cpm, 'plan': plan_cpm},
            'cpc': {'fact': fact_cpc, 'plan': plan_cpc},
            'cpo': {'fact': fact_cpo, 'plan': plan_cpo},
            'drr': {'fact': fact_drr, 'plan': plan_drr},
        }

        for metric, values in derived_values.items():
            absolute_variance = values['fact'] - values['plan']
            relative_variance = self._safe_divide(absolute_variance, values['plan']) if values['plan'] else (1.0 if values['fact'] > 0 else 0.0)
            variances[metric] = {
                'fact': values['fact'],
                'plan': values['plan'],
                'absolute_variance': absolute_variance,
                'relative_variance_percent': relative_variance * 100
            }
            
        return variances

    def analyze(self, comparison_data: List[Dict[str, Any]], dimensions: List[str]) -> Dict[str, Any]:
        """
        Выполняет многомерный анализ.

        Args:
            comparison_data: Список словарей с результатами сравнения.
            dimensions: Список разрезов для анализа (e.g., ['source', 'country']).

        Returns:
            Словарь с результатами анализа по каждому разрезу.
        """
        analysis_results = {}

        for dimension in dimensions:
            grouped_data = defaultdict(lambda: defaultdict(float))
            
            for record in comparison_data:
                if not record.get('matched'):
                    continue

                dim_value = record.get(dimension) or 'N/A'
                variances = record.get('variances', {})
                
                # Суммируем только базовые метрики, так как производные нужно пересчитывать на агрегированных данных
                base_metrics = ['spend', 'impressions', 'clicks', 'orders', 'revenue']
                for metric in base_metrics:
                    if metric in variances:
                        grouped_data[dim_value][f'plan_{metric}'] += variances[metric].get('plan', 0)
                        grouped_data[dim_value][f'fact_{metric}'] += variances[metric].get('fact', 0)
            
            dimension_results = {}
            for dim_value, group_data in grouped_data.items():
                dimension_results[dim_value] = self._calculate_group_variance(group_data)
            
            analysis_results[dimension] = dimension_results
            
        return analysis_results

def multi_dimensional_analysis_task(execution_date_str: str, **kwargs):
    """
    Задача Airflow для многомерного анализа.
    """
    logger.info("Запуск задачи многомерного анализа.")
    
    # 1. Получаем данные из таблицы media_plan_comparison
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM media_plan_comparison WHERE DATE_TRUNC('month', campaign_date) = DATE_TRUNC('month', %s::date)",
                    (execution_date_str,)
                )
                columns = [desc[0] for desc in cur.description]
                comparison_data = [dict(zip(columns, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Ошибка при получении данных для анализа: {e}")
        return

    if not comparison_data:
        logger.warning("Нет данных для многомерного анализа.")
        return

    # 2. Выполняем анализ
    analyzer = MultiDimensionalAnalyzer()
    dimensions_to_analyze = ['source', 'campaign_type', 'country', 'restaurant']
    analysis_results = analyzer.analyze(comparison_data, dimensions_to_analyze)

    # 3. Сохраняем результаты в БД
    _save_analysis_to_db(analysis_results, execution_date_str)
    
    logger.info("Многомерный анализ завершен.")

def _save_analysis_to_db(results: Dict[str, Any], execution_date_str: str):
    """Сохраняет результаты многомерного анализа в БД."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS multi_dimensional_analysis (
                        id SERIAL PRIMARY KEY,
                        analysis_date DATE NOT NULL,
                        dimension_name VARCHAR(255) NOT NULL,
                        dimension_value VARCHAR(255) NOT NULL,
                        variances JSONB,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Очищаем старые данные за этот месяц
                cur.execute("DELETE FROM multi_dimensional_analysis WHERE DATE_TRUNC('month', analysis_date) = DATE_TRUNC('month', %s::date)", (execution_date_str,))

                insert_query = """
                    INSERT INTO multi_dimensional_analysis (analysis_date, dimension_name, dimension_value, variances)
                    VALUES (%s, %s, %s, %s)
                """
                
                for dim_name, dim_results in results.items():
                    for dim_value, variances in dim_results.items():
                        cur.execute(insert_query, (
                            execution_date_str,
                            dim_name,
                            dim_value,
                            json.dumps(variances, default=str)
                        ))
            logger.info("Результаты многомерного анализа сохранены в БД.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении результатов многомерного анализа в БД: {e}")
