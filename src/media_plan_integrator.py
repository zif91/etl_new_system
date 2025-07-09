"""
Модуль для интеграции сравнения с медиапланом в ETL процесс.
"""
import logging
import os
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from .media_plan_matcher import MediaPlanMatcher
from .media_plan_importer import MediaPlanImporter
from .db import get_connection

logger = logging.getLogger(__name__)

def compare_with_media_plan_task(media_plan_path=None, execution_date=None, **kwargs):
    """
    Задача Airflow для сравнения фактических данных с медиапланом.
    
    Args:
        media_plan_path: Путь к файлу медиаплана (если None, будет импортирован заново)
        execution_date: Дата выполнения в формате 'YYYY-MM-DD'
        kwargs: Дополнительные параметры
        
    Returns:
        Путь к файлу с результатами сравнения
    """
    
    if execution_date is None:
        execution_date = datetime.now().strftime('%Y-%m-%d')
    
    if isinstance(execution_date, str):
        dt = datetime.strptime(execution_date, '%Y-%m-%d')
    else:
        dt = execution_date
    
    logger.info(f"Выполняем сравнение с медиапланом для даты: {dt.strftime('%Y-%m-%d')}")
    
    # 1. Загрузка данных медиаплана
    media_plan_data = _load_media_plan(media_plan_path, dt)
    if not media_plan_data:
        logger.warning("Данные медиаплана отсутствуют, задача завершается.")
        return None

    # 2. Загрузка ручных сопоставлений (если есть)
    manual_mappings = _load_manual_mappings()

    # 3. Создание матчера
    matcher = MediaPlanMatcher(media_plan_data, manual_mappings)
    
    # 4. Получение фактических данных из БД
    campaign_metrics = _get_campaign_metrics_from_db(dt)
    if not campaign_metrics:
        logger.warning("Нет данных кампаний для сравнения с медиапланом.")
        return None
    
    # 5. Выполнение сравнения и обработка результатов
    comparison_results, stats = _perform_comparison(matcher, campaign_metrics)
    
    logger.info(f"Сравнение завершено. "
                f"Сопоставлено: {stats['matched']} (ручных: {stats['manual']}, "
                f"точных: {stats['exact']}, нечетких: {stats['fuzzy']}). "
                f"Не сопоставлено: {stats['unmatched']}. "
                f"Неоднозначных: {stats['ambiguous']}.")

    # 6. Сохранение результатов
    output_path = _save_comparison_results(comparison_results, dt)
    
    # 7. Сохранение результатов в БД
    _save_results_to_db(comparison_results)
    
    return output_path

def _load_media_plan(media_plan_path: Optional[str], execution_date: datetime) -> List[Dict[str, Any]]:
    """Загружает данные медиаплана из файла или через импортер."""
    if media_plan_path:
        try:
            with open(media_plan_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка при чтении файла медиаплана: {e}")
            return []
    else:
        importer = MediaPlanImporter()
        return importer.import_media_plan(execution_date.strftime('%Y-%m'))

def _load_manual_mappings(mappings_path: str = "data/manual_mappings.json") -> Dict[Tuple, int]:
    """Загружает ручные сопоставления из JSON файла."""
    if not os.path.exists(mappings_path):
        return {}
    try:
        with open(mappings_path, 'r', encoding='utf-8') as f:
            raw_mappings = json.load(f)
        # Ключи в JSON - строки, их нужно преобразовать обратно в кортежи
        return {tuple(eval(k)): v for k, v in raw_mappings.items()}
    except (Exception, SyntaxError) as e:
        logger.error(f"Ошибка при загрузке ручных сопоставлений: {e}")
        return {}

def _get_campaign_metrics_from_db(execution_date: datetime) -> List[Dict[str, Any]]:
    """Получает агрегированные метрики кампаний из базы данных."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Запрос остается тот же, но вынесен в отдельную функцию
                cur.execute("""
                    SELECT 
                        c.date, c.restaurant, c.country, c.campaign_type, 
                        c.campaign_goal, c.source, c.campaign_name,
                        SUM(c.spend) as spend,
                        SUM(c.impressions) as impressions,
                        SUM(c.clicks) as clicks,
                        COUNT(DISTINCT o.order_id) as orders,
                        SUM(o.revenue) as revenue
                    FROM campaigns c
                    LEFT JOIN deduplicated_orders o ON c.source = o.attribution_source AND DATE(c.date) = DATE(o.order_date)
                    WHERE DATE_TRUNC('month', c.date) = DATE_TRUNC('month', %s::date)
                    GROUP BY c.date, c.restaurant, c.country, c.campaign_type, c.campaign_goal, c.source, c.campaign_name
                """, (execution_date,))
                
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Ошибка при получении данных из БД: {e}")
        return []

def _perform_comparison(matcher: MediaPlanMatcher, campaigns: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Выполняет сопоставление для списка кампаний и собирает статистику."""
    results = []
    stats = {
        'matched': 0, 'unmatched': 0, 'manual': 0, 
        'exact': 0, 'fuzzy': 0, 'ambiguous': 0
    }
    
    for campaign in campaigns:
        match_result = matcher.match_campaign_to_media_plan(campaign)
        
        campaign_info = {
            'campaign_date': campaign['date'].strftime('%Y-%m-%d') if isinstance(campaign.get('date'), datetime) else campaign.get('date'),
            'restaurant': campaign.get('restaurant'),
            'country': campaign.get('country'),
            'campaign_type': campaign.get('campaign_type'),
            'campaign_goal': campaign.get('campaign_goal'),
            'source': campaign.get('source'),
            'campaign_name': campaign.get('campaign_name')
        }

        if match_result:
            stats['matched'] += 1
            if match_result.get('is_manual'):
                stats['manual'] += 1
            elif match_result.get('is_fuzzy'):
                stats['fuzzy'] += 1
            else:
                stats['exact'] += 1
            
            if match_result.get('is_ambiguous'):
                stats['ambiguous'] += 1

            results.append({
                **campaign_info,
                'matched': True,
                'is_manual': match_result.get('is_manual', False),
                'is_fuzzy': match_result.get('is_fuzzy', False),
                'is_ambiguous': match_result.get('is_ambiguous', False),
                'match_score': match_result.get('match_score'),
                'media_plan_id': match_result['media_plan_id'],
                'variances': match_result['variances']
            })
        else:
            stats['unmatched'] += 1
            results.append({
                **campaign_info,
                'matched': False,
                'is_manual': False,
                'is_fuzzy': False,
                'is_ambiguous': False,
                'match_score': None,
                'media_plan_id': None,
                'variances': {}
            })
    return results, stats

def _save_comparison_results(results: List[Dict[str, Any]], execution_date: datetime) -> str:
    """Сохраняет результаты сравнения в JSON файл."""
    output_dir = "data/comparisons"
    os.makedirs(output_dir, exist_ok=True)
    
    file_name = f"comparison_{execution_date.strftime('%Y_%m')}.json"
    output_path = os.path.join(output_dir, file_name)
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4, default=str)
        logger.info(f"Результаты сравнения сохранены в файл: {output_path}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла с результатами: {e}")
        
    return output_path

def _save_results_to_db(results: List[Dict[str, Any]]):
    """
    Сохраняет результаты сравнения в базу данных.
    Предполагается наличие таблицы 'media_plan_comparison'.
    """
    if not results:
        return

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Создаем таблицу, если она не существует
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS media_plan_comparison (
                        id SERIAL PRIMARY KEY,
                        comparison_date DATE NOT NULL,
                        campaign_date DATE NOT NULL,
                        restaurant VARCHAR(255),
                        country VARCHAR(255),
                        campaign_type VARCHAR(255),
                        campaign_goal VARCHAR(255),
                        source VARCHAR(255),
                        campaign_name VARCHAR(512),
                        matched BOOLEAN NOT NULL,
                        is_manual BOOLEAN,
                        is_fuzzy BOOLEAN,
                        is_ambiguous BOOLEAN,
                        match_score INTEGER,
                        media_plan_id INTEGER,
                        variances JSONB,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Очищаем старые данные за этот месяц, чтобы избежать дублей
                month_start = datetime.strptime(results[0]['campaign_date'], '%Y-%m-%d').strftime('%Y-%m-01')
                cur.execute("DELETE FROM media_plan_comparison WHERE DATE_TRUNC('month', campaign_date) = %s", (month_start,))

                # Вставляем новые данные
                insert_query = """
                    INSERT INTO media_plan_comparison (
                        comparison_date, campaign_date, restaurant, country, campaign_type, 
                        campaign_goal, source, campaign_name, matched, is_manual, is_fuzzy, 
                        is_ambiguous, match_score, media_plan_id, variances
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                today = datetime.now().date()
                
                for res in results:
                    cur.execute(insert_query, (
                        today,
                        res['campaign_date'],
                        res.get('restaurant'),
                        res.get('country'),
                        res.get('campaign_type'),
                        res.get('campaign_goal'),
                        res.get('source'),
                        res.get('campaign_name'),
                        res['matched'],
                        res.get('is_manual'),
                        res.get('is_fuzzy'),
                        res.get('is_ambiguous'),
                        res.get('match_score'),
                        res.get('media_plan_id'),
                        json.dumps(res.get('variances'), default=str)
                    ))
            logger.info(f"Сохранено {len(results)} записей о сравнении в БД.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении результатов в БД: {e}")

# Пример вызова, если модуль запускается напрямую
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Устанавливаем дату для примера
    example_date = '2025-07-15'
    compare_with_media_plan_task(execution_date=example_date)
