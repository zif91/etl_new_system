"""
Операции с базой данных для промокодов и заказов.
Класс для работы с таблицей promo_orders, включающий функции массовой вставки,
выборки, обновления и удаления записей о промокодах и заказах.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
import json
import time

from src.db import get_connection, insert_promo_order
from psycopg2.extras import execute_values, Json, RealDictCursor

logger = logging.getLogger(__name__)


class PromoDBManager:
    """
    Менеджер для работы с промокодами и заказами в базе данных
    """
    
    def __init__(self):
        """
        Инициализирует менеджер базы данных для промокодов
        """
        pass
    
    def bulk_insert_promo_orders(self, promo_orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Массовая вставка записей о промокодах и заказах в базу данных с поддержкой транзакций,
        обработкой ошибок и логированием результатов. Оптимизировано для больших объемов данных.
        
        Args:
            promo_orders: список словарей с данными о промокодах и заказах
            
        Returns:
            Словарь с результатами вставки: {
                'total': общее количество записей,
                'inserted': количество вставленных записей,
                'updated': количество обновленных записей,
                'failed': количество записей, которые не удалось вставить,
                'failed_records': список записей, которые не удалось вставить (с ошибками),
                'processing_time': время выполнения операции в секундах
            }
        """
        start_time = time.time()
        stats = {
            'total': len(promo_orders),
            'inserted': 0,
            'updated': 0,
            'failed': 0,
            'failed_records': [],
            'processing_time': 0
        }
        
        if not promo_orders:
            logger.warning("Empty promo_orders list provided to bulk_insert_promo_orders")
            return stats
        
        # Для массовой вставки используем batch подход с собственной транзакцией для каждого батча
        conn = get_connection()
        try:
            conn.autocommit = False
            
            # Оптимизируем для большого количества записей: разбиваем на батчи
            batch_size = 100  # Оптимальный размер батча для PostgreSQL
            batches = [promo_orders[i:i+batch_size] for i in range(0, len(promo_orders), batch_size)]
            
            logger.info(f"Processing {len(promo_orders)} records in {len(batches)} batches")
            
            for batch_idx, batch in enumerate(batches):
                batch_start = time.time()
                batch_stats = {'inserted': 0, 'updated': 0, 'failed': 0}
                
                try:
                    # Подготавливаем данные для массовой вставки
                    values = []
                    for record in batch:
                        # Связываем с campaign_id если есть
                        campaign_id = record.get('campaign_id')
                        
                        # Проверяем наличие обязательных полей
                        if not record.get('transaction_id'):
                            raise ValueError(f"Record missing transaction_id: {record}")
                        
                        values.append((
                            record.get('promo_code'),
                            record.get('order_id'),
                            record.get('transaction_id'),
                            record.get('order_date'),
                            record.get('order_amount', 0),
                            record.get('restaurant'),
                            record.get('country'),
                            record.get('promo_source'),
                            campaign_id,
                            False  # is_processed
                        ))
                    
                    # Эффективная вставка с помощью execute_values
                    cur = conn.cursor()
                    result = execute_values(
                        cur,
                        """
                        INSERT INTO promo_orders(
                            promo_code, order_id, transaction_id, order_date, order_amount, 
                            restaurant, country, promo_source, campaign_id, is_processed
                        ) VALUES %s
                        ON CONFLICT (transaction_id) DO UPDATE SET
                            promo_code = EXCLUDED.promo_code,
                            order_id = EXCLUDED.order_id,
                            order_date = EXCLUDED.order_date,
                            order_amount = EXCLUDED.order_amount,
                            restaurant = EXCLUDED.restaurant,
                            country = EXCLUDED.country,
                            promo_source = EXCLUDED.promo_source,
                            campaign_id = EXCLUDED.campaign_id,
                            is_processed = EXCLUDED.is_processed,
                            updated_at = NOW()
                        RETURNING 
                            (xmax = 0) AS inserted, 
                            (xmax <> 0) AS updated
                        """,
                        values,
                        fetch=True
                    )
                    
                    # Анализируем результаты операции
                    for r in result:
                        if r[0]:  # inserted
                            batch_stats['inserted'] += 1
                        else:  # updated
                            batch_stats['updated'] += 1
                    
                    conn.commit()
                    
                    stats['inserted'] += batch_stats['inserted']
                    stats['updated'] += batch_stats['updated']
                    
                    batch_time = time.time() - batch_start
                    logger.info(f"Batch {batch_idx+1}/{len(batches)} processed in {batch_time:.2f}s. "
                               f"Inserted: {batch_stats['inserted']}, Updated: {batch_stats['updated']}")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error processing batch {batch_idx+1}: {str(e)}")
                    
                    # Откатываем к индивидуальным вставкам для этого батча
                    for idx, promo_order in enumerate(batch):
                        try:
                            insert_promo_order(promo_order)
                            
                            # Определяем, была ли запись вставлена или обновлена
                            cur = conn.cursor()
                            cur.execute(
                                "SELECT 1 FROM promo_orders WHERE transaction_id = %s FOR UPDATE",
                                (promo_order['transaction_id'],)
                            )
                            if cur.rowcount > 0:
                                stats['updated'] += 1
                            else:
                                stats['inserted'] += 1
                            conn.commit()
                            
                        except Exception as e:
                            conn.rollback()
                            stats['failed'] += 1
                            error_record = {**promo_order, 'error': str(e)}
                            stats['failed_records'].append(error_record)
                            logger.error(f"Failed to insert record {batch_idx*batch_size + idx}: {str(e)}")
            
            stats['processing_time'] = time.time() - start_time
            logger.info(f"Bulk insert completed in {stats['processing_time']:.2f}s. "
                       f"Inserted: {stats['inserted']}, Updated: {stats['updated']}, Failed: {stats['failed']}")
            
        except Exception as e:
            logger.error(f"Critical error during bulk insert: {str(e)}")
            conn.rollback()
            raise
        finally:
            conn.close()
        
        return stats
    
    def get_promo_orders(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Получает список заказов с промокодами из базы данных.
        
        Args:
            filters: словарь с фильтрами (например, {"restaurant": "Тануки", "country": "Казахстан"})
            
        Returns:
            Список словарей с данными о промокодах и заказах
        """
        sql = "SELECT * FROM promo_orders"
        params = {}
        
        if filters:
            where_clauses = []
            for key, value in filters.items():
                if key == 'date_from' and isinstance(value, (date, datetime)):
                    where_clauses.append("order_date >= %(date_from)s")
                    params['date_from'] = value
                elif key == 'date_to' and isinstance(value, (date, datetime)):
                    where_clauses.append("order_date <= %(date_to)s")
                    params['date_to'] = value
                elif key in ['restaurant', 'country', 'promo_code', 'promo_source']:
                    where_clauses.append(f"{key} = %({key})s")
                    params[key] = value
                elif key == 'transaction_id' or key == 'order_id':
                    where_clauses.append(f"{key} = %({key})s")
                    params[key] = value
            
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
        
        sql += " ORDER BY order_date DESC"
        
        try:
            conn = get_connection()
            # Используем RealDictCursor для получения результатов в виде словарей
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                results = cur.fetchall()
            
            logger.info(f"Retrieved {len(results)} promo orders")
            return results
        except Exception as e:
            logger.error(f"Error retrieving promo orders: {e}")
            raise
        finally:
            conn.close()
    
    def get_promo_stats(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Получает статистику по промокодам.
        
        Args:
            filters: словарь с фильтрами (например, {"restaurant": "Тануки", "country": "Казахстан"})
            
        Returns:
            Словарь со статистикой: {
                'total_orders': общее количество заказов,
                'total_amount': общая сумма заказов,
                'avg_order_amount': средняя сумма заказа,
                'by_restaurant': статистика по ресторанам,
                'by_country': статистика по странам,
                'by_promo_source': статистика по источникам промокодов
            }
        """
        result = {
            'total_orders': 0,
            'total_amount': 0,
            'avg_order_amount': 0,
            'by_restaurant': {},
            'by_country': {},
            'by_promo_source': {}
        }
        
        # Базовый SQL для основных метрик
        sql = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(order_amount) as total_amount,
            AVG(order_amount) as avg_order_amount
        FROM promo_orders
        """
        
        # SQL для группировки по ресторанам
        sql_by_restaurant = """
        SELECT 
            restaurant,
            COUNT(*) as orders,
            SUM(order_amount) as total_amount,
            AVG(order_amount) as avg_amount
        FROM promo_orders
        """
        
        # SQL для группировки по странам
        sql_by_country = """
        SELECT 
            country,
            COUNT(*) as orders,
            SUM(order_amount) as total_amount,
            AVG(order_amount) as avg_amount
        FROM promo_orders
        """
        
        # SQL для группировки по источникам промокодов
        sql_by_promo_source = """
        SELECT 
            COALESCE(promo_source, 'unknown') as promo_source,
            COUNT(*) as orders,
            SUM(order_amount) as total_amount,
            AVG(order_amount) as avg_amount
        FROM promo_orders
        """
        
        # Добавляем условия фильтрации, если они есть
        where_clauses = []
        params = {}
        
        if filters:
            for key, value in filters.items():
                if key == 'date_from' and isinstance(value, (date, datetime)):
                    where_clauses.append("order_date >= %(date_from)s")
                    params['date_from'] = value
                elif key == 'date_to' and isinstance(value, (date, datetime)):
                    where_clauses.append("order_date <= %(date_to)s")
                    params['date_to'] = value
                elif key in ['restaurant', 'country', 'promo_code', 'promo_source']:
                    where_clauses.append(f"{key} = %({key})s")
                    params[key] = value
        
        # Добавляем условия WHERE к запросам, если они есть
        if where_clauses:
            where_clause = " WHERE " + " AND ".join(where_clauses)
            sql += where_clause
            sql_by_restaurant += where_clause
            sql_by_country += where_clause
            sql_by_promo_source += where_clause
        
        # Добавляем группировку к запросам
        sql_by_restaurant += " GROUP BY restaurant"
        sql_by_country += " GROUP BY country"
        sql_by_promo_source += " GROUP BY promo_source"
        
        try:
            conn = get_connection()
            
            # Получаем основные метрики
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                basic_stats = cur.fetchone()
                if basic_stats:
                    result['total_orders'] = basic_stats['total_orders']
                    result['total_amount'] = float(basic_stats['total_amount']) if basic_stats['total_amount'] else 0
                    result['avg_order_amount'] = float(basic_stats['avg_order_amount']) if basic_stats['avg_order_amount'] else 0
            
            # Получаем статистику по ресторанам
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql_by_restaurant, params)
                for row in cur:
                    result['by_restaurant'][row['restaurant']] = {
                        'orders': row['orders'],
                        'total_amount': float(row['total_amount']),
                        'avg_amount': float(row['avg_amount'])
                    }
            
            # Получаем статистику по странам
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql_by_country, params)
                for row in cur:
                    result['by_country'][row['country']] = {
                        'orders': row['orders'],
                        'total_amount': float(row['total_amount']),
                        'avg_amount': float(row['avg_amount'])
                    }
            
            # Получаем статистику по источникам промокодов
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql_by_promo_source, params)
                for row in cur:
                    result['by_promo_source'][row['promo_source']] = {
                        'orders': row['orders'],
                        'total_amount': float(row['total_amount']),
                        'avg_amount': float(row['avg_amount'])
                    }
            
            logger.info("Retrieved promo statistics")
            return result
        
        except Exception as e:
            logger.error(f"Error retrieving promo statistics: {e}")
            raise
        finally:
            conn.close()
    
    def delete_promo_orders(self, transaction_ids: List[str]) -> int:
        """
        Удаляет записи о промокодах и заказах из базы данных.
        
        Args:
            transaction_ids: список ID транзакций для удаления
            
        Returns:
            Количество удаленных записей
        """
        if not transaction_ids:
            return 0
        
        sql = """
        DELETE FROM promo_orders
        WHERE transaction_id = ANY(%s)
        """
        
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(sql, (transaction_ids,))
                deleted_count = cur.rowcount
                conn.commit()
                logger.info(f"Deleted {deleted_count} promo orders")
                return deleted_count
        except Exception as e:
            logger.error(f"Error deleting promo orders: {e}")
            raise
        finally:
            conn.close()
            
    def mark_as_processed(self, transaction_ids: List[str], campaign_id: Optional[str] = None) -> int:
        """
        Отмечает записи о промокодах как обработанные и опционально связывает их с кампанией.
        
        Args:
            transaction_ids: список ID транзакций для обновления
            campaign_id: ID кампании для связывания (опционально)
            
        Returns:
            Количество обновленных записей
        """
        if not transaction_ids:
            return 0
        
        sql = """
        UPDATE promo_orders
        SET is_processed = TRUE, updated_at = NOW()
        """
        
        params = [transaction_ids]
        
        if campaign_id:
            sql += ", campaign_id = %s"
            params.append(campaign_id)
        
        sql += " WHERE transaction_id = ANY(%s)"
        
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                updated_count = cur.rowcount
                conn.commit()
                logger.info(f"Marked {updated_count} promo orders as processed")
                return updated_count
        except Exception as e:
            logger.error(f"Error marking promo orders as processed: {e}")
            raise
        finally:
            conn.close()
            
    def link_promo_orders_to_campaign(self, filter_params: Dict[str, Any], campaign_id: str) -> int:
        """
        Связывает промокоды с рекламной кампанией на основе фильтра.
        
        Args:
            filter_params: параметры фильтрации (например, promo_code, date_range)
            campaign_id: ID кампании для связывания
            
        Returns:
            Количество связанных записей
        """
        if not campaign_id:
            raise ValueError("campaign_id is required")
        
        sql_parts = ["UPDATE promo_orders SET campaign_id = %s, updated_at = NOW() WHERE 1=1"]
        params = [campaign_id]
        
        if 'promo_code' in filter_params:
            sql_parts.append("AND promo_code = %s")
            params.append(filter_params['promo_code'])
            
        if 'restaurant' in filter_params:
            sql_parts.append("AND restaurant = %s")
            params.append(filter_params['restaurant'])
            
        if 'country' in filter_params:
            sql_parts.append("AND country = %s")
            params.append(filter_params['country'])
            
        if 'date_from' in filter_params:
            sql_parts.append("AND order_date >= %s")
            params.append(filter_params['date_from'])
            
        if 'date_to' in filter_params:
            sql_parts.append("AND order_date <= %s")
            params.append(filter_params['date_to'])
            
        if 'promo_source' in filter_params:
            sql_parts.append("AND promo_source = %s")
            params.append(filter_params['promo_source'])
        
        sql = " ".join(sql_parts)
        
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                updated_count = cur.rowcount
                conn.commit()
                logger.info(f"Linked {updated_count} promo orders to campaign {campaign_id}")
                return updated_count
        except Exception as e:
            logger.error(f"Error linking promo orders to campaign: {e}")
            raise
        finally:
            conn.close()
            
    def generate_import_report(self, import_stats: Dict[str, Any], start_date: Optional[date] = None,
                              end_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Генерирует отчет по импорту промокодов.
        
        Args:
            import_stats: статистика импорта (результат bulk_insert_promo_orders)
            start_date: начальная дата для отчета
            end_date: конечная дата для отчета
            
        Returns:
            Словарь с отчетом
        """
        report = {
            'import_stats': import_stats,
            'summary': {},
            'by_restaurant': {},
            'by_country': {},
            'by_source': {},
            'generated_at': datetime.now().isoformat()
        }
        
        try:
            # Дополняем отчет статистикой из БД
            filters = {}
            if start_date:
                filters['date_from'] = start_date
            if end_date:
                filters['date_to'] = end_date
                
            db_stats = self.get_promo_stats(filters)
            
            report['summary'] = {
                'total_orders': db_stats['total_orders'],
                'total_amount': db_stats['total_amount'],
                'avg_order_amount': db_stats['avg_order_amount'],
                'processed_orders': import_stats['inserted'] + import_stats['updated'],
                'failed_orders': import_stats['failed'],
                'processing_time': import_stats.get('processing_time', 0),
            }
            
            report['by_restaurant'] = db_stats['by_restaurant']
            report['by_country'] = db_stats['by_country']
            report['by_source'] = db_stats['by_promo_source']
            
            logger.info(f"Generated import report for period {start_date} - {end_date}")
            return report
            
        except Exception as e:
            logger.error(f"Error generating import report: {e}")
            return {
                'import_stats': import_stats,
                'error': str(e),
                'generated_at': datetime.now().isoformat()
            }
