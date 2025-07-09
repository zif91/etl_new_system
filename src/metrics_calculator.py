"""
Расчёт метрик на основе дедуплицированных данных и расходов на рекламу.
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from src.db import get_connection, execute_values

logger = logging.getLogger(__name__)

def calculate_metrics_task(execution_date=None, **context):
    """
    Основная функция для запуска расчета метрик, интегрируется с Airflow.
    
    Args:
        execution_date: Дата выполнения в формате строки 'YYYY-MM-DD'
        context: Контекст выполнения Airflow (если используется)
        
    Returns:
        Словарь с результатами расчета
    """
    # Преобразуем строку в объект даты, если это строка
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    
    # Если дата не указана, используем вчерашнюю
    if not execution_date:
        execution_date = datetime.now().date() - timedelta(days=1)
        
    logger.info(f"Starting metrics calculation for date: {execution_date}")
    
    try:
        # Получаем дедуплицированные данные и данные о расходах
        dedup_data = get_deduplicated_transactions(execution_date)
        ad_costs = get_advertising_costs(execution_date)
        
        # Рассчитываем метрики
        metrics = calculate_metrics(dedup_data, ad_costs, execution_date)
        
        # Сохраняем метрики в базу данных
        save_metrics(metrics)
        
        logger.info(f"Metrics calculation completed for {execution_date}")
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating metrics: {e}")
        raise

def get_deduplicated_transactions(date):
    """
    Получает дедуплицированные данные о транзакциях за указанную дату.
    
    Args:
        date: Дата для извлечения данных
        
    Returns:
        Список дедуплицированных транзакций
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                transaction_id,
                transaction_date,
                utm_source,
                utm_medium,
                utm_campaign,
                purchase_revenue,
                is_promo_order,
                attribution_source,
                promo_code,
                promo_source
            FROM 
                deduplicated_transactions
            WHERE 
                transaction_date = %s
        """, (date,))
        
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        logger.info(f"Retrieved {len(results)} deduplicated transactions for {date}")
        return results
        
    except Exception as e:
        logger.error(f"Error retrieving deduplicated transactions: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def get_advertising_costs(date):
    """
    Получает данные о расходах на рекламу из всех источников за указанную дату.
    
    Args:
        date: Дата для извлечения данных
        
    Returns:
        Словарь с расходами по источникам
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Получаем расходы из Meta
        cur.execute("""
            SELECT 
                campaign_name, 
                SUM(spend) as spend
            FROM 
                meta_daily_metrics
            WHERE 
                date = %s
            GROUP BY 
                campaign_name
        """, (date,))
        
        meta_costs = {row[0]: float(row[1]) for row in cur.fetchall()}
        
        # Получаем расходы из Google Ads
        cur.execute("""
            SELECT 
                campaign_name, 
                SUM(cost) as cost
            FROM 
                google_ads_metrics
            WHERE 
                date = %s
            GROUP BY 
                campaign_name
        """, (date,))
        
        google_ads_costs = {row[0]: float(row[1]) for row in cur.fetchall()}
        
        # Получаем расходы из AppsFlyer
        cur.execute("""
            SELECT 
                campaign, 
                SUM(cost) as cost,
                SUM(installs) as installs,
                SUM(sessions) as sessions,
                SUM(in_app_events) as events
            FROM 
                appsflyer_metrics
            WHERE 
                date = %s
            GROUP BY 
                campaign
        """, (date,))
        
        appsflyer_costs = {}
        appsflyer_metrics = {}
        
        for row in cur.fetchall():
            campaign_name = row[0]
            appsflyer_costs[campaign_name] = float(row[1])
            appsflyer_metrics[campaign_name] = {
                'installs': int(row[2]) if row[2] else 0,
                'sessions': int(row[3]) if row[3] else 0,
                'events': int(row[4]) if row[4] else 0,
            }
        
        # Объединяем все расходы
        all_costs = {
            'meta': meta_costs,
            'google_ads': google_ads_costs,
            'appsflyer': appsflyer_costs
        }
        
        # Суммарные расходы по источникам
        source_totals = {
            'meta': sum(meta_costs.values()),
            'google_ads': sum(google_ads_costs.values()),
            'appsflyer': sum(appsflyer_costs.values()),
            'total': sum(meta_costs.values()) + sum(google_ads_costs.values()) + sum(appsflyer_costs.values())
        }
        
        # Данные по мобильным приложениям
        appsflyer_summary = {
            'total_installs': sum(metrics['installs'] for metrics in appsflyer_metrics.values()),
            'total_sessions': sum(metrics['sessions'] for metrics in appsflyer_metrics.values()),
            'total_events': sum(metrics['events'] for metrics in appsflyer_metrics.values()),
            'metrics_by_campaign': appsflyer_metrics
        }
        
        logger.info(f"Retrieved advertising costs for {date}: Total={source_totals['total']}, AppsFlyer installs={appsflyer_summary['total_installs']}")
        
        return {
            'campaigns': all_costs,
            'totals': source_totals,
            'appsflyer': appsflyer_summary
        }
        
    except Exception as e:
        logger.error(f"Error retrieving advertising costs: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def calculate_metrics(transactions, ad_costs, date):
    """
    Рассчитывает ключевые метрики на основе транзакций и расходов.
    
    Args:
        transactions: Список дедуплицированных транзакций
        ad_costs: Данные о расходах на рекламу
        date: Дата расчета
        
    Returns:
        Словарь с рассчитанными метриками
    """
    # Преобразуем транзакции в pandas DataFrame для удобства обработки
    if not transactions:
        logger.warning(f"No transactions found for {date}")
        return {
            'date': date.isoformat() if hasattr(date, 'isoformat') else date,
            'total_revenue': 0,
            'total_cost': ad_costs['totals']['total'],
            'total_orders': 0,
            'cpo': 0,
            'roas': 0,
            'drr': 0,
            'mobile_metrics': calculate_mobile_metrics(ad_costs)
        }
    
    df = pd.DataFrame(transactions)
    
    # Общий доход от всех транзакций
    total_revenue = float(df['purchase_revenue'].sum())
    
    # Общие расходы на рекламу
    total_cost = ad_costs['totals']['total']
    
    # Количество заказов
    total_orders = len(df)
    
    # Расчет CPO (Cost Per Order)
    cpo = total_cost / total_orders if total_orders > 0 else 0
    
    # Расчет ROAS (Return On Ad Spend)
    roas = total_revenue / total_cost if total_cost > 0 else 0
    
    # Расчет ДРР (Доля Рекламных Расходов)
    drr = (total_cost / total_revenue * 100) if total_revenue > 0 else 0
    
    # Группировка по источникам для детализированных метрик
    source_metrics = {}
    
    for source in df['utm_source'].dropna().unique():
        source_df = df[df['utm_source'] == source]
        source_revenue = float(source_df['purchase_revenue'].sum())
        source_orders = len(source_df)
        source_cost = 0
        
        # Пытаемся сопоставить источник с расходами
        if source.lower() == 'facebook' or source.lower() == 'instagram':
            source_cost = ad_costs['totals'].get('meta', 0)
        elif source.lower() == 'google':
            source_cost = ad_costs['totals'].get('google_ads', 0)
        elif source.lower() == 'appsflyer' or source.lower() == 'app':
            source_cost = ad_costs['totals'].get('appsflyer', 0)
        
        source_metrics[source] = {
            'revenue': source_revenue,
            'orders': source_orders,
            'cost': source_cost,
            'cpo': source_cost / source_orders if source_orders > 0 else 0,
            'roas': source_revenue / source_cost if source_cost > 0 else 0,
            'drr': (source_cost / source_revenue * 100) if source_revenue > 0 else 0
        }
    
    # Рассчитываем мобильные метрики
    mobile_metrics = calculate_mobile_metrics(ad_costs)
    
    metrics = {
        'date': date.isoformat() if hasattr(date, 'isoformat') else date,
        'total_revenue': total_revenue,
        'total_cost': total_cost,
        'total_orders': total_orders,
        'cpo': cpo,
        'roas': roas,
        'drr': drr,
        'by_source': source_metrics,
        'mobile_metrics': mobile_metrics
    }
    
    logger.info(f"Calculated metrics for {date}: Revenue={total_revenue}, Cost={total_cost}, Orders={total_orders}, CPO={cpo:.2f}, ROAS={roas:.2f}, DRR={drr:.2f}%")
    logger.info(f"Mobile metrics: Installs={mobile_metrics.get('total_installs', 0)}, CPI={mobile_metrics.get('cpi', 0):.2f}")
    
    return metrics

def calculate_mobile_metrics(ad_costs):
    """
    Рассчитывает метрики для мобильных приложений на основе данных AppsFlyer.
    
    Args:
        ad_costs: Данные о расходах на рекламу с включенными метриками AppsFlyer
        
    Returns:
        Словарь с рассчитанными мобильными метриками
    """
    # Проверяем наличие данных AppsFlyer
    if 'appsflyer' not in ad_costs or not ad_costs.get('appsflyer'):
        logger.warning("No AppsFlyer data found for mobile metrics calculation")
        return {
            'total_installs': 0,
            'total_sessions': 0,
            'total_events': 0,
            'cpi': 0,
            'cpe': 0
        }
        
    appsflyer_data = ad_costs['appsflyer']
    total_installs = appsflyer_data.get('total_installs', 0)
    total_sessions = appsflyer_data.get('total_sessions', 0)
    total_events = appsflyer_data.get('total_events', 0)
    total_cost = ad_costs['totals'].get('appsflyer', 0)
    
    # Расчет CPI (Cost Per Install)
    cpi = total_cost / total_installs if total_installs > 0 else 0
    
    # Расчет CPE (Cost Per Event)
    cpe = total_cost / total_events if total_events > 0 else 0
    
    # Расчет ARPU (Average Revenue Per User) - если есть данные о доходе
    # ARPU будем собирать из транзакций мобильных пользователей, если они есть
    
    # Детализация по кампаниям
    campaigns_metrics = {}
    for campaign, metrics in appsflyer_data.get('metrics_by_campaign', {}).items():
        campaign_cost = ad_costs['campaigns'].get('appsflyer', {}).get(campaign, 0)
        campaign_installs = metrics.get('installs', 0)
        campaign_sessions = metrics.get('sessions', 0)
        campaign_events = metrics.get('events', 0)
        
        campaigns_metrics[campaign] = {
            'installs': campaign_installs,
            'sessions': campaign_sessions,
            'events': campaign_events,
            'cost': campaign_cost,
            'cpi': campaign_cost / campaign_installs if campaign_installs > 0 else 0,
            'cpe': campaign_cost / campaign_events if campaign_events > 0 else 0
        }
    
    # Расчет показателей удержания (если доступны)
    retention_metrics = {}
    
    # Возвращаем все рассчитанные метрики
    return {
        'total_installs': total_installs,
        'total_sessions': total_sessions,
        'total_events': total_events,
        'cpi': cpi,
        'cpe': cpe,
        'by_campaign': campaigns_metrics,
        'retention': retention_metrics
    }

def save_metrics(metrics):
    """
    Сохраняет рассчитанные метрики в базу данных.
    
    Args:
        metrics: Словарь с метриками
        
    Returns:
        Количество сохраненных записей
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Проверяем, существует ли таблица
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'calculated_metrics'
            );
        """)
        
        if not cur.fetchone()[0]:
            logger.info("Creating calculated_metrics table")
            cur.execute("""
                CREATE TABLE calculated_metrics (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    total_revenue NUMERIC(15,2) NOT NULL,
                    total_cost NUMERIC(15,2) NOT NULL,
                    total_orders INTEGER NOT NULL,
                    cpo NUMERIC(15,2) NOT NULL,
                    roas NUMERIC(15,2) NOT NULL,
                    drr NUMERIC(15,2) NOT NULL,
                    source_metrics JSONB,
                    mobile_metrics JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (date)
                );
                
                CREATE INDEX idx_metrics_date ON calculated_metrics(date);
            """)
            conn.commit()
        else:
            # Проверяем, существует ли столбец mobile_metrics
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'calculated_metrics' AND column_name = 'mobile_metrics'
                );
            """)
            
            if not cur.fetchone()[0]:
                logger.info("Adding mobile_metrics column to calculated_metrics table")
                cur.execute("""
                    ALTER TABLE calculated_metrics
                    ADD COLUMN mobile_metrics JSONB;
                """)
                conn.commit()
        
        # Сохраняем общие метрики
        source_metrics = metrics.pop('by_source', {})
        mobile_metrics = metrics.pop('mobile_metrics', {})
        
        cur.execute("""
            INSERT INTO calculated_metrics
                (date, total_revenue, total_cost, total_orders, cpo, roas, drr, source_metrics, mobile_metrics)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) 
            DO UPDATE SET
                total_revenue = EXCLUDED.total_revenue,
                total_cost = EXCLUDED.total_cost,
                total_orders = EXCLUDED.total_orders,
                cpo = EXCLUDED.cpo,
                roas = EXCLUDED.roas,
                drr = EXCLUDED.drr,
                source_metrics = EXCLUDED.source_metrics,
                mobile_metrics = EXCLUDED.mobile_metrics,
                created_at = NOW()
        """, (
            metrics['date'],
            metrics['total_revenue'],
            metrics['total_cost'],
            metrics['total_orders'],
            metrics['cpo'],
            metrics['roas'],
            metrics['drr'],
            pd.io.json.dumps(source_metrics),
            pd.io.json.dumps(mobile_metrics)
        ))
        
        conn.commit()
        logger.info(f"Saved metrics for {metrics['date']}")
        
        return 1
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving metrics: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # При запуске как самостоятельного скрипта
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Calculate advertising metrics")
    parser.add_argument("--date", help="Date to calculate metrics for (YYYY-MM-DD)")
    args = parser.parse_args()
    
    execution_date = None
    if args.date:
        execution_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    
    calculate_metrics_task(execution_date)
