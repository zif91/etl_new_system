"""
Database persistence functions using psycopg2.
"""
import os
import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

import psycopg2
from psycopg2.extras import execute_values, Json

logger = logging.getLogger(__name__)


def get_connection():
    """
    Return a new database connection using DATABASE_URL from environment.
    """
    url = os.getenv('DATABASE_URL')
    if not url:
        raise EnvironmentError('DATABASE_URL is not set')
    return psycopg2.connect(url)


def upsert_campaign(campaign: dict):
    """
    Insert or update a campaign record.
    campaign: output of transform_campaign
    """
    sql = """
    INSERT INTO campaigns(
        campaign_id, campaign_name, parsed_campaign, account_id,
        objective, status, daily_budget, adset_id, adset_name,
        ad_id, campaign_type, campaign_goal, source, restaurant, city
    ) VALUES (
        %(campaign_id)s, %(campaign_name)s, %(parsed_campaign)s, %(account_id)s,
        %(objective)s, %(status)s, %(daily_budget)s, %(adset_id)s, %(adset_name)s,
        %(ad_id)s, %(campaign_type)s, %(campaign_goal)s, %(source)s, %(restaurant)s, %(city)s
    ) ON CONFLICT (campaign_id) DO UPDATE SET
        campaign_name = EXCLUDED.campaign_name,
        parsed_campaign = EXCLUDED.parsed_campaign,
        objective = EXCLUDED.objective,
        status = EXCLUDED.status,
        daily_budget = EXCLUDED.daily_budget,
        adset_id = EXCLUDED.adset_id,
        adset_name = EXCLUDED.adset_name,
        ad_id = EXCLUDED.ad_id,
        campaign_type = EXCLUDED.campaign_type,
        campaign_goal = EXCLUDED.campaign_goal,
        source = EXCLUDED.source,
        restaurant = EXCLUDED.restaurant,
        city = EXCLUDED.city,
        updated_at = NOW();
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, {**campaign, 'parsed_campaign': Json(campaign.get('parsed_campaign'))})
    conn.commit()
    cur.close()
    conn.close()


def insert_daily_metrics(metrics: list):
    """
    Bulk insert daily_metrics records.
    metrics: list of dicts from transform_insights
    """
    sql = """
    INSERT INTO daily_metrics(
        campaign_id, metric_date, impressions, clicks, spend, reach, cpm, cpc, ctr
    ) VALUES %s
    ON CONFLICT (campaign_id, metric_date) DO UPDATE SET
        impressions = EXCLUDED.impressions,
        clicks = EXCLUDED.clicks,
        spend = EXCLUDED.spend,
        reach = EXCLUDED.reach,
        cpm = EXCLUDED.cpm,
        cpc = EXCLUDED.cpc,
        ctr = EXCLUDED.ctr,
        updated_at = NOW();
    """
    values = [(
        m['campaign_id'], m['metric_date'], m['impressions'], m['clicks'],
        m['spend'], m['reach'], m['cpm'], m['cpc'], m['ctr']
    ) for m in metrics]
    conn = get_connection()
    cur = conn.cursor()
    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    conn.close()


def insert_ga4_metrics(records: list):
    """
    Bulk insert GA4 metrics records.
    records: list of dicts with keys matching ga4_metrics columns
    """
    sql = """
    INSERT INTO ga4_metrics(
        metric_date, utm_source, utm_medium, utm_campaign,
        sessions, users, conversions, purchase_revenue, transactions, transaction_ids
    ) VALUES %s
    ON CONFLICT (metric_date, utm_source, utm_medium, utm_campaign, transaction_ids) DO UPDATE SET
        sessions = EXCLUDED.sessions,
        users = EXCLUDED.users,
        conversions = EXCLUDED.conversions,
        purchase_revenue = EXCLUDED.purchase_revenue,
        transactions = EXCLUDED.transactions,
        updated_at = NOW();
    """
    values = [(
        r['date'], r.get('utm_source'), r.get('utm_medium'), r.get('utm_campaign'),
        r.get('sessions'), r.get('users'), r.get('conversions'),
        r.get('purchase_revenue'), r.get('transactions'), Json(r.get('transaction_ids'))
    ) for r in records]
    conn = get_connection()
    cur = conn.cursor()
    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    conn.close()


def insert_promo_order(promo_order: dict):
    """
    Вставляет запись о заказе с промокодом в таблицу promo_orders.
    
    Args:
        promo_order: словарь с данными о промокоде и заказе
    """
    sql = """
    INSERT INTO promo_orders(
        promo_code, order_id, transaction_id, order_date, order_amount, 
        restaurant, country, promo_source
    ) VALUES (
        %(promo_code)s, %(order_id)s, %(transaction_id)s, %(order_date)s, 
        %(order_amount)s, %(restaurant)s, %(country)s, %(promo_source)s
    ) ON CONFLICT (transaction_id) DO UPDATE SET
        promo_code = EXCLUDED.promo_code,
        order_id = EXCLUDED.order_id,
        order_date = EXCLUDED.order_date,
        order_amount = EXCLUDED.order_amount,
        restaurant = EXCLUDED.restaurant,
        country = EXCLUDED.country,
        promo_source = EXCLUDED.promo_source,
        updated_at = NOW();
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql, promo_order)
        conn.commit()
        logger.info(f"Inserted promo order with transaction_id: {promo_order.get('transaction_id')}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error inserting promo order: {e}")
        raise


def insert_deduplicated_metrics(deduplicated_data: List[Dict[str, Any]]) -> int:
    """
    Сохраняет дедуплицированные данные о транзакциях в базу данных.
    
    Args:
        deduplicated_data: Список словарей с дедуплицированными данными
        
    Returns:
        Количество вставленных записей
    """
    if not deduplicated_data:
        logger.warning("No deduplicated data to insert")
        return 0
    
    # Создаем таблицу для хранения дедуплицированных данных, если её еще нет
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Проверяем, существует ли таблица
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'deduplicated_transactions'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            # Создаем таблицу для хранения дедуплицированных данных
            cur.execute("""
                CREATE TABLE deduplicated_transactions (
                    id SERIAL PRIMARY KEY,
                    transaction_id TEXT NOT NULL,
                    transaction_date DATE,
                    utm_source TEXT,
                    utm_medium TEXT,
                    utm_campaign TEXT,
                    purchase_revenue NUMERIC,
                    is_promo_order BOOLEAN,
                    attribution_source TEXT,
                    match_type TEXT,
                    match_confidence NUMERIC,
                    promo_code TEXT,
                    promo_source TEXT,
                    fuzzy_matched_id TEXT,
                    processed_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (transaction_id)
                );
                
                CREATE INDEX idx_dedup_trans_date ON deduplicated_transactions(transaction_date);
                CREATE INDEX idx_dedup_attribution_source ON deduplicated_transactions(attribution_source);
                CREATE INDEX idx_dedup_is_promo_order ON deduplicated_transactions(is_promo_order);
            """)
            
            conn.commit()
            logger.info("Created deduplicated_transactions table")
        
        # Подготавливаем данные для вставки
        values = []
        for item in deduplicated_data:
            # Преобразуем дату из строки в объект date, если это строка
            transaction_date = item.get('date')
            if isinstance(transaction_date, str):
                try:
                    transaction_date = datetime.strptime(transaction_date, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    transaction_date = None
            
            # Извлекаем utm-параметры
            utm_parts = item.get('sourceMedium', '').split(' / ')
            utm_source = utm_parts[0] if len(utm_parts) > 0 else None
            utm_medium = utm_parts[1] if len(utm_parts) > 1 else None
            
            values.append((
                item.get('transaction_id'),
                transaction_date,
                utm_source,
                utm_medium,
                item.get('campaign'),
                item.get('purchase_revenue'),
                item.get('is_promo_order', False),
                item.get('attribution_source'),
                item.get('match_type'),
                item.get('match_confidence', 0.0),
                item.get('promo_code'),
                item.get('promo_source'),
                item.get('fuzzy_matched_id')
            ))
        
        # Выполняем вставку с обработкой дубликатов
        execute_values(
            cur,
            """
            INSERT INTO deduplicated_transactions (
                transaction_id, transaction_date, utm_source, utm_medium, utm_campaign,
                purchase_revenue, is_promo_order, attribution_source, match_type,
                match_confidence, promo_code, promo_source, fuzzy_matched_id
            ) VALUES %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                transaction_date = EXCLUDED.transaction_date,
                utm_source = EXCLUDED.utm_source,
                utm_medium = EXCLUDED.utm_medium,
                utm_campaign = EXCLUDED.utm_campaign,
                purchase_revenue = EXCLUDED.purchase_revenue,
                is_promo_order = EXCLUDED.is_promo_order,
                attribution_source = EXCLUDED.attribution_source,
                match_type = EXCLUDED.match_type,
                match_confidence = EXCLUDED.match_confidence,
                promo_code = EXCLUDED.promo_code,
                promo_source = EXCLUDED.promo_source,
                fuzzy_matched_id = EXCLUDED.fuzzy_matched_id,
                processed_at = NOW()
            """,
            values
        )
        
        conn.commit()
        inserted_count = len(values)
        logger.info(f"Inserted {inserted_count} deduplicated records")
        return inserted_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting deduplicated data: {e}")
        raise
    finally:
        conn.close()


def insert_appsflyer_metrics(data: List[Dict[str, Any]]) -> int:
    """
    Сохраняет трансформированные данные AppsFlyer в базу данных.
    
    Args:
        data: Список объектов с данными AppsFlyer
        
    Returns:
        Количество вставленных записей
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Проверяем, существует ли таблица, и создаем её если нет
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'appsflyer_metrics'
            );
        """)
        if not cur.fetchone()[0]:
            logger.info("Creating appsflyer_metrics table")
            cur.execute("""
                CREATE TABLE appsflyer_metrics (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    media_source TEXT NOT NULL,
                    campaign TEXT NOT NULL,
                    installs INTEGER DEFAULT 0,
                    clicks INTEGER DEFAULT 0,
                    impressions INTEGER DEFAULT 0,
                    cost NUMERIC(12,2) DEFAULT 0.0,
                    cost_per_install NUMERIC(12,2) DEFAULT 0.0,
                    purchases INTEGER DEFAULT 0,
                    revenue NUMERIC(12,2) DEFAULT 0.0,
                    platform TEXT,
                    country_code TEXT,
                    day_1_retention NUMERIC(5,2),
                    day_7_retention NUMERIC(5,2),
                    day_30_retention NUMERIC(5,2),
                    ltv_day_7 NUMERIC(12,2),
                    ltv_day_30 NUMERIC(12,2),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (date, media_source, campaign)
                );
                
                CREATE INDEX idx_appsflyer_date ON appsflyer_metrics(date);
                CREATE INDEX idx_appsflyer_media_source ON appsflyer_metrics(media_source);
                CREATE INDEX idx_appsflyer_campaign ON appsflyer_metrics(campaign);
            """)
            conn.commit()
            logger.info("Created appsflyer_metrics table")
        
        # Подготавливаем данные для вставки
        values = []
        for item in data:
            # Преобразуем дату из строки в объект date, если это строка
            date_value = item.get('date')
            if isinstance(date_value, str):
                try:
                    date_value = datetime.strptime(date_value, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    date_value = None
            
            values.append((
                date_value,
                item.get('media_source', ''),
                item.get('campaign', ''),
                item.get('installs', 0),
                item.get('clicks', 0),
                item.get('impressions', 0),
                item.get('cost', 0.0),
                item.get('cost_per_install', 0.0),
                item.get('purchases', 0),
                item.get('revenue', 0.0),
                item.get('platform', ''),
                item.get('country_code', ''),
                item.get('day_1_retention', None),
                item.get('day_7_retention', None),
                item.get('day_30_retention', None),
                item.get('ltv_day_7', None),
                item.get('ltv_day_30', None)
            ))
        
        # Выполняем вставку с обработкой дубликатов
        execute_values(
            cur,
            """
            INSERT INTO appsflyer_metrics (
                date, media_source, campaign, installs, clicks, impressions, cost,
                cost_per_install, purchases, revenue, platform, country_code,
                day_1_retention, day_7_retention, day_30_retention, 
                ltv_day_7, ltv_day_30
            ) VALUES %s
            ON CONFLICT (date, media_source, campaign) DO UPDATE SET
                installs = EXCLUDED.installs,
                clicks = EXCLUDED.clicks,
                impressions = EXCLUDED.impressions,
                cost = EXCLUDED.cost,
                cost_per_install = EXCLUDED.cost_per_install,
                purchases = EXCLUDED.purchases,
                revenue = EXCLUDED.revenue,
                platform = EXCLUDED.platform,
                country_code = EXCLUDED.country_code,
                day_1_retention = EXCLUDED.day_1_retention,
                day_7_retention = EXCLUDED.day_7_retention,
                day_30_retention = EXCLUDED.day_30_retention,
                ltv_day_7 = EXCLUDED.ltv_day_7,
                ltv_day_30 = EXCLUDED.ltv_day_30,
                updated_at = NOW()
            """,
            values
        )
        
        conn.commit()
        inserted_count = len(values)
        logger.info(f"Inserted {inserted_count} AppsFlyer metrics records")
        return inserted_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting AppsFlyer data: {e}")
        raise
    finally:
        cur.close()
        conn.close()
