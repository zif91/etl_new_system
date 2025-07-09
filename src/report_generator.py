"""
Генерация отчетов на основе рассчитанных метрик и данных дедупликации.
"""

import os
import logging
import json
import smtplib
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from src.db import get_connection

logger = logging.getLogger(__name__)

def generate_reports_task(execution_date=None, **context):
    """
    Основная функция для генерации отчетов, интегрируется с Airflow.
    
    Args:
        execution_date: Дата выполнения в формате строки 'YYYY-MM-DD'
        context: Контекст выполнения Airflow (если используется)
        
    Returns:
        Словарь с информацией о сгенерированных отчетах
    """
    # Преобразуем строку в объект даты, если это строка
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    
    # Если дата не указана, используем вчерашнюю
    if not execution_date:
        execution_date = datetime.now().date() - timedelta(days=1)
        
    logger.info(f"Starting report generation for date: {execution_date}")
    
    # Создаем директории для отчетов
    reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')
    daily_dir = os.path.join(reports_dir, execution_date.strftime('%Y-%m-%d'))
    
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(daily_dir, exist_ok=True)
    
    reports = {}
    
    try:
        # 1. Генерация ежедневного отчета по метрикам
        metrics_report = generate_metrics_report(execution_date, os.path.join(daily_dir, 'metrics_report.csv'))
        reports['metrics'] = metrics_report
        
        # 2. Генерация отчета по дедупликации
        dedup_report = generate_deduplication_report(execution_date, os.path.join(daily_dir, 'deduplication_report.csv'))
        reports['deduplication'] = dedup_report
        
        # 3. Генерация отчета по сравнению с медиапланом
        mediaplan_report = generate_mediaplan_comparison(execution_date, os.path.join(daily_dir, 'mediaplan_comparison.csv'))
        reports['mediaplan'] = mediaplan_report
        
        # 4. Генерация сводного JSON-отчета
        summary = {
            'date': execution_date.isoformat(),
            'metrics': metrics_report['summary'],
            'deduplication': dedup_report['summary'],
            'mediaplan': mediaplan_report['summary']
        }
        
        with open(os.path.join(daily_dir, 'summary.json'), 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        # 5. Отправка отчета по email (если настроено)
        if os.environ.get('ENABLE_EMAIL_REPORTS', '').lower() == 'true':
            send_report_email(execution_date, daily_dir, summary)
            reports['email_sent'] = True
        
        logger.info(f"Report generation completed for {execution_date}")
        return {
            'date': execution_date.isoformat(),
            'reports_dir': daily_dir,
            'reports': reports
        }
        
    except Exception as e:
        logger.error(f"Error generating reports: {e}")
        raise

def generate_metrics_report(date, output_file):
    """
    Генерирует отчет по рассчитанным метрикам.
    
    Args:
        date: Дата для отчета
        output_file: Путь для сохранения отчета
        
    Returns:
        Информация о сгенерированном отчете
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Получаем метрики за указанную дату
        cur.execute("""
            SELECT 
                date,
                total_revenue,
                total_cost,
                total_orders,
                cpo,
                roas,
                drr,
                source_metrics,
                mobile_metrics
            FROM 
                calculated_metrics
            WHERE 
                date = %s
        """, (date,))
        
        row = cur.fetchone()
        
        if not row:
            logger.warning(f"No metrics data found for {date}")
            return {
                'status': 'empty',
                'message': f"No metrics data found for {date}",
                'summary': {}
            }
        
        # Колонки для CSV файла
        columns = ['date', 'total_revenue', 'total_cost', 'total_orders', 'cpo', 'roas', 'drr']
        metrics = dict(zip(columns, row[:-2]))  # Все, кроме source_metrics и mobile_metrics
        
        # Получаем данные по источникам и мобильным метрикам
        source_metrics = json.loads(row[-2]) if row[-2] else {}
        mobile_metrics = json.loads(row[-1]) if row[-1] else {}
        
        # Записываем в CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Заголовок
            writer.writerow(['Advertising Metrics Report', date])
            writer.writerow([])
            
            # Основные метрики
            writer.writerow(['Overall Metrics', ''])
            writer.writerow(['Date', 'Total Revenue', 'Total Cost', 'Orders', 'CPO', 'ROAS', 'DRR (%)'])
            writer.writerow([
                metrics['date'],
                f"{metrics['total_revenue']:.2f}",
                f"{metrics['total_cost']:.2f}",
                metrics['total_orders'],
                f"{metrics['cpo']:.2f}",
                f"{metrics['roas']:.2f}",
                f"{metrics['drr']:.2f}%"
            ])
            
            # Пустая строка
            writer.writerow([])
            
            # Метрики по источникам
            writer.writerow(['Source Breakdown', ''])
            writer.writerow(['Source', 'Revenue', 'Cost', 'Orders', 'CPO', 'ROAS', 'DRR (%)'])
            
            for source, data in source_metrics.items():
                writer.writerow([
                    source,
                    f"{data['revenue']:.2f}",
                    f"{data['cost']:.2f}",
                    data['orders'],
                    f"{data['cpo']:.2f}",
                    f"{data['roas']:.2f}",
                    f"{data['drr']:.2f}%"
                ])
            
            # Пустая строка
            writer.writerow([])
            
            # Мобильные метрики
            if mobile_metrics:
                writer.writerow(['Mobile App Metrics', ''])
                writer.writerow(['Total Installs', 'Total Sessions', 'Total Events', 'CPI', 'CPE'])
                writer.writerow([
                    mobile_metrics.get('total_installs', 0),
                    mobile_metrics.get('total_sessions', 0),
                    mobile_metrics.get('total_events', 0),
                    f"{mobile_metrics.get('cpi', 0):.2f}",
                    f"{mobile_metrics.get('cpe', 0):.2f}"
                ])
                
                # Метрики по кампаниям
                if 'by_campaign' in mobile_metrics and mobile_metrics['by_campaign']:
                    writer.writerow([])
                    writer.writerow(['Mobile Campaigns', ''])
                    writer.writerow(['Campaign', 'Installs', 'Sessions', 'Events', 'Cost', 'CPI', 'CPE'])
                    
                    for campaign, data in mobile_metrics['by_campaign'].items():
                        writer.writerow([
                            campaign,
                            data.get('installs', 0),
                            data.get('sessions', 0),
                            data.get('events', 0),
                            f"{data.get('cost', 0):.2f}",
                            f"{data.get('cpi', 0):.2f}",
                            f"{data.get('cpe', 0):.2f}"
                        ])
        
        logger.info(f"Generated metrics report: {output_file}")
        
        # Создаем краткую сводку для возврата
        mobile_summary = {}
        if mobile_metrics:
            mobile_summary = {
                'total_installs': mobile_metrics.get('total_installs', 0),
                'total_sessions': mobile_metrics.get('total_sessions', 0),
                'cpi': mobile_metrics.get('cpi', 0),
                'has_campaign_data': bool(mobile_metrics.get('by_campaign', {}))
            }
        
        return {
            'status': 'success',
            'file': output_file,
            'summary': {
                'total_revenue': metrics['total_revenue'],
                'total_cost': metrics['total_cost'],
                'total_orders': metrics['total_orders'],
                'cpo': metrics['cpo'],
                'roas': metrics['roas'],
                'drr': metrics['drr'],
                'sources_count': len(source_metrics),
                'mobile_metrics': mobile_summary
            }
        }
    
    except Exception as e:
        logger.error(f"Error generating metrics report: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def generate_deduplication_report(date, output_file):
    """
    Генерирует отчет по дедупликации заказов.
    
    Args:
        date: Дата для отчета
        output_file: Путь для сохранения отчета
        
    Returns:
        Информация о сгенерированном отчете
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Получаем статистику дедупликации
        cur.execute("""
            SELECT 
                transaction_date,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN is_promo_order = true THEN 1 ELSE 0 END) as promo_orders,
                SUM(CASE WHEN is_promo_order = false THEN 1 ELSE 0 END) as non_promo_orders,
                SUM(CASE WHEN attribution_source = 'exact_match' THEN 1 ELSE 0 END) as exact_matches,
                SUM(CASE WHEN attribution_source = 'fuzzy_match' THEN 1 ELSE 0 END) as fuzzy_matches,
                SUM(CASE WHEN attribution_source = 'criteria_match' THEN 1 ELSE 0 END) as criteria_matches,
                SUM(CASE WHEN attribution_source = 'no_match' THEN 1 ELSE 0 END) as no_matches,
                SUM(purchase_revenue) as total_revenue
            FROM 
                deduplicated_transactions
            WHERE 
                transaction_date = %s
            GROUP BY 
                transaction_date
        """, (date,))
        
        row = cur.fetchone()
        
        if not row:
            logger.warning(f"No deduplication data found for {date}")
            return {
                'status': 'empty',
                'message': f"No deduplication data found for {date}",
                'summary': {}
            }
            
        # Колонки и данные для основной статистики
        columns = [
            'transaction_date', 'total_transactions', 'promo_orders',
            'non_promo_orders', 'exact_matches', 'fuzzy_matches',
            'criteria_matches', 'no_matches', 'total_revenue'
        ]
        stats = dict(zip(columns, row))
        
        # Получаем статистику по источникам атрибуции
        cur.execute("""
            SELECT 
                utm_source,
                COUNT(*) as transactions,
                SUM(purchase_revenue) as revenue
            FROM 
                deduplicated_transactions
            WHERE 
                transaction_date = %s
            GROUP BY 
                utm_source
            ORDER BY 
                transactions DESC
        """, (date,))
        
        source_stats = [dict(zip(['utm_source', 'transactions', 'revenue'], row)) for row in cur.fetchall()]
        
        # Записываем отчет в CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Заголовок
            writer.writerow(['Deduplication Report', date.isoformat()])
            writer.writerow([])
            
            # Основная статистика
            writer.writerow(['Total Transactions', stats['total_transactions']])
            writer.writerow(['Promo Orders', stats['promo_orders']])
            writer.writerow(['Non-Promo Orders', stats['non_promo_orders']])
            writer.writerow(['Exact Matches', stats['exact_matches']])
            writer.writerow(['Fuzzy Matches', stats['fuzzy_matches']])
            writer.writerow(['Criteria Matches', stats['criteria_matches']])
            writer.writerow(['No Matches', stats['no_matches']])
            writer.writerow(['Total Revenue', f"{stats['total_revenue']:.2f}"])
            
            # Пустая строка
            writer.writerow([])
            
            # Статистика по источникам
            writer.writerow(['UTM Source', 'Transactions', 'Revenue'])
            
            for source in source_stats:
                if not source['utm_source']:
                    source_name = "(not set)"
                else:
                    source_name = source['utm_source']
                    
                writer.writerow([
                    source_name,
                    source['transactions'],
                    f"{source['revenue']:.2f}"
                ])
        
        logger.info(f"Generated deduplication report: {output_file}")
        
        return {
            'status': 'success',
            'file': output_file,
            'summary': {
                'total_transactions': stats['total_transactions'],
                'promo_orders': stats['promo_orders'],
                'non_promo_orders': stats['non_promo_orders'],
                'match_rate': (stats['exact_matches'] + stats['fuzzy_matches'] + stats['criteria_matches']) / stats['total_transactions'] if stats['total_transactions'] > 0 else 0,
                'total_revenue': stats['total_revenue']
            }
        }
    
    except Exception as e:
        logger.error(f"Error generating deduplication report: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def generate_mediaplan_comparison(date, output_file):
    """
    Генерирует отчет по сравнению с медиапланом.
    
    Args:
        date: Дата для отчета
        output_file: Путь для сохранения отчета
        
    Returns:
        Информация о сгенерированном отчете
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Проверяем наличие таблицы с медиапланами
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'media_plan'
            );
        """)
        
        if not cur.fetchone()[0]:
            logger.warning("Media plan table does not exist")
            return {
                'status': 'not_implemented',
                'message': "Media plan comparison not implemented yet",
                'summary': {}
            }
        
        # Получаем данные медиаплана на текущий месяц
        month_start = date.replace(day=1)
        next_month = month_start.replace(month=month_start.month + 1) if month_start.month < 12 else month_start.replace(year=month_start.year + 1, month=1)
        month_end = next_month - timedelta(days=1)
        
        # Запрос на получение данных медиаплана
        cur.execute("""
            SELECT 
                source,
                campaign_type,
                goal,
                restaurant,
                country,
                planned_budget,
                planned_impressions,
                planned_orders
            FROM 
                media_plan
            WHERE 
                month = %s
        """, (month_start,))
        
        plan_data = [dict(zip([
            'source', 'campaign_type', 'goal', 'restaurant', 'country',
            'planned_budget', 'planned_impressions', 'planned_orders'
        ], row)) for row in cur.fetchall()]
        
        if not plan_data:
            logger.warning(f"No media plan data found for {month_start}")
            return {
                'status': 'empty',
                'message': f"No media plan data found for {month_start}",
                'summary': {}
            }
        
        # Получаем фактические данные с начала месяца
        # Meta данные
        cur.execute("""
            SELECT 
                source,
                campaign_type,
                campaign_goal as goal,
                restaurant,
                city,
                SUM(spend) as actual_budget,
                SUM(impressions) as actual_impressions
            FROM 
                meta_daily_metrics
            WHERE 
                date >= %s AND date <= %s
            GROUP BY 
                source, campaign_type, campaign_goal, restaurant, city
        """, (month_start, date))
        
        meta_actuals = [dict(zip([
            'source', 'campaign_type', 'goal', 'restaurant', 'city',
            'actual_budget', 'actual_impressions'
        ], row)) for row in cur.fetchall()]
        
        # Google Ads данные
        cur.execute("""
            SELECT 
                source,
                campaign_type_determined as campaign_type,
                campaign_goal as goal,
                restaurant,
                city,
                SUM(cost) as actual_budget,
                SUM(impressions) as actual_impressions
            FROM 
                google_ads_metrics
            WHERE 
                date >= %s AND date <= %s
            GROUP BY 
                source, campaign_type_determined, campaign_goal, restaurant, city
        """, (month_start, date))
        
        google_ads_actuals = [dict(zip([
            'source', 'campaign_type', 'goal', 'restaurant', 'city',
            'actual_budget', 'actual_impressions'
        ], row)) for row in cur.fetchall()]
        
        # Объединяем данные
        actuals = meta_actuals + google_ads_actuals
        
        # Создаем сравнение плана и факта
        comparison = []
        total_plan_budget = 0
        total_actual_budget = 0
        total_variance = 0
        
        for plan in plan_data:
            # Находим соответствующие фактические данные
            matching_actuals = [
                a for a in actuals 
                if a['source'] == plan['source'] and 
                a['campaign_type'] == plan['campaign_type'] and
                a['goal'] == plan['goal'] and
                a['restaurant'] == plan['restaurant']
            ]
            
            actual_budget = sum(a['actual_budget'] for a in matching_actuals)
            actual_impressions = sum(a['actual_impressions'] for a in matching_actuals)
            
            # Рассчитываем отклонения
            budget_variance = actual_budget - plan['planned_budget']
            budget_variance_pct = (budget_variance / plan['planned_budget'] * 100) if plan['planned_budget'] > 0 else 0
            
            impressions_variance = actual_impressions - plan['planned_impressions']
            impressions_variance_pct = (impressions_variance / plan['planned_impressions'] * 100) if plan['planned_impressions'] > 0 else 0
            
            # Добавляем для итогов
            total_plan_budget += plan['planned_budget']
            total_actual_budget += actual_budget
            total_variance += budget_variance
            
            # Добавляем в результат
            comparison.append({
                'source': plan['source'],
                'campaign_type': plan['campaign_type'],
                'goal': plan['goal'],
                'restaurant': plan['restaurant'],
                'country': plan['country'],
                'planned_budget': plan['planned_budget'],
                'actual_budget': actual_budget,
                'budget_variance': budget_variance,
                'budget_variance_pct': budget_variance_pct,
                'planned_impressions': plan['planned_impressions'],
                'actual_impressions': actual_impressions,
                'impressions_variance': impressions_variance,
                'impressions_variance_pct': impressions_variance_pct
            })
        
        # Рассчитываем общее отклонение
        total_variance_pct = (total_variance / total_plan_budget * 100) if total_plan_budget > 0 else 0
        
        # Записываем отчет в CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Заголовок
            writer.writerow(['Media Plan Comparison', f"{month_start} to {date}"])
            writer.writerow(['Days elapsed', (date - month_start).days + 1])
            writer.writerow(['Days in month', (month_end - month_start).days + 1])
            writer.writerow(['Completion percentage', f"{((date - month_start).days + 1) / ((month_end - month_start).days + 1) * 100:.2f}%"])
            writer.writerow([])
            
            # Итоговые значения
            writer.writerow(['Total Plan Budget', f"{total_plan_budget:.2f}"])
            writer.writerow(['Total Actual Budget', f"{total_actual_budget:.2f}"])
            writer.writerow(['Total Variance', f"{total_variance:.2f}"])
            writer.writerow(['Total Variance %', f"{total_variance_pct:.2f}%"])
            writer.writerow([])
            
            # Детализация
            writer.writerow([
                'Source', 'Type', 'Goal', 'Restaurant', 'Country',
                'Plan Budget', 'Actual Budget', 'Budget Var', 'Budget Var %',
                'Plan Imp', 'Actual Imp', 'Imp Var', 'Imp Var %'
            ])
            
            for item in comparison:
                writer.writerow([
                    item['source'],
                    item['campaign_type'],
                    item['goal'],
                    item['restaurant'],
                    item['country'],
                    f"{item['planned_budget']:.2f}",
                    f"{item['actual_budget']:.2f}",
                    f"{item['budget_variance']:.2f}",
                    f"{item['budget_variance_pct']:.2f}%",
                    item['planned_impressions'],
                    item['actual_impressions'],
                    item['impressions_variance'],
                    f"{item['impressions_variance_pct']:.2f}%"
                ])
        
        logger.info(f"Generated media plan comparison report: {output_file}")
        
        return {
            'status': 'success',
            'file': output_file,
            'summary': {
                'month': month_start.strftime('%Y-%m'),
                'days_elapsed': (date - month_start).days + 1,
                'days_in_month': (month_end - month_start).days + 1,
                'completion_percentage': ((date - month_start).days + 1) / ((month_end - month_start).days + 1),
                'total_plan_budget': total_plan_budget,
                'total_actual_budget': total_actual_budget,
                'total_variance': total_variance,
                'total_variance_pct': total_variance_pct
            }
        }
    
    except Exception as e:
        logger.error(f"Error generating media plan comparison: {e}")
        # Для этого отчета возвращаем статус не реализовано при ошибке,
        # так как функциональность сравнения с медиапланом может быть не полностью готова
        return {
            'status': 'error',
            'message': str(e),
            'summary': {}
        }
    finally:
        cur.close()
        conn.close()

def send_report_email(date, reports_dir, summary):
    """
    Отправляет отчет по электронной почте.
    
    Args:
        date: Дата отчета
        reports_dir: Директория с отчетами
        summary: Сводная информация для тела письма
        
    Returns:
        True, если отчет успешно отправлен
    """
    try:
        # Проверяем настройки для отправки писем
        smtp_server = os.environ.get('SMTP_SERVER')
        smtp_port = int(os.environ.get('SMTP_PORT', 587))
        smtp_user = os.environ.get('SMTP_USER')
        smtp_password = os.environ.get('SMTP_PASSWORD')
        from_email = os.environ.get('REPORT_FROM_EMAIL')
        to_emails = os.environ.get('REPORT_TO_EMAILS', '').split(',')
        
        if not (smtp_server and smtp_user and smtp_password and from_email and to_emails):
            logger.warning("Email settings not configured, skipping report email")
            return False
        
        # Создаем письмо
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = ", ".join(to_emails)
        msg['Subject'] = f"Advertising Analytics Report - {date}"
        
        # Тело письма
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .metric {{ font-weight: bold; }}
                .section-header {{ background-color: #4CAF50; color: white; padding: 10px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <h1>Advertising Analytics Report</h1>
            <h2>{date}</h2>
            
            <div class="section-header">Key Metrics Summary</div>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td class="metric">Total Revenue</td>
                    <td>{summary.get('metrics', {}).get('total_revenue', 0):.2f}</td>
                </tr>
                <tr>
                    <td class="metric">Total Cost</td>
                    <td>{summary.get('metrics', {}).get('total_cost', 0):.2f}</td>
                </tr>
                <tr>
                    <td class="metric">Total Orders</td>
                    <td>{summary.get('metrics', {}).get('total_orders', 0)}</td>
                </tr>
                <tr>
                    <td class="metric">CPO</td>
                    <td>{summary.get('metrics', {}).get('cpo', 0):.2f}</td>
                </tr>
                <tr>
                    <td class="metric">ROAS</td>
                    <td>{summary.get('metrics', {}).get('roas', 0):.2f}</td>
                </tr>
                <tr>
                    <td class="metric">DRR</td>
                    <td>{summary.get('metrics', {}).get('drr', 0):.2f}%</td>
                </tr>
            </table>
            
            <div class="section-header">Mobile App Metrics</div>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td class="metric">Total Installs</td>
                    <td>{summary.get('metrics', {}).get('mobile_metrics', {}).get('total_installs', 0)}</td>
                </tr>
                <tr>
                    <td class="metric">Total Sessions</td>
                    <td>{summary.get('metrics', {}).get('mobile_metrics', {}).get('total_sessions', 0)}</td>
                </tr>
                <tr>
                    <td class="metric">CPI (Cost Per Install)</td>
                    <td>{summary.get('metrics', {}).get('mobile_metrics', {}).get('cpi', 0):.2f}</td>
                </tr>
            </table>
            
            <div class="section-header">Deduplication Summary</div>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td class="metric">Total Transactions</td>
                    <td>{summary.get('deduplication', {}).get('total_transactions', 0)}</td>
                </tr>
                <tr>
                    <td class="metric">Promo Orders</td>
                    <td>{summary.get('deduplication', {}).get('promo_orders', 0)}</td>
                </tr>
                <tr>
                    <td class="metric">Non-Promo Orders</td>
                    <td>{summary.get('deduplication', {}).get('non_promo_orders', 0)}</td>
                </tr>
                <tr>
                    <td class="metric">Match Rate</td>
                    <td>{summary.get('deduplication', {}).get('match_rate', 0)*100:.2f}%</td>
                </tr>
            </table>
            
            <div class="section-header">Media Plan Comparison</div>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td class="metric">Month</td>
                    <td>{summary.get('mediaplan', {}).get('month', 'N/A')}</td>
                </tr>
                <tr>
                    <td class="metric">Budget Plan</td>
                    <td>{summary.get('mediaplan', {}).get('total_plan_budget', 0):.2f}</td>
                </tr>
                <tr>
                    <td class="metric">Budget Actual</td>
                    <td>{summary.get('mediaplan', {}).get('total_actual_budget', 0):.2f}</td>
                </tr>
                <tr>
                    <td class="metric">Budget Variance</td>
                    <td>{summary.get('mediaplan', {}).get('total_variance', 0):.2f} ({summary.get('mediaplan', {}).get('total_variance_pct', 0):.2f}%)</td>
                </tr>
                <tr>
                    <td class="metric">Month Progress</td>
                    <td>{summary.get('mediaplan', {}).get('completion_percentage', 0)*100:.2f}%</td>
                </tr>
            </table>
            
            <p>Full reports are attached to this email.</p>
            
            <p>This is an automated report.</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        # Прикрепляем файлы отчетов
        for filename in os.listdir(reports_dir):
            if filename.endswith('.csv') or filename.endswith('.json'):
                filepath = os.path.join(reports_dir, filename)
                
                attachment = MIMEBase('application', 'octet-stream')
                with open(filepath, 'rb') as file:
                    attachment.set_payload(file.read())
                
                encoders.encode_base64(attachment)
                attachment.add_header(
                    'Content-Disposition',
                    f'attachment; filename="{filename}"'
                )
                
                msg.attach(attachment)
        
        # Отправляем письмо
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"Report email sent to {', '.join(to_emails)}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending report email: {e}")
        return False


if __name__ == "__main__":
    # При запуске как самостоятельного скрипта
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate advertising reports")
    parser.add_argument("--date", help="Date to generate reports for (YYYY-MM-DD)")
    args = parser.parse_args()
    
    execution_date = None
    if args.date:
        execution_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    
    generate_reports_task(execution_date)
