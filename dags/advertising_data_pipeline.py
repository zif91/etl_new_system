"""
Основной DAG для ETL-процесса аналитики рекламных кампаний.

Этот DAG выполняет следующие шаги:
1. Импорт данных из различных источников (Meta, GA4, Google Ads, Google Sheets, AppsFlyer)
2. Дедупликация заказов
3. Расчет метрик
4. Финальная трансформация данных
5. Генерация отчетов и уведомлений
6. Сравнение с медиапланом

DAG запускается ежедневно в 6:00 утра.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.helpers import chain
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator

# Импорт функций для обработки данных
import sys
import os

# Добавляем путь к проекту в sys.path для корректного импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.meta_importer import import_meta_insights
from src.ga4_importer import import_and_store_ga4_metrics
from src.google_ads_importer import import_and_store_google_ads_metrics
from src.promo_importer import import_promo_codes
from src.appsflyer_importer import import_and_store_appsflyer_data
from src.deduplication_process import deduplicate_orders_task
from src.metrics_calculator import calculate_metrics_task
from src.report_generator import generate_reports_task
from src.media_plan_importer import import_media_plan_task
from src.media_plan_integrator import compare_with_media_plan_task
from src.performance_analyzer import compare_month_to_month_task
from src.multi_dimensional_analyzer import multi_dimensional_analysis_task

# Параметры по умолчанию для задач
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['analytics-alerts@example.com'],
    'execution_timeout': timedelta(hours=2),  # Timeout для выполнения задач
    'sla': timedelta(hours=4)  # SLA для всего DAG
}

def on_failure_callback(context):
    """
    Функция вызывается при ошибке в задаче.
    Отправляет уведомление с деталями ошибки.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    subject = f"⚠️ Airflow alert: {task_id} failed in {dag_id}"
    
    body = f"""
    <h2 style="color: red;">Task Failure Alert</h2>
    <p>Task <b>{task_id}</b> in DAG <b>{dag_id}</b> failed on {execution_date}.</p>
    
    <h3>Error details:</h3>
    <pre style="background-color: #f8f8f8; padding: 10px; border-radius: 5px;">
    {exception}
    </pre>
    
    <h3>Task details:</h3>
    <ul>
        <li><b>Execution date:</b> {execution_date}</li>
        <li><b>Task instance:</b> {context.get('task_instance')}</li>
        <li><b>DAG run:</b> {context.get('dag_run')}</li>
    </ul>
    
    <p>Please check the Airflow logs for more details.</p>
    """
    
    send_email(
        to=default_args['email'],
        subject=subject,
        html_content=body
    )

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Функция вызывается при нарушении SLA задачи.
    """
    subject = f"🕒 Airflow SLA missed: {dag.dag_id}"
    
    html_content = f"""
    <h2 style="color: orange;">SLA Miss Alert</h2>
    <p>SLA was missed for DAG <b>{dag.dag_id}</b>.</p>
    
    <h3>Tasks that missed SLA:</h3>
    <ul>
    """
    
    for task in task_list:
        html_content += f"<li>{task}</li>"
    
    html_content += """
    </ul>
    
    <p>This could indicate performance issues or bottlenecks in the pipeline.</p>
    """
    
    send_email(
        to=default_args['email'],
        subject=subject,
        html_content=html_content
    )

# Создаем DAG
dag = DAG(
    'advertising_data_pipeline',
    default_args=default_args,
    description='Импорт и обработка данных рекламных кампаний из всех источников',
    schedule_interval='0 6 * * *',  # Каждый день в 6:00
    catchup=False,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['advertising', 'etl', 'analytics'],
    doc_md="""
    # Advertising Data Pipeline
    
    This DAG processes advertising data from multiple sources, including:
    
    * Meta (Facebook/Instagram)
    * Google Analytics 4
    * Google Ads
    * Promo codes from Google Sheets
    * AppsFlyer mobile analytics
    
    ## Pipeline Steps
    
    1. Import data from all sources (parallel)
    2. Deduplicate orders
    3. Calculate metrics
    4. Generate reports
    5. Send notifications
    
    ## Schedule
    
    Runs daily at 6:00 AM.
    """
)

# Создаем группу задач для импорта данных
with TaskGroup(group_id="data_import", dag=dag) as import_group:
    start_import = DummyOperator(
        task_id="start_import",
        dag=dag,
        doc_md="Starting point for data import tasks"
    )
    
    # 1. Meta (Facebook/Instagram) API
    import_meta = PythonOperator(
        task_id='import_meta_data',
        python_callable=import_meta_insights,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'},  # Передаем дату запуска
        dag=dag,
        sla=timedelta(hours=1),  # SLA для задачи
        doc_md="""
        ### Импорт данных из Meta (Facebook и Instagram)
        
        Извлекает данные из Meta Marketing API:
        - Показы, клики, расходы
        - Кампании, группы объявлений, объявления
        - UTM-метки
        
        Сохраняет данные в таблицы БД с разбивкой по дням.
        """
    )
    
    # 2. Google Analytics 4
    import_ga4 = PythonOperator(
        task_id='import_ga4_data',
        python_callable=import_and_store_ga4_metrics,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'},
        dag=dag,
        sla=timedelta(hours=1),  # SLA для задачи
        doc_md="""
        ### Импорт данных из Google Analytics 4
        
        Извлекает данные из Google Analytics Data API:
        - Сессии, пользователи
        - Конверсии, транзакции
        - Источники трафика и UTM-метки
        
        Сохраняет данные в таблицы БД для дальнейшей дедупликации.
        """
    )
    
    # 3. Google Ads
    import_google_ads = PythonOperator(
        task_id='import_google_ads_data',
        python_callable=import_and_store_google_ads_metrics,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'},
        dag=dag,
        sla=timedelta(hours=1),  # SLA для задачи
        doc_md="""
        ### Импорт данных из Google Ads
        
        Извлекает данные из Google Ads API:
        - Показы, клики, расходы
        - Конверсии
        - Структура кампаний
        
        Сохраняет данные в таблицы БД с разбивкой по дням.
        """
    )
    
    # 4. Google Sheets (Промокоды)
    import_promo = PythonOperator(
        task_id='import_promo_codes',
        python_callable=import_promo_codes,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ds }}'},
        dag=dag,
        sla=timedelta(hours=1),  # SLA для задачи
        doc_md="""
        ### Импорт данных о промокодах из Google Sheets
        
        Извлекает данные из таблиц Google Sheets:
        - Промокоды
        - Номера заказов
        - Источники промокодов
        
        Сохраняет данные в таблицы БД для дальнейшей дедупликации.
        """
    )
    
    # 5. AppsFlyer
    import_appsflyer = PythonOperator(
        task_id='import_appsflyer_data',
        python_callable=import_and_store_appsflyer_data,
        provide_context=True,
        op_kwargs={
            'start_date': '{{ ds }}',
            'end_date': '{{ ds }}',
            'include_retention': True,
            'include_ltv': True
        },
        dag=dag,
        sla=timedelta(hours=1),  # SLA для задачи
        doc_md="""
        ### Импорт данных из AppsFlyer
        
        Извлекает данные из AppsFlyer Reporting API:
        - Установки приложений
        - In-app события и покупки
        - Показатели удержания и LTV
        
        Сохраняет данные в таблицу appsflyer_metrics.
        """
    )
    
    end_import = DummyOperator(
        task_id="end_import",
        dag=dag,
        trigger_rule='all_done',  # Выполняется, когда все предыдущие задачи завершены (даже с ошибкой)
        doc_md="End point for data import tasks"
    )
    
    # Определяем последовательность выполнения задач в группе
    start_import >> [import_meta, import_ga4, import_google_ads, import_promo, import_appsflyer] >> end_import

# 6. Дедупликация заказов
deduplicate = PythonOperator(
    task_id='deduplicate_orders',
    python_callable=deduplicate_orders_task,
    provide_context=True,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
    sla=timedelta(hours=1),  # SLA для задачи
    doc_md="""
    ### Дедупликация заказов
    
    Выполняет дедупликацию заказов между различными источниками:
    - Сопоставляет заказы по transaction_id
    - Применяет нечеткое сопоставление
    - Разрешает конфликты атрибуции
    
    Сохраняет дедуплицированные заказы в таблицу deduplicated_transactions.
    """
)

# 7. Расчет метрик
calculate_metrics = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics_task,
    provide_context=True,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
    sla=timedelta(minutes=30),  # SLA для задачи
    doc_md="""
    ### Расчет рекламных метрик
    
    Рассчитывает основные метрики для анализа:
    - CPO (Cost Per Order)
    - ROI / ROAS
    - CPA (Cost Per Action)
    - ДРР (Доля Рекламных Расходов)
    - Мобильные метрики (CPI, CPE, и др.)
    
    Сохраняет рассчитанные метрики в таблицу calculated_metrics.
    """
)

# 8. Генерация отчетов
generate_reports = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports_task,
    provide_context=True,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
    sla=timedelta(minutes=30),  # SLA для задачи
    doc_md="""
    ### Генерация отчетов
    
    Генерирует отчеты на основе обработанных данных:
    - Ежедневные сводки
    - Отчет по дедупликации
    - Сравнение план-факт
    - Мобильная аналитика
    
    Сохраняет отчеты в директорию reports и/или отправляет их по email.
    """
)

# 9. Импорт медиаплана
import_media_plan = PythonOperator(
    task_id='import_media_plan',
    python_callable=import_media_plan_task,
    provide_context=True,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
    sla=timedelta(minutes=30),  # SLA для задачи
    doc_md="""
    ### Импорт данных медиаплана
    
    Импортирует данные медиаплана из Google Sheets:
    - Плановые бюджеты
    - Целевые метрики (показы, клики, заказы)
    - Разбивка по ресторанам, странам, типам кампаний
    
    Сохраняет данные медиаплана в формате JSON для дальнейшего сравнения.
    """
)

# 10. Сравнение с медиапланом
compare_with_media_plan = PythonOperator(
    task_id='compare_with_media_plan',
    python_callable=compare_with_media_plan_task,
    provide_context=True,
    op_kwargs={
        'execution_date': '{{ ds }}',
        'media_plan_path': "{{ ti.xcom_pull(task_ids='import_media_plan') }}"
    },
    dag=dag,
    sla=timedelta(minutes=30),  # SLA для задачи
    doc_md="""
    ### Сравнение с медиапланом
    
    Выполняет сравнение фактических данных кампаний с медиапланом:
    - Сопоставляет кампании с записями медиаплана
    - Рассчитывает абсолютные и относительные отклонения
    - Генерирует отчет о выполнении плана
    
    Сохраняет результаты сравнения в базу данных и в JSON-файл.
    """
)

# 10.5 Анализ эффективности месяц к месяцу
analyze_monthly_performance = PythonOperator(
    task_id='analyze_monthly_performance',
    python_callable=compare_month_to_month_task,
    provide_context=True,
    op_kwargs={
        'execution_date': '{{ ds }}',
        'metrics': ['CPM', 'CPC', 'CPA', 'CPO', 'DRR', 'budget'],
        'dimensions': ['channel', 'audience', 'creative_type']
    },
    dag=dag,
    sla=timedelta(minutes=30),  # SLA для задачи
    doc_md="""
    ### Анализ эффективности месяц к месяцу
    
    Выполняет сравнительный анализ эффективности рекламных кампаний:
    - Сравнивает ключевые метрики с предыдущим месяцем
    - Анализирует изменения по различным измерениям
    - Определяет статистически значимые отклонения
    - Генерирует сводный отчет по изменениям
    
    Сохраняет результаты анализа в директорию data/comparisons/.
    """
)

# 10.6 Многомерный анализ план-факт
run_multi_dimensional_analysis = PythonOperator(
    task_id='run_multi_dimensional_analysis',
    python_callable=multi_dimensional_analysis_task,
    provide_context=True,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
    sla=timedelta(minutes=20),
    doc_md="""
    ### Многомерный анализ план-факт
    
    Выполняет агрегацию и анализ результатов сравнения с медиапланом по различным срезам:
    - по каналу
    - по стране
    - по типу кампании
    
    Сохраняет агрегированные результаты в таблицу `multi_dimensional_analysis`.
    """
)


# 11. Уведомление об успешном выполнении
success_notification = EmailOperator(
    task_id='success_notification',
    to=default_args['email'],
    subject='✅ Advertising Data Pipeline completed successfully | {{ ds }}',
    html_content="""
    <h2 style="color: green;">Pipeline Completed Successfully</h2>
    <p>The advertising data pipeline for {{ ds }} has completed successfully.</p>
    
    <h3>Key Performance Indicators:</h3>
    <ul>
        <li>Data imported from all sources</li>
        <li>Orders deduplicated</li>
        <li>Metrics calculated</li>
        <li>Reports generated and sent</li>
        <li>Media plan comparison completed</li>
    </ul>
    
    <p>Please check the reports in the specified directory or your email inbox.</p>
    """,
    dag=dag
)

# Определение зависимостей между задачами

# Основной поток: импорт -> дедупликация -> расчет метрик -> генерация отчетов
chain(
    import_group,
    deduplicate,
    calculate_metrics,
    generate_reports
)

# После генерации отчетов запускаем две параллельные ветки:
# 1. Анализ эффективности месяц к месяцу
# 2. Импорт медиаплана и сравнение с ним
generate_reports >> [analyze_monthly_performance, import_media_plan]

# Сравнение с медиапланом зависит от его импорта
import_media_plan >> compare_with_media_plan

# Многомерный анализ зависит от сравнения с медиапланом
compare_with_media_plan >> run_multi_dimensional_analysis

# Уведомление об успехе отправляется после завершения обеих веток
chain([run_multi_dimensional_analysis, analyze_monthly_performance], success_notification)
