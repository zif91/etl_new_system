# ETL Система Аналитики Рекламных Кампаний

Система ETL для агрегации и анализа данных рекламных кампаний из различных источников с возможностью сравнения с медиапланом.

## Основные возможности

- 📊 Импорт данных из Meta API, Google Analytics 4, Google Ads, Google Sheets, AppsFlyer
- 🔗 Дедупликация заказов между источниками
- 📈 Расчет рекламных метрик (CPM, CPC, CPA, CPO, ДРР)
- 📋 Сравнение план-факт с медиапланом
- 🎯 Многомерный анализ по различным срезам
- 📅 Анализ эффективности месяц к месяцу
- 🤖 Автоматизация через Apache Airflow

## Архитектура

- **Backend**: Python (FastAPI/Django)
- **База данных**: PostgreSQL
- **ETL**: Apache Airflow
- **Контейнеризация**: Docker
- **Визуализация**: Apache Superset / Grafana

## Структура проекта

```
├── dags/                    # Airflow DAGs
│   └── advertising_data_pipeline.py
├── src/                     # Исходный код модулей
│   ├── meta_importer.py
│   ├── ga4_importer.py
│   ├── google_ads_importer.py
│   ├── media_plan_matcher.py
│   ├── media_plan_integrator.py
│   ├── multi_dimensional_analyzer.py
│   ├── performance_analyzer.py
│   └── ...
├── tests/                   # Тесты
├── migrations/              # Миграции БД
├── docs/                    # Документация
├── data/                    # Данные и отчеты
└── requirements.txt         # Зависимости Python
```

## Установка и настройка

### 1. Клонирование репозитория

```bash
git clone git@github.com:zif91/etl_new_system.git
cd etl_new_system
```

### 2. Создание виртуального окружения

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# или
.venv\Scripts\activate     # Windows
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Настройка переменных окружения

Скопируйте `.env.example` в `.env` и заполните необходимые переменные:

```bash
cp .env.example .env
```

Переменные окружения:

- `DATABASE_URL` - строка подключения к PostgreSQL
- `META_APP_ID`, `META_APP_SECRET` - Meta API credentials
- `GOOGLE_APPLICATION_CREDENTIALS` - путь к JSON-файлу Google Service Account
- `GA4_PROPERTY_ID` - ID Google Analytics 4 property
- `GOOGLE_ADS_CUSTOMER_ID` - Customer ID Google Ads
- `APPSFLYER_API_TOKEN` - токен AppsFlyer API

### 5. Инициализация базы данных

```bash
# Запуск миграций
python -m src.db migrate
```

### 6. Запуск Airflow

```bash
# Инициализация Airflow
airflow db init

# Создание пользователя
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Запуск веб-сервера
airflow webserver --port 8080

# В отдельном терминале - запуск планировщика
airflow scheduler
```

## Использование

### Запуск ETL-пайплайна

Пайплайн запускается автоматически по расписанию (ежедневно в 6:00) или вручную через Airflow UI.

### Основные задачи

1. **Импорт данных** - загрузка данных из всех источников
2. **Дедупликация заказов** - устранение дублирования между источниками
3. **Расчет метрик** - вычисление рекламных показателей
4. **Сравнение с медиапланом** - план-факт анализ
5. **Многомерный анализ** - анализ по различным срезам
6. **Генерация отчетов** - создание аналитических отчетов

### Отчеты

Отчеты сохраняются в директории `data/comparisons/`:

- `comparison_{YEAR}_{MONTH}.json` - результаты сравнения с медиапланом
- `performance_comparison_{CURRENT}_vs_{PREVIOUS}.json` - сравнение месяц к месяцу

## Тестирование

Запуск всех тестов:

```bash
python -m unittest discover tests/
```

Запуск конкретного теста:

```bash
python -m unittest tests.test_media_plan_matcher
```

## Документация

Подробная документация доступна в директории `docs/`:

- [Модуль сравнения с медиапланом](docs/media_plan_comparison_module.md)
- [PRD системы](scripts/PRD.txt)

## Deployment

### Docker

```bash
# Сборка образа
docker build -t etl-analytics .

# Запуск с docker-compose
docker-compose up -d
```

### Production

Для развертывания на production сервере:

1. Клонирование репозитория
2. Настройка переменных окружения
3. Установка зависимостей
4. Инициализация базы данных
5. Настройка Airflow
6. Настройка cron-задач или systemd сервисов

## Лицензия

Проект разработан для внутреннего использования.

## Контакты

По вопросам разработки и поддержки обращайтесь к команде разработки.
