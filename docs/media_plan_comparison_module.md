# Документация по модулю сравнения эффективности рекламных кампаний

## Обзор компонентов

Модуль сравнения эффективности рекламных кампаний включает в себя следующие компоненты:

1. **MediaPlanMatcher** - класс для сопоставления кампаний с медиапланом

   - Точное сопоставление
   - Нечеткое (fuzzy) сопоставление
   - Ручные сопоставления
   - Обработка неоднозначных совпадений
   - Расчет отклонений

2. **MediaPlanIntegrator** - функциональность для интеграции сопоставления в ETL-процесс

   - Загрузка данных медиаплана
   - Загрузка ручных сопоставлений
   - Получение метрик кампаний из БД
   - Сохранение результатов сравнения

3. **MultiDimensionalAnalyzer** - многомерный анализ результатов сравнения

   - Агрегация по различным срезам (источник, тип кампании, страна, ресторан)
   - Расчет отклонений в агрегированных данных
   - Сохранение результатов в БД

4. **PerformanceAnalyzer** - анализ эффективности месяц к месяцу
   - Сравнение метрик с предыдущим месяцем
   - Группировка по различным измерениям
   - Статистический анализ значимости изменений
   - Генерация сводной информации

## Интеграция в DAG

Модули интегрированы в основной DAG (`advertising_data_pipeline`):

1. `compare_with_media_plan` - оператор для сравнения фактических данных с медиапланом
2. `analyze_monthly_performance` - оператор для анализа эффективности месяц к месяцу
3. `run_multi_dimensional_analysis` - оператор для многомерного анализа план-факт

## Технические детали

### Сопоставление кампаний с медиапланом

Сопоставление происходит по следующему алгоритму:

1. Сначала проверяются ручные сопоставления (по идентификатору кампании)
2. Если ручное сопоставление не найдено, выполняется точное сопоставление по ключу (месяц, ресторан, страна, тип кампании, цель, источник)
3. Если точное сопоставление не найдено, выполняется нечеткое сопоставление с системой "очков" для оценки качества совпадения
4. При наличии нескольких подходящих совпадений (неоднозначность), выбирается наиболее подходящее по бюджету

### Расчет отклонений

Расчет отклонений включает:

1. Абсолютное отклонение = факт - план
2. Относительное отклонение (%) = (факт - план) / план \* 100

Расчет производится как для базовых метрик (spend, impressions, clicks, orders, revenue), так и для производных метрик (CPM, CPC, CPA, CPO, DRR).

### Многомерный анализ

Многомерный анализ выполняет агрегацию данных по различным срезам (источник, тип кампании, страна, ресторан) и рассчитывает отклонения для агрегированных данных.

### Анализ месяц к месяцу

Анализ месяц к месяцу выполняет сравнение метрик текущего месяца с предыдущим месяцем, рассчитывает отклонения и выполняет статистический анализ значимости изменений.

## Схема данных

### Таблица media_plan_comparison

Таблица для хранения результатов сопоставления с медиапланом:

```sql
CREATE TABLE media_plan_comparison (
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
```

### Таблица multi_dimensional_analysis

Таблица для хранения результатов многомерного анализа:

```sql
CREATE TABLE multi_dimensional_analysis (
    id SERIAL PRIMARY KEY,
    analysis_date DATE NOT NULL,
    dimension_name VARCHAR(255) NOT NULL,
    dimension_value VARCHAR(255) NOT NULL,
    variances JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## Формат файлов отчетов

### Отчет о сопоставлении с медиапланом

Файл: `data/comparisons/comparison_{YEAR}_{MONTH}.json`

```json
[
  {
    "campaign_date": "2025-07-15",
    "restaurant": "Тануки",
    "country": "Казахстан",
    "campaign_type": "Performance",
    "campaign_goal": "Заказы",
    "source": "Google search",
    "campaign_name": "Search|CPC|Almaty|Tanuki|No_Brand|Keywords",
    "matched": true,
    "is_manual": false,
    "is_fuzzy": false,
    "is_ambiguous": false,
    "media_plan_id": 1,
    "variances": {
      "spend": {
        "fact": 9500.0,
        "plan": 10000.0,
        "absolute_variance": -500.0,
        "relative_variance_percent": -5.0
      },
      "impressions": {
        "fact": 480000,
        "plan": 500000,
        "absolute_variance": -20000,
        "relative_variance_percent": -4.0
      },
      "clicks": {
        "fact": 9800,
        "plan": 10000,
        "absolute_variance": -200,
        "relative_variance_percent": -2.0
      },
      "cpm": {
        "fact": 19.79,
        "plan": 20.0,
        "absolute_variance": -0.21,
        "relative_variance_percent": -1.05
      }
    }
  }
]
```

### Отчет о сравнении месяц к месяцу

Файл: `data/comparisons/performance_comparison_{CURRENT_MONTH}_vs_{PREVIOUS_MONTH}.json`

```json
{
  "status": "success",
  "current_month": "2025-07",
  "previous_month": "2025-06",
  "dimensions": ["restaurant", "country", "campaign_type", "source"],
  "metrics": [
    "spend",
    "impressions",
    "clicks",
    "orders",
    "revenue",
    "cpm",
    "cpc",
    "cpo",
    "drr"
  ],
  "results": [
    {
      "restaurant": "Тануки",
      "country": "Казахстан",
      "campaign_type": "Performance",
      "source": "Google search",
      "metrics": {
        "spend": {
          "current": 9500.0,
          "previous": 8500.0,
          "absolute_variance": 1000.0,
          "relative_variance_percent": 11.76
        }
      },
      "statistical_significance": {
        "spend": {
          "p_value": 0.023,
          "significant": true,
          "t_statistic": 2.81
        }
      }
    }
  ],
  "summary": {
    "overall": {
      "total_categories": 10,
      "improved_metrics_count": 12,
      "worsened_metrics_count": 5
    },
    "by_metric": {
      "spend": {
        "current_average": 9500.0,
        "previous_average": 8500.0,
        "absolute_variance_average": 1000.0,
        "relative_variance_percent_average": 11.76,
        "significant_changes": true
      }
    }
  }
}
```

## Инструкция по использованию

### 1. Сопоставление с медиапланом

Для запуска сопоставления с медиапланом используйте задачу `compare_with_media_plan_task`:

```python
from src.media_plan_integrator import compare_with_media_plan_task

# Запуск задачи для указанной даты
compare_with_media_plan_task(execution_date="2025-07-15")
```

### 2. Многомерный анализ

Для запуска многомерного анализа используйте задачу `multi_dimensional_analysis_task`:

```python
from src.multi_dimensional_analyzer import multi_dimensional_analysis_task

# Запуск задачи для указанной даты
multi_dimensional_analysis_task(execution_date_str="2025-07-15")
```

### 3. Анализ месяц к месяцу

Для запуска анализа месяц к месяцу используйте задачу `compare_month_to_month_task`:

```python
from src.performance_analyzer import compare_month_to_month_task

# Запуск задачи для указанного месяца
compare_month_to_month_task(
    current_month="2025-07",
    previous_month="2025-06",
    dimensions=["restaurant", "country", "campaign_type", "source"],
    metrics=["spend", "impressions", "clicks", "orders", "revenue", "cpm", "cpc", "cpo", "drr"],
    output_path="data/comparisons"
)
```

### 4. Ручное сопоставление

Для добавления ручных сопоставлений создайте или обновите файл `data/manual_mappings.json`:

```json
{
  "(\"2025-07-01\", \"Белла\", \"Казахстан\", \"Awareness\", \"Охват/Узнаваемость\", \"Мета\", \"Instagram | CPM | Almaty | Bella | Interests | День Рождения\")": 2
}
```

## Заключение

Модуль сравнения эффективности рекламных кампаний с медиапланом обеспечивает полный цикл анализа от сопоставления кампаний с медиапланом до многомерного анализа и сравнения месяц к месяцу. Модуль включает в себя точное и нечеткое сопоставление, ручные сопоставления, расчет отклонений и статистический анализ значимости изменений.
