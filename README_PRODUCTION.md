# ETL System Tanuki Family - Production Ready

## 🎯 Production Status: DEPLOYED & READY

Эта система полностью готова для production использования. Все заглушки удалены, боевые API-ключи настроены, DAG'и используют реальные функции.

## 🔧 Production Configuration

### API Keys & Credentials (настроены)

- ✅ Facebook/Meta Business API
- ✅ Google Ads API (с refresh_token)
- ✅ Google Analytics 4 API
- ✅ Google Sheets API
- ✅ AppsFlyer API

### Database (готова)

- ✅ PostgreSQL с миграциями
- ✅ Все таблицы созданы
- ✅ Индексы для оптимизации

### Airflow (запущен)

- ✅ Docker-compose конфигурация
- ✅ DAG'и с реальными функциями
- ✅ Подключение к PostgreSQL

## 🚀 Deployment

### На сервере запущены:

```bash
# Все контейнеры активны
docker ps
# airflow-webserver, airflow-scheduler, postgres, redis, nginx

# Airflow UI доступен
http://your-server:8080
# admin / admin
```

### Статус DAG'ов:

- `advertising_data_pipeline` - основной production DAG
- Все импорты реальных функций работают
- Нет заглушек в коде

## 📊 Data Sources Configured

1. **Meta Ads**: Account IDs настроены в `config/accounts_config.py`
2. **Google Ads**: Customer IDs и refresh_token готовы
3. **GA4**: Property IDs настроены
4. **Google Sheets**: Промокоды и медиапланы
5. **AppsFlyer**: App IDs для мобильной аналитики

## 🔐 Security

- Все секреты в `.env.prod` (не в git)
- Credentials в `credentials/` (не в git)
- `.gitignore` защищает от случайных коммитов секретов

## 📁 Key Files

### Production DAGs

- `dags/advertising_data_pipeline.py` - основной ETL process

### Data Importers (все реальные, без заглушек)

- `src/meta_importer.py` - Facebook/Instagram данные
- `src/ga4_importer.py` - Google Analytics 4
- `src/google_ads_importer.py` - Google Ads метрики
- `src/appsflyer_importer.py` - мобильная аналитика
- `src/promo_importer.py` - промокоды из Sheets

### Data Processing

- `src/deduplication_process.py` - дедупликация заказов
- `src/metrics_calculator.py` - расчет KPI
- `src/report_generator.py` - генерация отчетов

### Configuration

- `config/accounts_config.py` - ID всех аккаунтов
- `.env.prod` - production переменные среды

## 🎯 Next Steps

1. Мониторить работу DAG'ов в Airflow UI
2. Проверить качество данных в PostgreSQL
3. Настроить алерты на ошибки в импорте
4. Добавить Grafana для мониторинга метрик

## 📞 Support

При проблемах проверьте:

- Логи Airflow: `docker logs etl_new_system-airflow-webserver-1`
- Статус контейнеров: `docker ps`
- Подключение к БД: `psql -h localhost -p 5432 -U airflow -d airflow`

---

**Status**: ✅ PRODUCTION READY - All stubs removed, real APIs configured
**Last Updated**: January 2025
**Version**: 1.0.0
