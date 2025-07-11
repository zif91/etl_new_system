# 🖥️ Требования к серверу для ETL системы аналитики рекламных кампаний

## 📊 Анализ нагрузки системы

### Компоненты системы:

1. **Apache Airflow** (планировщик + веб-сервер)
2. **Python ETL процессы** (импорт, обработка, анализ данных)
3. **PostgreSQL** (база данных)
4. **Redis** (кэширование, очереди)
5. **Nginx** (обратный прокси)

### Объем обрабатываемых данных (оценка):

- **Ежедневный импорт**: ~50,000-100,000 записей кампаний
- **Промокоды**: ~1,000-5,000 записей в день
- **GA4 данные**: ~10,000-50,000 сессий в день
- **Расчетные метрики**: ~20,000-100,000 вычислений в день

---

## 💻 МИНИМАЛЬНЫЕ требования к серверу

### 🏁 Базовая конфигурация (тестовое окружение)

```
CPU: 2 vCPU (2.4 GHz)
RAM: 4 GB
SSD: 40 GB
Network: 100 Mbps
OS: Ubuntu 20.04 LTS или новее
```

**Примерная стоимость**: \$20-30/месяц на VPS

### 🚀 Рекомендуемая конфигурация (production)

```
CPU: 4 vCPU (2.4 GHz+)
RAM: 8 GB
SSD: 100 GB
Network: 1 Gbps
OS: Ubuntu 20.04 LTS или новее
```

**Примерная стоимость**: \$40-60/месяц на VPS

### ⚡ Оптимальная конфигурация (высокие нагрузки)

```
CPU: 8 vCPU (3.0 GHz+)
RAM: 16 GB
SSD: 200 GB
Network: 1 Gbps
OS: Ubuntu 20.04 LTS или новее
```

**Примерная стоимость**: \$80-120/месяц на VPS

---

## 📈 Обоснование ресурсов

### 🧠 CPU (Процессор)

- **Apache Airflow**: 0.5-1 vCPU для планировщика и веб-сервера
- **Python ETL процессы**: 1-2 vCPU для параллельной обработки данных
- **PostgreSQL**: 0.5-1 vCPU для запросов и индексирование
- **Остальные сервисы**: 0.5 vCPU (Nginx, Redis, система)

**Минимум**: 2 vCPU | **Рекомендуется**: 4 vCPU

### 🧠 RAM (Оперативная память)

- **Apache Airflow**: 1-2 GB (планировщик + веб-сервер + worker'ы)
- **Python процессы**: 1-3 GB (pandas, обработка данных)
- **PostgreSQL**: 1-2 GB (буферы, кэши запросов)
- **Redis**: 256-512 MB
- **Система + Docker**: 1 GB

**Минимум**: 4 GB | **Рекомендуется**: 8 GB

### 💾 Диск (SSD)

- **Операционная система**: 10 GB
- **Docker образы**: 5-10 GB
- **PostgreSQL данные**: 5-20 GB (растет со временем)
- **Логи Airflow**: 2-5 GB
- **Временные файлы**: 5-10 GB
- **Резерв**: 10-20 GB

**Минимум**: 40 GB | **Рекомендуется**: 100 GB

### 🌐 Сеть

- **API запросы**: Стабильное соединение для внешних API
- **Загрузка данных**: 10-50 MB в день
- **Веб-интерфейс**: Минимальные требования

**Минимум**: 100 Mbps | **Рекомендуется**: 1 Gbps

---

## ⚖️ Специфика Apache Airflow + Python

### Apache Airflow требования:

- **Memory**: 1-2 GB для стабильной работы
- **CPU**: Минимум 1 vCPU, лучше 2+ для параллельных задач
- **Storage**: 5-10 GB для метаданных и логов

### Python ETL процессы:

- **Memory**: 1-3 GB для pandas, numpy операций
- **CPU**: 1-2 vCPU для параллельной обработки
- **I/O**: SSD обязательно для быстрой обработки данных

### PostgreSQL:

- **Memory**: 1-2 GB для буферов и кэшей
- **CPU**: 0.5-1 vCPU для запросов
- **Storage**: SSD для быстрых запросов, IOPS важны

---

## 🏷️ Рекомендуемые провайдеры VPS

### 💰 Бюджетные варианты:

1. **DigitalOcean** - Droplet 4GB RAM, 2 vCPU, 80GB SSD (~\$24/месяц)
2. **Vultr** - Regular Performance 4GB RAM, 2 vCPU, 80GB SSD (~\$24/месяц)
3. **Linode** - Shared 4GB RAM, 2 vCPU, 80GB SSD (~\$24/месяц)

### 🚀 Production варианты:

1. **DigitalOcean** - Droplet 8GB RAM, 4 vCPU, 160GB SSD (~\$48/месяц)
2. **AWS** - t3.large 8GB RAM, 2 vCPU + EBS 100GB (~\$60/месяц)
3. **Google Cloud** - e2-standard-2 8GB RAM, 2 vCPU + 100GB SSD (~\$55/месяц)

### 🏢 Корпоративные варианты:

1. **AWS** - c5.2xlarge 16GB RAM, 8 vCPU + EBS (~\$120/месяц)
2. **Azure** - Standard_D4s_v3 16GB RAM, 4 vCPU (~\$140/месяц)
3. **Google Cloud** - c2-standard-8 32GB RAM, 8 vCPU (~\$200/месяц)

---

## 📋 Контрольный список перед выбором сервера

### ✅ Обязательные требования:

- [ ] Минимум 4 GB RAM
- [ ] Минимум 2 vCPU
- [ ] SSD диск минимум 40 GB
- [ ] Ubuntu 20.04+ или CentOS 8+
- [ ] Стабильное интернет-соединение
- [ ] Root доступ или sudo права

### ✅ Рекомендуемые дополнения:

- [ ] Backup/снапшоты
- [ ] Мониторинг ресурсов
- [ ] Firewall настройка
- [ ] SSL сертификаты
- [ ] Автоматические обновления безопасности

---

## 🔧 Оптимизация производительности

### Для экономии ресурсов:

1. **Отключить ненужные Airflow компоненты**
2. **Настроить connection pooling в PostgreSQL**
3. **Использовать Redis для кэширования**
4. **Оптимизировать SQL запросы и индексы**
5. **Настроить logrotate для логов**

### Для масштабирования:

1. **Использовать CeleryExecutor для Airflow**
2. **Разделить компоненты по контейнерам**
3. **Настроить PostgreSQL репликацию**
4. **Использовать Load Balancer**
5. **Добавить мониторинг (Prometheus + Grafana)**

---

## 📊 Итоговая рекомендация

### 🏁 Для начала (тестирование, MVP):

**VPS: 4 GB RAM, 2 vCPU, 80 GB SSD - \$24-30/месяц**

### 🚀 Для production (рекомендуется):

**VPS: 8 GB RAM, 4 vCPU, 100 GB SSD - \$48-60/месяц**

### ⚡ Для высоких нагрузок:

**VPS: 16 GB RAM, 8 vCPU, 200 GB SSD - \$80-120/месяц**

**Вывод**: Система может работать на минимальной конфигурации (4 GB RAM, 2 vCPU), но для стабильной production-работы рекомендуется 8 GB RAM и 4 vCPU.
