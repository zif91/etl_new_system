# Оптимизация процесса деплоя ETL системы

## Проблема

Установка зависимостей Apache Airflow через pip может занимать очень много времени (до 30-60 минут) из-за:

- Backtracking при разрешении зависимостей
- Большого количества транзитивных зависимостей
- Конфликтов версий между пакетами

## Решения

### 1. Использование constraints файла (рекомендуется)

**Файл:** `Dockerfile` (по умолчанию)

Использует официальный constraints файл Apache Airflow для конкретной версии Python:

```dockerfile
ARG AIRFLOW_VERSION=2.7.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

**Преимущества:**

- Быстрая установка (5-10 минут вместо 30-60)
- Проверенные совместимые версии зависимостей
- Официальная поддержка Apache Airflow

### 2. Использование официального образа Airflow

**Файл:** `Dockerfile.airflow`

Использует готовый образ `apache/airflow:2.7.3-python3.11`:

```dockerfile
FROM apache/airflow:2.7.3-python3.11
```

**Преимущества:**

- Мгновенная "установка" Airflow (уже включён в образ)
- Оптимизированная конфигурация
- Регулярные обновления безопасности

## Использование

### Стандартный деплой (с constraints)

```bash
./fast_deploy.sh
```

### Деплой с официальным образом Airflow

```bash
./fast_deploy.sh airflow
```

### Ручной деплой

```bash
# С constraints
export DOCKERFILE=Dockerfile
docker-compose build --no-cache
docker-compose up -d

# С официальным образом
export DOCKERFILE=Dockerfile.airflow
docker-compose build --no-cache
docker-compose up -d
```

## Рекомендации

1. **Для production:** Используйте официальный образ (`./fast_deploy.sh airflow`)
2. **Для development:** Используйте constraints (`./fast_deploy.sh`)
3. **Для CI/CD:** Используйте многоэтапную сборку с кэшированием слоёв

## Время сборки

- **Без оптимизации:** 30-60 минут
- **С constraints:** 5-10 минут
- **С официальным образом:** 2-3 минуты

## Устранение неполадок

### Проблема: Долгая сборка

```bash
# Очистка Docker кэша
docker system prune -a

# Пересборка с нуля
docker-compose build --no-cache
```

### Проблема: Конфликты зависимостей

```bash
# Проверка версий в constraints
curl -s "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt" | grep <package_name>
```

### Проблема: Нехватка памяти при сборке

```bash
# Увеличение лимита памяти для Docker
docker system info | grep -i memory
```

## Мониторинг сборки

```bash
# Мониторинг процесса сборки
docker-compose build --progress=plain

# Проверка использования ресурсов
docker stats
```
