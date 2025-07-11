# Базовый образ Python
FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя airflow
RUN useradd --create-home --shell /bin/bash airflow

# Установка рабочей директории
WORKDIR /app

# Установка Airflow версии 2.7.3 с constraints
ARG AIRFLOW_VERSION=2.7.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Установка Apache Airflow с constraints для быстрой установки
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Копирование файла зависимостей
COPY requirements.txt .

# Установка дополнительных зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Создание необходимых директорий для логов Airflow
RUN mkdir -p /app/logs /app/logs/scheduler /app/logs/webserver /app/logs/dag_processor \
    /app/data/comparisons /app/data/reports

# Установка прав доступа
RUN chown -R airflow:airflow /app

# Переключение на пользователя airflow
USER airflow

# Переменные окружения для Airflow
ENV AIRFLOW_HOME=/app
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/dags
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

# Экспорт портов
EXPOSE 8080

# Команда по умолчанию
CMD ["bash", "-c", "airflow db migrate && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true && airflow webserver --port 8080 & airflow scheduler"]
