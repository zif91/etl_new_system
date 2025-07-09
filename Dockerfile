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

# Копирование файла зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Создание необходимых директорий
RUN mkdir -p /app/logs /app/data/comparisons /app/data/reports

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

# Инициализация базы данных Airflow
RUN airflow db init

# Экспорт портов
EXPOSE 8080

# Команда по умолчанию
CMD ["bash", "-c", "airflow webserver --port 8080 & airflow scheduler"]
