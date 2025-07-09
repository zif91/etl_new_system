#!/bin/bash

# Быстрый деплой с оптимизацией процесса сборки
set -e

echo "=== Быстрый деплой ETL системы ==="
echo ""

# Проверка типа сборки
if [[ "$1" == "airflow" ]]; then
    echo "Используется официальный образ Apache Airflow"
    export DOCKERFILE="Dockerfile.airflow"
else
    echo "Используется оптимизированный образ с constraints"
    export DOCKERFILE="Dockerfile"
fi

echo "Dockerfile: $DOCKERFILE"
echo ""

# Остановка существующих контейнеров
echo "Остановка существующих контейнеров..."
docker-compose down || true

# Очистка старых образов
echo "Очистка старых образов..."
docker system prune -f

# Сборка с указанным Dockerfile
echo "Сборка образа..."
docker-compose build --no-cache

# Запуск контейнеров
echo "Запуск контейнеров..."
docker-compose up -d

echo ""
echo "=== Деплой завершён ==="
echo ""

# Проверка статуса
echo "Проверка статуса контейнеров:"
docker-compose ps

echo ""
echo "Логи последних 50 строк:"
docker-compose logs --tail=50

echo ""
echo "Для мониторинга логов в реальном времени:"
echo "docker-compose logs -f"
echo ""
echo "Веб-интерфейс Airflow будет доступен по адресу: http://localhost:8080"
echo "Логин: admin, Пароль: admin"
