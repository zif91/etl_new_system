#!/bin/bash

# Скрипт для обновления production системы на сервере
# Запускать на сервере после git push

echo "🚀 Starting production deployment update..."

# Переходим в директорию проекта
cd /root/etl_new_system || {
    echo "❌ Project directory not found!"
    exit 1
}

echo "📥 Pulling latest changes from GitHub..."
git pull origin main

if [ $? -ne 0 ]; then
    echo "❌ Git pull failed!"
    exit 1
fi

echo "🛑 Stopping current containers..."
docker compose down

echo "🔄 Rebuilding containers with latest code..."
docker compose build --no-cache

echo "🚀 Starting updated containers..."
docker compose up -d

echo "⏱️  Waiting for services to start..."
sleep 30

echo "✅ Checking container status..."
docker ps

echo "🎯 Checking Airflow webserver..."
curl -f http://localhost:8080/health || echo "⚠️  Airflow might still be starting..."

echo ""
echo "🎉 Production deployment completed!"
echo "📊 Airflow UI: http://your-server:8080"
echo "👤 Login: admin / admin"
echo ""
echo "📋 Next steps:"
echo "1. Check Airflow UI for DAG status"
echo "2. Enable advertising_data_pipeline DAG"
echo "3. Monitor first run for any errors"
echo "4. Check database for imported data"

echo ""
echo "🔍 Useful commands:"
echo "docker logs etl_new_system-airflow-webserver-1    # Check Airflow logs"
echo "docker logs etl_new_system-airflow-scheduler-1    # Check scheduler logs"
echo "docker exec -it etl_new_system-postgres-1 psql -U airflow -d airflow    # Connect to database"
