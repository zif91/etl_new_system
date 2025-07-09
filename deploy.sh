#!/bin/bash

# Скрипт для развертывания ETL системы на production сервере

set -e  # Выход при ошибке

echo "🚀 Начинаем развертывание ETL системы аналитики рекламных кампаний..."

# Проверка прав root
if [[ $EUID -eq 0 ]]; then
   echo "❌ Этот скрипт не должен запускаться от root" 
   exit 1
fi

# Переменные
PROJECT_DIR="/opt/etl-analytics"
REPO_URL="https://github.com/your-username/etl_new_system.git"  # Замените на ваш URL
SERVICE_USER="etl-user"
PYTHON_VERSION="3.11"

echo "📁 Создание директории проекта..."
sudo mkdir -p $PROJECT_DIR
sudo chown $USER:$USER $PROJECT_DIR

echo "📦 Обновление системы..."
sudo apt update && sudo apt upgrade -y

echo "🐍 Установка Python $PYTHON_VERSION..."
sudo apt install -y python$PYTHON_VERSION python$PYTHON_VERSION-venv python$PYTHON_VERSION-dev
sudo apt install -y build-essential libpq-dev curl git

echo "🐋 Установка Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo usermod -aG docker $USER
    rm get-docker.sh
fi

echo "🐋 Установка Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

echo "📂 Клонирование репозитория..."
cd $PROJECT_DIR
if [ -d ".git" ]; then
    echo "Обновление существующего репозитория..."
    git pull origin main
else
    echo "Клонирование нового репозитория..."
    git clone $REPO_URL .
fi

echo "🔧 Настройка переменных окружения..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "⚠️  ВНИМАНИЕ: Необходимо настроить переменные окружения в файле .env"
    echo "   Отредактируйте файл $PROJECT_DIR/.env"
    read -p "Нажмите Enter после настройки .env файла..."
fi

echo "🗄️  Установка PostgreSQL..."
sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Создание базы данных и пользователя
echo "🗄️  Настройка базы данных..."
sudo -u postgres psql -c "CREATE DATABASE advertising_analytics;" || echo "База данных уже существует"
sudo -u postgres psql -c "CREATE USER etl_user WITH PASSWORD 'your_password';" || echo "Пользователь уже существует"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE advertising_analytics TO etl_user;" || true

echo "🔥 Настройка файрвола..."
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 8080/tcp
sudo ufw --force enable

echo "📋 Создание SSL сертификатов..."
sudo mkdir -p nginx/ssl
if [ ! -f "nginx/ssl/cert.pem" ]; then
    echo "Создание самоподписанного сертификата для тестирования..."
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout nginx/ssl/key.pem \
        -out nginx/ssl/cert.pem \
        -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"
fi

echo "🐋 Запуск контейнеров..."
docker-compose down || true
docker-compose build
docker-compose up -d

echo "⏳ Ожидание запуска сервисов..."
sleep 30

echo "🔍 Проверка статуса сервисов..."
docker-compose ps

echo "🎉 Развертывание завершено!"
echo ""
echo "📊 Airflow UI доступен по адресу: http://$(hostname -I | awk '{print $1}'):8080"
echo "👤 Пользователь по умолчанию: admin"
echo "🔑 Пароль нужно создать через: docker-compose exec etl-app airflow users create"
echo ""
echo "📝 Полезные команды:"
echo "  - Просмотр логов: docker-compose logs -f"
echo "  - Перезапуск: docker-compose restart"
echo "  - Остановка: docker-compose down"
echo "  - Обновление: git pull && docker-compose build && docker-compose up -d"
echo ""
echo "⚠️  НЕ ЗАБУДЬТЕ:"
echo "  1. Настроить переменные окружения в .env"
echo "  2. Создать пользователя Airflow"
echo "  3. Настроить SSL сертификаты для production"
echo "  4. Настроить резервное копирование базы данных"
