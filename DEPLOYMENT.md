# Инструкция по развертыванию в Git и на боевом сервере

## 1. Отправка кода в Git-репозиторий

### Создайте репозиторий на GitHub/GitLab:

1. Зайдите на GitHub.com или GitLab.com
2. Создайте новый репозиторий с именем `etl-analytics-system`
3. Скопируйте URL репозитория (например: `https://github.com/username/etl-analytics-system.git`)

### Добавьте remote и отправьте код:

```bash
cd /Users/zif91/Programs_py/etl_new_system

# Добавить удаленный репозиторий
git remote add origin <URL_ВАШЕГО_РЕПОЗИТОРИЯ>

# Отправить код в репозиторий
git branch -M main
git push -u origin main
```

## 2. Развертывание на боевом сервере

### Требования к серверу:

- Ubuntu 20.04 LTS или новее
- Минимум 4GB RAM
- Минимум 50GB свободного места
- Доступ к интернету
- Права sudo

### Подключение к серверу и развертывание:

```bash
# Подключение к серверу
ssh username@your-server-ip

# Клонирование репозитория
sudo mkdir -p /opt/etl-analytics
sudo chown $USER:$USER /opt/etl-analytics
cd /opt/etl-analytics
git clone <URL_ВАШЕГО_РЕПОЗИТОРИЯ> .

# Настройка переменных окружения
cp .env.example .env
nano .env  # Заполните все необходимые переменные

# Запуск скрипта развертывания
chmod +x deploy.sh
./deploy.sh
```

### После развертывания:

1. **Создайте пользователя Airflow:**

```bash
docker-compose exec etl-app airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123
```

2. **Проверьте работу сервисов:**

```bash
docker-compose ps
docker-compose logs -f
```

3. **Откройте Airflow UI:**
   - Перейдите по адресу: `http://your-server-ip:8080`
   - Логин: admin
   - Пароль: admin123 (или тот, что указали при создании)

### Настройка SSL для production:

```bash
# Установка Certbot для Let's Encrypt
sudo apt install certbot

# Получение SSL сертификата
sudo certbot certonly --standalone -d your-domain.com

# Копирование сертификатов
sudo cp /etc/letsencrypt/live/your-domain.com/fullchain.pem nginx/ssl/cert.pem
sudo cp /etc/letsencrypt/live/your-domain.com/privkey.pem nginx/ssl/key.pem

# Перезапуск контейнеров
docker-compose restart
```

### Автозапуск при перезагрузке сервера:

```bash
# Копирование systemd сервиса
sudo cp etl-analytics.service /etc/systemd/system/

# Включение автозапуска
sudo systemctl daemon-reload
sudo systemctl enable etl-analytics.service
sudo systemctl start etl-analytics.service
```

### Мониторинг и обслуживание:

```bash
# Просмотр логов
docker-compose logs -f

# Просмотр статуса
docker-compose ps

# Обновление кода
cd /opt/etl-analytics
git pull origin main
docker-compose build
docker-compose up -d

# Резервное копирование БД
docker-compose exec postgres pg_dump -U postgres advertising_analytics > backup_$(date +%Y%m%d_%H%M%S).sql
```

### Полезные команды для администрирования:

```bash
# Подключение к БД
docker-compose exec postgres psql -U postgres advertising_analytics

# Подключение к контейнеру приложения
docker-compose exec etl-app bash

# Просмотр ресурсов
docker stats

# Очистка неиспользуемых образов
docker system prune -a
```

## 3. Контрольный список безопасности для production:

- [ ] Настроены SSL сертификаты
- [ ] Изменены пароли по умолчанию
- [ ] Настроен файрвол (UFW)
- [ ] Настроено резервное копирование БД
- [ ] Настроены уведомления о сбоях
- [ ] Ограничен доступ к Airflow UI
- [ ] Настроено логирование
- [ ] Проверена работа всех DAG'ов

## 4. Переменные окружения для .env файла:

```bash
# База данных
DATABASE_URL=postgresql://etl_user:your_password@localhost:5432/advertising_analytics
POSTGRES_PASSWORD=your_secure_password

# Meta API
META_APP_ID=your_meta_app_id
META_APP_SECRET=your_meta_app_secret

# Google APIs
GA4_PROPERTY_ID=your_ga4_property_id
GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json
GOOGLE_ADS_CUSTOMER_ID=your_google_ads_customer_id

# AppsFlyer
APPSFLYER_API_TOKEN=your_appsflyer_token

# Airflow
AIRFLOW__CORE__FERNET_KEY=your_generated_fernet_key
AIRFLOW__WEBSERVER__SECRET_KEY=your_generated_secret_key
```

Для генерации ключей Fernet и Secret:

```python
from cryptography.fernet import Fernet
print(f"FERNET_KEY: {Fernet.generate_key().decode()}")

import secrets
print(f"SECRET_KEY: {secrets.token_urlsafe(32)}")
```
