#!/bin/bash

# Запуск тестов для Google Sheets интеграции
echo "Running tests for Google Sheets integration..."

# Запуск тестов с помощью unittest
python -m unittest tests/test_google_sheets_client.py tests/test_promo_importer.py

echo "Tests completed!"
