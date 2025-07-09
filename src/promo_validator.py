"""
Валидация и трансформация данных о промокодах из Google Sheets.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class PromoCodeValidator:
    """
    Класс для валидации данных о промокодах
    """
    
    @staticmethod
    def validate_promo_code(promo_code: str) -> bool:
        """
        Проверяет валидность промокода.
        
        Args:
            promo_code: Промокод для проверки
            
        Returns:
            True если промокод валиден, иначе False
        """
        if not promo_code or not isinstance(promo_code, str):
            return False
        
        # Проверяем, что промокод содержит только буквы, цифры и допустимые спецсимволы
        # и имеет длину от 3 до 30 символов
        pattern = r'^[a-zA-Z0-9_\-]{3,30}$'
        if not re.match(pattern, promo_code):
            logger.warning(f"Invalid promo code format: {promo_code}")
            return False
        
        return True
    
    @staticmethod
    def validate_order_id(order_id: str) -> bool:
        """
        Проверяет валидность ID заказа.
        
        Args:
            order_id: ID заказа для проверки
            
        Returns:
            True если ID заказа валиден, иначе False
        """
        if not order_id or not isinstance(order_id, str):
            return False
        
        # Проверяем, что ID заказа содержит только буквы, цифры и допустимые спецсимволы
        pattern = r'^[a-zA-Z0-9_\-]{1,100}$'
        if not re.match(pattern, order_id):
            logger.warning(f"Invalid order ID format: {order_id}")
            return False
        
        return True
    
    @staticmethod
    def validate_transaction_id(transaction_id: str) -> bool:
        """
        Проверяет валидность ID транзакции.
        
        Args:
            transaction_id: ID транзакции для проверки
            
        Returns:
            True если ID транзакции валиден, иначе False
        """
        if not transaction_id or not isinstance(transaction_id, str):
            return False
        
        # Проверяем, что ID транзакции содержит только буквы, цифры и допустимые спецсимволы
        pattern = r'^[a-zA-Z0-9_\-]{1,100}$'
        if not re.match(pattern, transaction_id):
            logger.warning(f"Invalid transaction ID format: {transaction_id}")
            return False
        
        return True
    
    @staticmethod
    def validate_restaurant(restaurant: str, allowed_restaurants: Optional[List[str]] = None) -> bool:
        """
        Проверяет валидность ресторана.
        
        Args:
            restaurant: Название ресторана для проверки
            allowed_restaurants: Список разрешенных ресторанов (если None, проверяется только непустое значение)
            
        Returns:
            True если ресторан валиден, иначе False
        """
        if not restaurant or not isinstance(restaurant, str):
            return False
        
        if allowed_restaurants and restaurant not in allowed_restaurants:
            logger.warning(f"Restaurant {restaurant} not in allowed list: {allowed_restaurants}")
            return False
        
        return True
    
    @staticmethod
    def validate_country(country: str, allowed_countries: Optional[List[str]] = None) -> bool:
        """
        Проверяет валидность страны.
        
        Args:
            country: Название страны для проверки
            allowed_countries: Список разрешенных стран (если None, проверяется только непустое значение)
            
        Returns:
            True если страна валидна, иначе False
        """
        if not country or not isinstance(country, str):
            return False
        
        if allowed_countries and country not in allowed_countries:
            logger.warning(f"Country {country} not in allowed list: {allowed_countries}")
            return False
        
        return True
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[datetime.date]:
        """
        Парсит дату из строки в разных форматах.
        
        Args:
            date_str: Строка с датой
            
        Returns:
            Объект datetime.date или None, если дата не распознана
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        # Пробуем различные форматы дат
        date_formats = ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']
        
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format).date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    @staticmethod
    def parse_amount(amount_str: Any) -> Optional[float]:
        """
        Парсит сумму, учитывая разные форматы (с точкой или запятой в качестве разделителя дробной части).
        
        Args:
            amount_str: Строка или число, представляющее сумму
            
        Returns:
            Число типа float или None, если сумма не распознана
        """
        if amount_str is None:
            return None
        
        # Если это уже число, просто возвращаем его
        if isinstance(amount_str, (int, float)):
            return float(amount_str)
        
        if not isinstance(amount_str, str):
            return None
        
        try:
            # Удаляем все пробелы
            amount_str = amount_str.strip().replace(' ', '')
            
            # Если запятая используется как разделитель дробной части (например, '1500,50')
            # и нет других запятых, заменяем ее на точку
            if ',' in amount_str and amount_str.count(',') == 1 and '.' not in amount_str:
                # Проверяем, что после запятой идут только цифры (до 2 знаков)
                parts = amount_str.split(',')
                if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) <= 2:
                    amount_str = amount_str.replace(',', '.')
            # Если запятая используется как разделитель тысяч (например, '1,500.00')
            elif ',' in amount_str and '.' in amount_str:
                # Удаляем все запятые
                amount_str = amount_str.replace(',', '')
            
            # Пробуем преобразовать строку в число
            value = float(amount_str)
            
            # Проверяем, что значение неотрицательное
            if value < 0:
                logger.warning(f"Negative amount not allowed: {amount_str}")
                return None
                
            return value
            
        except (ValueError, TypeError):
            logger.warning(f"Could not parse amount: {amount_str}")
            return None


class PromoCodeTransformer:
    """
    Класс для трансформации данных о промокодах
    """
    
    def __init__(self, allowed_restaurants: Optional[List[str]] = None, allowed_countries: Optional[List[str]] = None):
        """
        Инициализирует трансформер промокодов.
        
        Args:
            allowed_restaurants: Список разрешенных ресторанов
            allowed_countries: Список разрешенных стран
        """
        self.validator = PromoCodeValidator()
        self.allowed_restaurants = allowed_restaurants or ["Тануки", "Каспийка", "Белла"]
        self.allowed_countries = allowed_countries or ["Казахстан", "Узбекистан"]
    
    def transform_record(self, record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        """
        Трансформирует запись о промокоде из сырого формата в формат для БД.
        
        Args:
            record: Словарь с данными о промокоде
            
        Returns:
            Кортеж: (трансформированная запись или None, список ошибок)
        """
        errors = []
        
        # Проверяем наличие обязательных полей
        required_fields = ['promo_code', 'order_id', 'order_date', 'order_amount', 'restaurant', 'country']
        for field in required_fields:
            if field not in record or not record[field]:
                errors.append(f"Missing required field: {field}")
        
        if errors:
            return None, errors
        
        # Проверяем и трансформируем промокод
        if not self.validator.validate_promo_code(record['promo_code']):
            errors.append(f"Invalid promo code: {record['promo_code']}")
        
        # Проверяем и трансформируем ID заказа
        if not self.validator.validate_order_id(record['order_id']):
            errors.append(f"Invalid order ID: {record['order_id']}")
        
        # Определяем и проверяем ID транзакции
        transaction_id = record.get('transaction_id', record['order_id'])
        if not self.validator.validate_transaction_id(transaction_id):
            errors.append(f"Invalid transaction ID: {transaction_id}")
        
        # Проверяем и трансформируем дату заказа
        order_date = self.validator.parse_date(record['order_date'])
        if not order_date:
            errors.append(f"Invalid order date: {record['order_date']}")
        
        # Проверяем и трансформируем сумму заказа
        order_amount = self.validator.parse_amount(record['order_amount'])
        if order_amount is None:
            errors.append(f"Invalid order amount: {record['order_amount']}")
        
        # Проверяем ресторан
        if not self.validator.validate_restaurant(record['restaurant'], self.allowed_restaurants):
            errors.append(f"Invalid restaurant: {record['restaurant']}")
        
        # Проверяем страну
        if not self.validator.validate_country(record['country'], self.allowed_countries):
            errors.append(f"Invalid country: {record['country']}")
        
        if errors:
            return None, errors
        
        # Создаем трансформированную запись
        transformed_record = {
            'promo_code': record['promo_code'],
            'order_id': record['order_id'],
            'transaction_id': transaction_id,
            'order_date': order_date,
            'order_amount': order_amount,
            'restaurant': record['restaurant'],
            'country': record['country'],
            'promo_source': record.get('promo_source')
        }
        
        return transformed_record, []
    
    def transform_records(self, records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
        """
        Трансформирует список записей о промокодах из сырого формата в формат для БД.
        
        Args:
            records: Список словарей с данными о промокодах
            
        Returns:
            Кортеж: (список трансформированных записей, словарь с ошибками по индексам)
        """
        transformed_records = []
        errors_by_index = {}
        
        for i, record in enumerate(records):
            transformed_record, errors = self.transform_record(record)
            
            if errors:
                errors_by_index[i] = errors
            
            if transformed_record:
                transformed_records.append(transformed_record)
        
        return transformed_records, errors_by_index
