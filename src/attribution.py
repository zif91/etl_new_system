"""
Система атрибуции источников заказов.

Модуль предоставляет функциональность для определения источников
заказов на основе различных моделей атрибуции и приоритетов.
"""

import logging
from typing import Dict, List, Any, Optional, Set, Union
from datetime import datetime, timedelta
import re

logger = logging.getLogger(__name__)


class AttributionModel:
    """Базовый класс для моделей атрибуции."""
    
    LAST_CLICK = 'last_click'
    FIRST_CLICK = 'first_click'
    LINEAR = 'linear'
    TIME_DECAY = 'time_decay'
    POSITION_BASED = 'position_based'
    CUSTOM = 'custom'
    
    @staticmethod
    def get_available_models() -> List[str]:
        """
        Возвращает список доступных моделей атрибуции.
        
        Returns:
            Список названий доступных моделей
        """
        return [
            AttributionModel.LAST_CLICK,
            AttributionModel.FIRST_CLICK,
            AttributionModel.LINEAR,
            AttributionModel.TIME_DECAY,
            AttributionModel.POSITION_BASED,
            AttributionModel.CUSTOM
        ]


class AttributionSource:
    """Константы источников атрибуции."""
    
    # Основные источники
    PROMO_CODE = 'promo_code'
    UTM_SOURCE = 'utm_source'
    DIRECT = 'direct'
    REFERRAL = 'referral'
    ORGANIC = 'organic'
    INTERNAL = 'internal'
    UNKNOWN = 'unknown'
    
    # Внешние платформы 
    GOOGLE_ADS = 'google_ads'
    GOOGLE_ORGANIC = 'google_organic'
    META = 'meta'
    INSTAGRAM = 'instagram'
    FACEBOOK = 'facebook'
    YANDEX = 'yandex'
    
    # Типы медиа
    PAID = 'paid'
    ORGANIC_SOCIAL = 'organic_social'
    EMAIL = 'email'
    SMS = 'sms'
    PUSH = 'push'
    QR = 'qr'
    OFFLINE = 'offline'


class AttributionRules:
    """Правила атрибуции заказов."""
    
    def __init__(self, priority_list: Optional[List[str]] = None, 
                 source_mapping: Optional[Dict[str, str]] = None,
                 default_source: str = AttributionSource.UTM_SOURCE,
                 attribution_model: str = AttributionModel.LAST_CLICK):
        """
        Инициализирует правила атрибуции.
        
        Args:
            priority_list: Список приоритетов источников (от высокого к низкому).
                           По умолчанию: [PROMO_CODE, UTM_SOURCE, ...]
            source_mapping: Словарь маппинга значений utm_source/medium на стандартизированные источники
            default_source: Источник по умолчанию, если не определен
            attribution_model: Модель атрибуции для использования
        """
        self.priority_list = priority_list or [
            AttributionSource.PROMO_CODE,   # Промокоды имеют наивысший приоритет
            AttributionSource.UTM_SOURCE,   # UTM из GA4
            AttributionSource.REFERRAL,     # Реферальные ссылки
            AttributionSource.DIRECT,       # Прямые переходы
            AttributionSource.ORGANIC       # Органические посещения
        ]
        
        self.source_mapping = source_mapping or {
            # Сопоставление utm_source/medium с источниками
            'google / cpc': AttributionSource.GOOGLE_ADS,
            'google / organic': AttributionSource.GOOGLE_ORGANIC,
            'facebook / paid': AttributionSource.FACEBOOK,
            'instagram / paid': AttributionSource.INSTAGRAM,
            'facebook / referral': AttributionSource.FACEBOOK,
            'instagram / referral': AttributionSource.INSTAGRAM,
            'email / email': AttributionSource.EMAIL,
            'push / notification': AttributionSource.PUSH,
            '(direct) / (none)': AttributionSource.DIRECT,
            'qr / offline': AttributionSource.QR
        }
        
        self.default_source = default_source
        self.attribution_model = attribution_model

    def get_priority(self, source: str) -> int:
        """
        Возвращает приоритет источника.
        
        Args:
            source: Название источника
            
        Returns:
            Приоритет (меньше число - выше приоритет), -1 если не найден
        """
        try:
            return self.priority_list.index(source)
        except ValueError:
            return len(self.priority_list)  # Источник не в списке - низший приоритет
    
    def standardize_source(self, utm_source: Optional[str], utm_medium: Optional[str]) -> str:
        """
        Стандартизирует источник на основе utm_source и utm_medium.
        
        Args:
            utm_source: Значение utm_source
            utm_medium: Значение utm_medium
            
        Returns:
            Стандартизированный источник
        """
        if not utm_source:
            return self.default_source
        
        # Создаем ключ для поиска в маппинге
        key = f"{utm_source} / {utm_medium}" if utm_medium else utm_source
        
        # Возвращаем маппинг или сам utm_source, если маппинг не найден
        return self.source_mapping.get(key.lower(), utm_source)


class AttributionSourceAssigner:
    """
    Класс для назначения источников атрибуции транзакциям.
    """
    
    def __init__(self, rules: Optional[AttributionRules] = None):
        """
        Инициализирует ассайнер источников атрибуции.
        
        Args:
            rules: Правила атрибуции
        """
        self.rules = rules or AttributionRules()
        
        # Статистика атрибуции
        self.stats = {
            'processed': 0,
            'sources': {},
            'models_used': {}
        }
    
    def assign_attribution_source(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Назначает источник атрибуции для транзакции.
        
        Args:
            transaction: Словарь с данными о транзакции
            
        Returns:
            Обновленная транзакция с полем attribution_source
        """
        # Обновляем статистику
        self.stats['processed'] += 1
        
        # Если у транзакции уже есть promo_code, используем его как источник
        if transaction.get('is_promo_order', False) and transaction.get('promo_code'):
            transaction['attribution_source'] = AttributionSource.PROMO_CODE
            transaction['attribution_details'] = {
                'source': AttributionSource.PROMO_CODE,
                'promo_code': transaction.get('promo_code'),
                'promo_source': transaction.get('promo_source'),
                'model_used': AttributionModel.LAST_CLICK
            }
            
            self._update_source_stats(AttributionSource.PROMO_CODE, AttributionModel.LAST_CLICK)
            return transaction
        
        # Извлекаем UTM-параметры
        utm_parts = transaction.get('sourceMedium', '').split(' / ')
        utm_source = utm_parts[0] if len(utm_parts) > 0 else None
        utm_medium = utm_parts[1] if len(utm_parts) > 1 else None
        
        # Стандартизируем источник
        std_source = self.rules.standardize_source(utm_source, utm_medium)
        
        # Определяем тип источника
        is_paid = self._is_paid_source(utm_source, utm_medium)
        
        # Используем модель атрибуции по умолчанию (last click)
        model_used = AttributionModel.LAST_CLICK
        
        # Заполняем данные об атрибуции
        transaction['attribution_source'] = std_source
        transaction['attribution_details'] = {
            'source': std_source,
            'utm_source': utm_source,
            'utm_medium': utm_medium,
            'utm_campaign': transaction.get('campaign'),
            'is_paid': is_paid,
            'model_used': model_used,
            'source_priority': self.rules.get_priority(std_source)
        }
        
        self._update_source_stats(std_source, model_used)
        return transaction
    
    def assign_attribution_to_transactions(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Назначает источники атрибуции для списка транзакций.
        
        Args:
            transactions: Список транзакций
            
        Returns:
            Список обновленных транзакций
        """
        # Сбрасываем статистику
        self._reset_stats()
        
        # Назначаем атрибуцию каждой транзакции
        return [self.assign_attribution_source(transaction) for transaction in transactions]
    
    def resolve_attribution_conflict(self, transactions: List[Dict[str, Any]], 
                                     key_field: str = 'order_id') -> List[Dict[str, Any]]:
        """
        Разрешает конфликты атрибуции для транзакций с одинаковым ключом.
        
        Args:
            transactions: Список транзакций
            key_field: Поле, по которому группируются транзакции
            
        Returns:
            Список транзакций с разрешенными конфликтами
        """
        if not transactions:
            return []
        
        # Группируем транзакции по key_field
        grouped_transactions = {}
        for transaction in transactions:
            key = transaction.get(key_field)
            if not key:
                continue
                
            if key not in grouped_transactions:
                grouped_transactions[key] = []
            grouped_transactions[key].append(transaction)
        
        # Разрешаем конфликты для каждой группы
        resolved_transactions = []
        for key, group in grouped_transactions.items():
            if len(group) == 1:
                # Если в группе одна транзакция, добавляем её без изменений
                resolved_transactions.append(group[0])
            else:
                # Сортируем транзакции по приоритету источника (меньше - выше приоритет)
                sorted_transactions = sorted(
                    group, 
                    key=lambda t: t.get('attribution_details', {}).get('source_priority', 999)
                )
                
                # Используем транзакцию с наивысшим приоритетом
                winner = sorted_transactions[0]
                
                # Логируем разрешение конфликта
                sources = [t.get('attribution_source') for t in group]
                logger.info(
                    f"Resolved attribution conflict for {key_field}={key}. "
                    f"Sources: {sources}. Selected: {winner.get('attribution_source')}"
                )
                
                resolved_transactions.append(winner)
        
        return resolved_transactions
    
    def _is_paid_source(self, utm_source: Optional[str], utm_medium: Optional[str]) -> bool:
        """
        Определяет, является ли источник платным.
        
        Args:
            utm_source: Значение utm_source
            utm_medium: Значение utm_medium
            
        Returns:
            True, если источник платный, иначе False
        """
        if not utm_source or not utm_medium:
            return False
        
        # Проверяем платные медиа
        paid_media = {'cpc', 'ppc', 'cpm', 'paid', 'paidsocial', 'display'}
        return utm_medium.lower() in paid_media
    
    def _reset_stats(self):
        """Сбрасывает статистику атрибуции."""
        self.stats = {
            'processed': 0,
            'sources': {},
            'models_used': {}
        }
    
    def _update_source_stats(self, source: str, model: str):
        """
        Обновляет статистику по источникам и моделям атрибуции.
        
        Args:
            source: Источник атрибуции
            model: Используемая модель атрибуции
        """
        # Обновляем статистику по источникам
        if source in self.stats['sources']:
            self.stats['sources'][source] += 1
        else:
            self.stats['sources'][source] = 1
            
        # Обновляем статистику по моделям
        if model in self.stats['models_used']:
            self.stats['models_used'][model] += 1
        else:
            self.stats['models_used'][model] = 1
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику атрибуции.
        
        Returns:
            Словарь со статистикой
        """
        return self.stats
