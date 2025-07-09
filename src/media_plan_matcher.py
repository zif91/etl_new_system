"""
Модуль для сопоставления данных рекламных кампаний с медиапланом.
"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

class MediaPlanMatcher:
    """
    Сопоставляет фактические данные кампаний с запланированными данными из медиаплана.
    """

    def __init__(self, media_plan_data: List[Dict[str, Any]], manual_mappings: Optional[Dict[Tuple, int]] = None):
        """
        Инициализирует матчер с данными медиаплана и ручными сопоставлениями.

        Args:
            media_plan_data: Список словарей, где каждый словарь представляет
                             одну строку медиаплана.
            manual_mappings: Словарь для ручного сопоставления.
                             Ключ - кортеж-идентификатор кампании, значение - ID медиаплана.
        """
        self.media_plan = self._prepare_media_plan(media_plan_data)
        self.manual_mappings = manual_mappings or {}
        # Создаем обратный индекс для быстрого поиска элемента плана по ID
        self.media_plan_by_id = {item['id']: item for item in media_plan_data}


    def _prepare_media_plan(self, media_plan_data: List[Dict[str, Any]]) -> Dict[Tuple, Dict[str, Any]]:
        """
        Преобразует список словарей медиаплана в словарь для быстрого поиска.
        Ключом является кортеж из критериев сопоставления.
        """
        prepared_plan = {}
        for item in media_plan_data:
            # Убедимся, что дата в плане - это первый день месяца
            plan_month = datetime.strptime(item['month'], '%Y-%m-%d').strftime('%Y-%m-01')
            
            key = (
                plan_month,
                item.get('restaurant'),
                item.get('country'),
                item.get('campaign_type'),
                item.get('goal'),
                item.get('source')
            )
            prepared_plan[key] = item
        return prepared_plan

    def _get_campaign_identifier(self, campaign_data: Dict[str, Any]) -> Optional[Tuple]:
        """Создает уникальный идентификатор для кампании на основе ее атрибутов."""
        try:
            # Используем campaign_name для большей уникальности
            campaign_month = campaign_data['date'].strftime('%Y-%m-01')
            return (
                campaign_month,
                campaign_data.get('restaurant'),
                campaign_data.get('country'),
                campaign_data.get('campaign_type'),
                campaign_data.get('campaign_goal'),
                campaign_data.get('source'),
                campaign_data.get('campaign_name')
            )
        except (AttributeError, ValueError):
            # self.logger.warning(...) можно добавить логирование
            return None

    def match_campaign_to_media_plan(
        self,
        campaign_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Находит соответствующую запись в медиаплане для данной кампании и рассчитывает отклонения.

        Сопоставление происходит по ключу:
        (месяц, ресторан, страна, тип кампании, цель, источник)

        Args:
            campaign_data: Словарь с данными по конкретной кампании.
                           Ожидаемые ключи: 'restaurant', 'country', 'campaign_type',
                           'campaign_goal', 'source', 'date', 'spend', 'impressions', 'clicks', 'orders'.

        Returns:
            Словарь с ID медиаплана и отклонениями или None, если совпадение не найдено.
        """
        # 1. Проверка ручного сопоставления
        campaign_identifier = self._get_campaign_identifier(campaign_data)
        if campaign_identifier and campaign_identifier in self.manual_mappings:
            media_plan_id = self.manual_mappings[campaign_identifier]
            matched_plan_item = self.media_plan_by_id.get(media_plan_id)
            
            if matched_plan_item:
                variances = self._calculate_variance(campaign_data, matched_plan_item)
                return {
                    'media_plan_id': media_plan_id,
                    'matched_plan_item': matched_plan_item,
                    'variances': variances,
                    'is_manual': True,
                    'is_fuzzy': False
                }

        # 2. Точное сопоставление
        try:
            campaign_month = campaign_data['date'].strftime('%Y-%m-01')
        except (AttributeError, ValueError):
             # Если дата в неверном формате или не объект datetime
            # self.logger.warning(f"Неверный формат даты для кампании: {campaign_data.get('campaign_name')}")
            return None

        match_key = (
            campaign_month,
            campaign_data.get('restaurant'),
            campaign_data.get('country'),
            campaign_data.get('campaign_type'),
            campaign_data.get('campaign_goal'),
            campaign_data.get('source')
        )

        matched_plan_item = self.media_plan.get(match_key)

        if matched_plan_item:
            variances = self._calculate_variance(campaign_data, matched_plan_item)
            return {
                'media_plan_id': matched_plan_item.get('id'),
                'matched_plan_item': matched_plan_item,
                'variances': variances,
                'is_manual': False,
                'is_fuzzy': False
            }
        
        # 3. Нечеткое сопоставление
        return self._find_best_fuzzy_match(campaign_data)

    def _find_best_fuzzy_match(
        self,
        campaign_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Ищет наиболее подходящее совпадение в медиаплане, если точное совпадение не найдено.
        Использует систему "очков" для оценки качества совпадения.
        """
        best_matches = []
        highest_score = 0

        try:
            campaign_month = campaign_data['date'].strftime('%Y-%m-01')
        except (AttributeError, ValueError):
            return None

        for key, plan_item in self.media_plan.items():
            # Сравниваем только в рамках одного месяца
            if key[0] != campaign_month:
                continue

            score = 0
            # Критерии и их "вес" в очках
            criteria_map = {
                'restaurant': 'restaurant',
                'country': 'country',
                'campaign_type': 'campaign_type',
                'goal': 'campaign_goal', # В данных кампании ключ 'campaign_goal'
                'source': 'source'
            }
            
            for plan_key, campaign_key in criteria_map.items():
                if plan_item.get(plan_key) == campaign_data.get(campaign_key):
                    score += 1
            
            if score > highest_score:
                highest_score = score
                best_matches = [plan_item] # Новый лучший результат
            elif score == highest_score and score > 0:
                best_matches.append(plan_item) # Еще один результат с таким же скором
            
        # Считаем совпадение успешным, если набрано достаточно очков (например, > 3)
        if highest_score >= 3 and best_matches:
            is_ambiguous = len(best_matches) > 1
            best_match = best_matches[0]

            # Улучшенная обработка неоднозначности: выбираем по ближайшему бюджету
            if is_ambiguous:
                campaign_spend = campaign_data.get('spend', 0)
                closest_match = None
                smallest_diff = float('inf')

                for match in best_matches:
                    plan_budget = match.get('planned_budget', 0)
                    diff = abs(campaign_spend - plan_budget)
                    
                    if diff < smallest_diff:
                        smallest_diff = diff
                        closest_match = match
                
                # Если удалось найти ближайшее совпадение, используем его
                best_match = closest_match if closest_match else best_matches[0]

            variances = self._calculate_variance(campaign_data, best_match)
            return {
                'media_plan_id': best_match.get('id'),
                'matched_plan_item': best_match,
                'variances': variances,
                'match_score': highest_score,
                'is_manual': False,
                'is_fuzzy': True,
                'is_ambiguous': is_ambiguous
            }

        return None


    def _safe_divide(self, numerator: float, denominator: float) -> float:
        """Безопасное деление для избежания ZeroDivisionError."""
        if denominator == 0:
            return 0.0
        return numerator / denominator

    def _calculate_variance(
        self,
        campaign_metrics: Dict[str, Any],
        plan_metrics: Dict[str, Any]
    ) -> Dict[str, Dict[str, float]]:
        """
        Рассчитывает абсолютное и процентное отклонение фактических и производных метрик от плановых.
        Производные метрики: CPM, CPC, CPA, CPO, ДРР.

        Args:
            campaign_metrics: Фактические метрики кампании.
            plan_metrics: Плановые метрики.

        Returns:
            Словарь с отклонениями по каждой метрике.
        """
        variances = {}
        metrics_to_compare = {
            'spend': 'planned_budget',
            'impressions': 'planned_impressions',
            'clicks': 'planned_clicks',
            'orders': 'planned_orders',
            'revenue': 'planned_revenue'
        }

        # 1. Собираем базовые фактические и плановые метрики
        fact = {key: campaign_metrics.get(key, 0) or 0 for key in metrics_to_compare.keys()}
        plan = {key: plan_metrics.get(plan_key, 0) or 0 for key, plan_key in metrics_to_compare.items()}

        # 2. Рассчитываем отклонения для базовых метрик
        for key in fact:
            absolute_variance = fact[key] - plan[key]
            relative_variance = self._safe_divide(absolute_variance, plan[key]) if plan[key] else (1.0 if fact[key] > 0 else 0.0)
            
            variances[key] = {
                'fact': fact[key],
                'plan': plan[key],
                'absolute_variance': absolute_variance,
                'relative_variance_percent': relative_variance * 100
            }

        # 3. Рассчитываем производные метрики (fact и plan)
        derived_fact = {
            'cpm': self._safe_divide(fact['spend'] * 1000, fact['impressions']),
            'cpc': self._safe_divide(fact['spend'], fact['clicks']),
            'cpa': self._safe_divide(fact['spend'], fact['orders']),
            'drr': self._safe_divide(fact['spend'] * 100, fact['revenue'])
        }
        # CPO часто используется как синоним CPA
        derived_fact['cpo'] = derived_fact['cpa']

        derived_plan = {
            'cpm': self._safe_divide(plan['spend'] * 1000, plan['impressions']),
            'cpc': self._safe_divide(plan['spend'], plan['clicks']),
            'cpa': self._safe_divide(plan['spend'], plan['orders']),
            'drr': self._safe_divide(plan['spend'] * 100, plan['revenue'])
        }
        derived_plan['cpo'] = derived_plan['cpa']

        # 4. Рассчитываем отклонения для производных метрик
        for key in derived_fact:
            fact_value = derived_fact[key]
            plan_value = derived_plan[key]
            
            absolute_variance = fact_value - plan_value
            # Для стоимостных метрик (CPM, CPC, CPA, DRR) инвертируем знак отклонения,
            # так как меньшее значение является лучшим результатом.
            # Таким образом, отрицательное отклонение будет означать экономию.
            relative_variance = self._safe_divide(absolute_variance, plan_value) if plan_value else (1.0 if fact_value > 0 else 0.0)

            variances[key] = {
                'fact': fact_value,
                'plan': plan_value,
                'absolute_variance': absolute_variance,
                'relative_variance_percent': relative_variance * 100
            }
            
        return variances
