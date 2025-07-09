"""
Трансформация данных AppsFlyer API в стандартный формат.
"""
from typing import Dict, List, Any
from datetime import datetime


def transform_appsflyer_installs(installs_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразует данные отчета об установках AppsFlyer в стандартный формат.
    
    Args:
        installs_data: Данные об установках из API AppsFlyer
        
    Returns:
        Трансформированные данные в стандартном формате
    """
    transformed_data = []
    
    for item in installs_data:
        transformed = {
            "date": item.get("date", ""),
            "media_source": item.get("media_source", ""),
            "campaign": item.get("campaign", ""),
            "installs": item.get("installs", 0),
            "cost_per_install": item.get("cost_per_install", 0.0),
            "clicks": item.get("clicks", 0),
            "impressions": item.get("impressions", 0),
            "cost": item.get("cost", 0.0),
            "app_open": item.get("app_open", 0),
            "platform": item.get("platform", ""),
            "country_code": item.get("country_code", ""),
            "raw_data": item  # Сохраняем исходные данные для отладки и возможного последующего анализа
        }
        
        transformed_data.append(transformed)
    
    return transformed_data


def transform_appsflyer_events(events_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразует данные отчета о событиях AppsFlyer в стандартный формат.
    
    Args:
        events_data: Данные о событиях из API AppsFlyer
        
    Returns:
        Трансформированные данные в стандартном формате
    """
    transformed_data = []
    
    for item in events_data:
        event_name = item.get("event_name", "")
        
        # Определяем, является ли событие покупкой
        is_purchase = "purchase" in event_name.lower() or "order" in event_name.lower()
        
        transformed = {
            "date": item.get("date", ""),
            "media_source": item.get("media_source", ""),
            "campaign": item.get("campaign", ""),
            "event_name": event_name,
            "event_count": item.get("event_counter", 0),
            "event_revenue": item.get("event_revenue", 0.0),
            "purchases": item.get("event_counter", 0) if is_purchase else 0,
            "revenue": item.get("event_revenue", 0.0) if is_purchase else 0.0,
            "platform": item.get("platform", ""),
            "country_code": item.get("country_code", ""),
            "raw_data": item  # Сохраняем исходные данные
        }
        
        transformed_data.append(transformed)
    
    return transformed_data


def transform_appsflyer_retention(retention_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразует данные отчета о показателях удержания AppsFlyer в стандартный формат.
    
    Args:
        retention_data: Данные об удержании из API AppsFlyer
        
    Returns:
        Трансформированные данные в стандартном формате
    """
    transformed_data = []
    
    for item in retention_data:
        # Извлекаем данные по удержанию для разных дней (day_1, day_7, day_30 и т.д.)
        retention_days = {}
        for key, value in item.items():
            if key.startswith("retention_day_"):
                day_num = key.replace("retention_day_", "")
                retention_days[f"day_{day_num}"] = value
        
        transformed = {
            "date": item.get("date", ""),
            "media_source": item.get("media_source", ""),
            "campaign": item.get("campaign", ""),
            "installs": item.get("installs", 0),
            "retention_days": retention_days,
            "day_1_retention": retention_days.get("day_1", 0.0),
            "day_7_retention": retention_days.get("day_7", 0.0),
            "day_30_retention": retention_days.get("day_30", 0.0),
            "platform": item.get("platform", ""),
            "country_code": item.get("country_code", ""),
            "raw_data": item  # Сохраняем исходные данные
        }
        
        transformed_data.append(transformed)
    
    return transformed_data


def transform_appsflyer_ltv(ltv_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Преобразует данные отчета о LTV AppsFlyer в стандартный формат.
    
    Args:
        ltv_data: Данные о LTV из API AppsFlyer
        
    Returns:
        Трансформированные данные в стандартном формате
    """
    transformed_data = []
    
    for item in ltv_data:
        # Извлекаем данные LTV для разных дней (ltv_day_1, ltv_day_7, ltv_day_30 и т.д.)
        ltv_days = {}
        for key, value in item.items():
            if key.startswith("ltv_day_"):
                day_num = key.replace("ltv_day_", "")
                ltv_days[f"day_{day_num}"] = value
        
        transformed = {
            "date": item.get("date", ""),
            "media_source": item.get("media_source", ""),
            "campaign": item.get("campaign", ""),
            "installs": item.get("installs", 0),
            "ltv_days": ltv_days,
            "ltv_day_1": ltv_days.get("day_1", 0.0),
            "ltv_day_7": ltv_days.get("day_7", 0.0),
            "ltv_day_30": ltv_days.get("day_30", 0.0),
            "platform": item.get("platform", ""),
            "country_code": item.get("country_code", ""),
            "raw_data": item  # Сохраняем исходные данные
        }
        
        transformed_data.append(transformed)
    
    return transformed_data


def merge_appsflyer_data(
    installs_data: List[Dict[str, Any]], 
    events_data: List[Dict[str, Any]],
    retention_data: List[Dict[str, Any]] = None,
    ltv_data: List[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Объединяет различные типы данных AppsFlyer в общий формат для сохранения.
    
    Args:
        installs_data: Трансформированные данные об установках
        events_data: Трансформированные данные о событиях
        retention_data: Трансформированные данные об удержании (опционально)
        ltv_data: Трансформированные данные о LTV (опционально)
        
    Returns:
        Объединенные данные в стандартном формате
    """
    # Создаем словарь для группировки данных по дате, медиа-источнику и кампании
    grouped_data = {}
    
    # Обрабатываем данные об установках
    for item in installs_data:
        key = (item["date"], item["media_source"], item["campaign"])
        
        if key not in grouped_data:
            grouped_data[key] = {
                "date": item["date"],
                "media_source": item["media_source"],
                "campaign": item["campaign"],
                "installs": item["installs"],
                "clicks": item["clicks"],
                "impressions": item["impressions"],
                "cost": item["cost"],
                "cost_per_install": item["cost_per_install"],
                "purchases": 0,
                "revenue": 0.0,
                "platform": item.get("platform", ""),
                "country_code": item.get("country_code", "")
            }
        else:
            grouped_data[key].update({
                "installs": grouped_data[key]["installs"] + item["installs"],
                "clicks": grouped_data[key]["clicks"] + item["clicks"],
                "impressions": grouped_data[key]["impressions"] + item["impressions"],
                "cost": grouped_data[key]["cost"] + item["cost"]
            })
            # Пересчитываем CPI
            if grouped_data[key]["installs"] > 0:
                grouped_data[key]["cost_per_install"] = grouped_data[key]["cost"] / grouped_data[key]["installs"]
    
    # Обрабатываем данные о событиях (в первую очередь покупки)
    for item in events_data:
        key = (item["date"], item["media_source"], item["campaign"])
        
        if key not in grouped_data:
            grouped_data[key] = {
                "date": item["date"],
                "media_source": item["media_source"],
                "campaign": item["campaign"],
                "installs": 0,
                "clicks": 0,
                "impressions": 0,
                "cost": 0.0,
                "cost_per_install": 0.0,
                "purchases": item["purchases"],
                "revenue": item["revenue"],
                "platform": item.get("platform", ""),
                "country_code": item.get("country_code", "")
            }
        else:
            grouped_data[key].update({
                "purchases": grouped_data[key]["purchases"] + item["purchases"],
                "revenue": grouped_data[key]["revenue"] + item["revenue"]
            })
    
    # Обрабатываем данные о показателях удержания, если они предоставлены
    if retention_data:
        for item in retention_data:
            key = (item["date"], item["media_source"], item["campaign"])
            
            if key in grouped_data:
                grouped_data[key].update({
                    "day_1_retention": item["day_1_retention"],
                    "day_7_retention": item["day_7_retention"],
                    "day_30_retention": item["day_30_retention"]
                })
    
    # Обрабатываем данные о LTV, если они предоставлены
    if ltv_data:
        for item in ltv_data:
            key = (item["date"], item["media_source"], item["campaign"])
            
            if key in grouped_data:
                grouped_data[key].update({
                    "ltv_day_7": item["ltv_day_7"],
                    "ltv_day_30": item["ltv_day_30"]
                })
    
    # Преобразуем словарь обратно в список
    merged_data = list(grouped_data.values())
    
    return merged_data
