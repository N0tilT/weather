import pandas as pd
import os
from datetime import datetime
import json
from pathlib import Path
import numpy as np
import logging

CLEANED_DIR = "data/cleaned"
ENRICHED_DIR = "data/enriched"
def load_city_reference():
    """Загружает справочник городов из файла JSON."""
    ref_path = Path("config/cities_reference.json") # Меняем путь на JSON
    if ref_path.exists():
        with open(ref_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        logging.warning(f"Cities reference file {ref_path} not found. Using default values.")
        default_ref = {
            "Москва": {
                "federal_district": "Центральный",
                "timezone": "UTC+3",
                "population": 12500000,
                "tourism_season": "Круглогодично"
            },
            "Санкт-Петербург": {
                "federal_district": "Северо-Западный",
                "timezone": "UTC+3",
                "population": 5400000,
                "tourism_season": "Май-Сентябрь"
            },
            "Сочи": {
                "federal_district": "Южный",
                "timezone": "UTC+3",
                "population": 400000,
                "tourism_season": "Май-Октябрь"
            },
            "Казань": {
                "federal_district": "Приволжский",
                "timezone": "UTC+3",
                "population": 1300000,
                "tourism_season": "Апрель-Октябрь"
            },
            "Новосибирск": {
                "federal_district": "Сибирский",
                "timezone": "UTC+7",
                "population": 1600000,
                "tourism_season": "Июнь-Август"
            },
            "Екатеринбург": {
                "federal_district": "Уральский",
                "timezone": "UTC+5",
                "population": 1500000,
                "tourism_season": "Май-Сентябрь"
            },
            "Иркутск": {
                "federal_district": "Сибирский",
                "timezone": "UTC+8",
                "population": 620000,
                "tourism_season": "Май-Сентябрь"
            }
        }
        return default_ref


def calculate_comfort_index(row):
    """Calculate comfort index based on temperature, humidity and wind"""
    # Проверяем, есть ли необходимые данные
    if pd.isna(row['temperature']):
        return 0
    
    # Веса для разных факторов
    temp_score = max(0, min(40, 40 - abs(row['temperature'] - 22)))  # Оптимальная температура ~22°C
    humidity_score = max(0, 25 - (abs(row['humidity'] - 50) / 2)) if not pd.isna(row['humidity']) else 25  # Оптимальная влажность ~50%
    wind_score = max(0, 20 - (row['wind_speed'] * 2)) if not pd.isna(row['wind_speed']) else 20  # Скорость ветра влияет отрицательно
    pressure_score = max(0, 15 - (abs(row['pressure'] - 1013) / 10)) if not pd.isna(row['pressure']) else 15  # Нормальное давление ~1013 гПа
    
    # Общий комфорт-индекс
    comfort_index = temp_score + humidity_score + wind_score + pressure_score
    return round(min(100, comfort_index))

def get_weather_description(weather_code):
    """Преобразует WMO код погоды в текстовое описание"""
    if pd.isna(weather_code):
        return "Неизвестно"
    
    wmo_descriptions = {
        0: "Ясно",
        1: "В основном ясно",
        2: "Частично облачно",
        3: "Облачно",
        45: "Туман",
        51: "Морось слабая",
        53: "Морось умеренная",
        55: "Морось плотная",
        61: "Дождь слабый",
        63: "Дождь умеренный",
        65: "Дождь сильный",
        71: "Снег слабый",
        73: "Снег умеренный",
        75: "Снег сильный",
        80: "Ливень слабый",
        81: "Ливень умеренный",
        82: "Ливень сильный",
        95: "Гроза",
        96: "Гроза с градом",
        99: "Гроза с сильным градом"
    }
    return wmo_descriptions.get(int(weather_code), f"Погода код {int(weather_code)}")

def get_recommended_activity(comfort_index, weather_code, precipitation_total):
    """Возвращает рекомендуемую активность на основе погоды"""
    if pd.isna(weather_code):
        return "Неизвестно"
    
    weather_code = int(weather_code)
    
    # Проверяем осадки
    if precipitation_total and precipitation_total > 1.0:
        return "Музеи и закрытые развлечения"
    elif weather_code in [45, 51, 53, 55, 61, 63, 65, 80, 81, 82]:  # Осадки
        return "Музеи и закрытые развлечения"
    elif weather_code in [71, 73, 75]:  # Снег
        if comfort_index > 40:
            return "Зимние виды спорта"
        else:
            return "Домашний отдых"
    elif weather_code in [95, 96, 99]:  # Гроза
        return "Домашний отдых"
    elif weather_code in [0, 1, 2]:  # Ясно, почти ясно
        if 60 <= comfort_index <= 80:
            return "Прогулки и открытые мероприятия"
        elif comfort_index > 80:
            return "Открытые мероприятия с осторожностью (жара)"
        else:
            return "Музеи и закрытые развлечения"
    else:
        # Если комфортность высока и нет осадков
        if comfort_index >= 70:
            return "Прогулки и открытые мероприятия"
        elif comfort_index >= 40:
            return "Музеи и закрытые развлечения"
        else:
            return "Домашний отдых"

def is_tourist_season(city, month, tourism_season):
    """Проверяет, соответствует ли месяц туристическому сезону"""
    current_month = month
    
    if "Круглогодично" in tourism_season:
        return True
    elif "Май" in tourism_season and 5 <= current_month <= 9:
        return True
    elif "Июнь" in tourism_season and 6 <= current_month <= 8:
        return True
    elif "Апрель" in tourism_season and 4 <= current_month <= 10:
        return True
    elif "Июль" in tourism_season and 7 <= current_month <= 8:
        return True
    elif "Сентябрь" in tourism_season and 9 <= current_month <= 10:
        return True
    elif "Октябрь" in tourism_season and 10 <= current_month <= 11:
        return True
    return False

def enrich_data():
    today = datetime.now()
    timestamp = today.strftime("%Y%m%d")
    
    # Load cleaned data
    cleaned_file_path = f"{CLEANED_DIR}/weather_cleaned_{timestamp}.csv"
    if not os.path.exists(cleaned_file_path):
        raise FileNotFoundError(f"Cleaned data file not found: {cleaned_file_path}")
    
    cleaned_df = pd.read_csv(cleaned_file_path)
    
    # Load reference data
    city_reference = load_city_reference()
    
    # Добавляем колонку date из current_time (берем только дату)
    cleaned_df['date'] = pd.to_datetime(cleaned_df['current_time']).dt.date.astype(str)
    
    # Агрегируем данные по городу и дню (берем средние/последние значения)
    # Группируем по городу и дню, берем последние значения для текущих данных
    grouped = cleaned_df.groupby(['city_name', 'date']).agg({
        'latitude': 'first',  # координаты постоянны
        'longitude': 'first',
        'temperature': 'mean',  # средняя температура за день
        'feels_like': 'mean',
        'humidity': 'mean',
        'pressure': 'mean',
        'wind_speed': 'mean',
        'wind_direction': 'mean',
        'wind_gusts': 'max',  # максимальный порыв
        'precipitation': 'sum',  # суммарные осадки за день
        'cloud_cover': 'mean',
        'weather_code': 'first',  # берем первый код погоды (или моду)
        'visibility': 'mean',
        'is_day': 'first',
        'collection_time': 'first',
        'current_time': 'first',
        'daily_temp_max': 'max',  # берем из суточных данных
        'daily_temp_min': 'min',
        'daily_precipitation': 'first',  # берем из суточных данных
        'daily_weather_code': 'first'
    }).reset_index()
    
    # Add reference data and calculations
    enriched_data = []
    for _, row in grouped.iterrows():
        city_name = row['city_name']
        city_ref = city_reference.get(city_name, {})
        
        if city_ref:
            enriched_row = row.to_dict()
            enriched_row.update(city_ref)
            
            # Calculate comfort index (используем среднюю температуру за день)
            enriched_row['comfort_index'] = calculate_comfort_index(row)
            
            # Get weather description
            enriched_row['weather_description'] = get_weather_description(row['weather_code'])
            
            # Get recommended activity
            # Используем daily_precipitation если доступно, иначе precipitation
            precip_value = row['daily_precipitation'] if not pd.isna(row['daily_precipitation']) else row['precipitation']
            enriched_row['recommended_activity'] = get_recommended_activity(
                enriched_row['comfort_index'], 
                row['weather_code'],
                precip_value
            )
            
            # Check tourist season match
            date_obj = datetime.strptime(row['date'], '%Y-%m-%d')
            enriched_row['tourist_season_match'] = is_tourist_season(
                city_name, 
                date_obj.month, 
                city_ref.get("tourism_season", "")
            )
            
            # Additional weather insights
            if not pd.isna(row['daily_weather_code']):
                enriched_row['uv_index_risk'] = "Высокий" if row['daily_weather_code'] > 7 else "Средний" if row['daily_weather_code'] > 3 else "Низкий"
            else:
                enriched_row['uv_index_risk'] = "Неизвестно"
            
            # Determine precipitation type
            if pd.isna(row['temperature']):
                enriched_row['precipitation_type'] = "Неизвестно"
            elif row['temperature'] < 0:
                enriched_row['precipitation_type'] = "Снег"
            elif precip_value and precip_value > 0:
                enriched_row['precipitation_type'] = "Дождь"
            else:
                enriched_row['precipitation_type'] = "Без осадков"
            
            enriched_data.append(enriched_row)
    
    # Save enriched data
    os.makedirs(ENRICHED_DIR, exist_ok=True)
    enriched_df = pd.DataFrame(enriched_data)
    enriched_df.to_csv(f"{ENRICHED_DIR}/weather_enriched_{timestamp}.csv", index=False)
    
    # Save reference data if doesn't exist
    ref_df = pd.DataFrame.from_dict(city_reference, orient='index').reset_index()
    ref_df.rename(columns={'index': 'city_name'}, inplace=True)
    ref_df.to_csv(f"{ENRICHED_DIR}/cities_reference.csv", index=False)

if __name__ == "__main__":
    enrich_data()