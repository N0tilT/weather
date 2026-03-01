# scripts/create_reports.py
import pandas as pd
import os
from datetime import datetime
import numpy as np
from pathlib import Path # Добавим импорт Path

ENRICHED_DIR = "data/enriched"
AGGREGATED_DIR = "data/aggregated"

def create_reports():
    today = datetime.now()
    timestamp = today.strftime("%Y%m%d")
    
    # Load enriched data
    enriched_file_path = f"{ENRICHED_DIR}/weather_enriched_{timestamp}.csv"
    enriched_path = Path(enriched_file_path)
    
    if not enriched_path.exists():
        print(f"Warning: Enriched data file {enriched_file_path} not found. Creating empty reports.")
        # Создаем пустые отчеты или используем данные за вчерашний день как fallback
        # Пока просто создадим пустые файлы
        os.makedirs(AGGREGATED_DIR, exist_ok=True)
        empty_df = pd.DataFrame()
        empty_df.to_csv(f"{AGGREGATED_DIR}/city_tourism_rating.csv", index=False)
        empty_df.to_csv(f"{AGGREGATED_DIR}/federal_districts_summary.csv", index=False)
        empty_df.to_csv(f"{AGGREGATED_DIR}/travel_recommendations.csv", index=False)
        return

    # --- Проверим, пуст ли файл ---
    if enriched_path.stat().st_size == 0:
        print(f"Warning: Enriched data file {enriched_file_path} is empty. Creating empty reports.")
        os.makedirs(AGGREGATED_DIR, exist_ok=True)
        empty_df = pd.DataFrame()
        empty_df.to_csv(f"{AGGREGATED_DIR}/city_tourism_rating.csv", index=False)
        empty_df.to_csv(f"{AGGREGATED_DIR}/federal_districts_summary.csv", index=False)
        empty_df.to_csv(f"{AGGREGATED_DIR}/travel_recommendations.csv", index=False)
        return

    try:
        enriched_df = pd.read_csv(enriched_file_path)
    except pd.errors.EmptyDataError:
        print(f"Warning: Enriched data file {enriched_file_path} has no data rows. Creating empty reports.")
        os.makedirs(AGGREGATED_DIR, exist_ok=True)
        empty_df = pd.DataFrame()
        empty_df.to_csv(f"{AGGREGATED_DIR}/city_tourism_rating.csv", index=False)
        empty_df.to_csv(f"{AGGREGATED_DIR}/federal_districts_summary.csv", index=False)
        empty_df.to_csv(f"{AGGREGATED_DIR}/travel_recommendations.csv", index=False)
        return

    # В этом файле уже есть колонка 'date' - используем только сегодняшние данные
    today_str = today.strftime("%Y-%m-%d")
    # Проверим, есть ли колонка 'date'
    if 'date' in enriched_df.columns:
        current_data = enriched_df[enriched_df['date'] == today_str].copy()
    else:
        # Если нет колонки date, используем все данные (например, если это старый формат)
        current_data = enriched_df.copy()
    
    # Если нет данных за сегодня, используем все данные (последние)
    if current_data.empty:
        # Берем последние данные по дате для каждого города, если колонка 'date' существует
        if 'date' in enriched_df.columns and not enriched_df.empty:
             try:
                 enriched_df['date_parsed'] = pd.to_datetime(enriched_df['date'])
                 current_data = enriched_df.loc[enriched_df.groupby('city_name')['date_parsed'].idxmax()]
             except:
                 # Если не получилось, используем все
                 current_data = enriched_df
        else:
            current_data = enriched_df # Используем все, если 'date' нет или df пуст

    # Витрина 1: Рейтинг городов для туризма
    if not current_data.empty and 'comfort_index' in current_data.columns:
        city_rating = current_data[[
            'city_name', 'temperature', 'comfort_index', # Используем 'temperature', а не 'temperature_avg'
            'recommended_activity', 'weather_recommendation' # 'weather_recommendation' может не существовать, проверим
        ]].copy()
        
        # Убедимся, что 'weather_recommendation' существует
        if 'weather_recommendation' not in city_rating.columns:
            city_rating['weather_recommendation'] = city_rating['comfort_index'].apply(lambda x: "Рекомендуется" if x > 60 else "Не рекомендуется")
        
        city_rating = city_rating.sort_values('comfort_index', ascending=False)
        city_rating['Рейтинг'] = range(1, len(city_rating) + 1)
        
        city_rating = city_rating[['Рейтинг', 'city_name', 'temperature', 'comfort_index', 'recommended_activity', 'weather_recommendation']] # Используем 'temperature'
    else:
        # Создаем пустой датафрейм с нужными колонками
        city_rating = pd.DataFrame(columns=['Рейтинг', 'city_name', 'temperature', 'comfort_index', 'recommended_activity', 'weather_recommendation']) # Используем 'temperature'
    
    city_rating.to_csv(f"{AGGREGATED_DIR}/city_tourism_rating.csv", index=False)
    
    # Витрина 2: Сводка по федеральным округам
    if not current_data.empty and 'federal_district' in current_data.columns and 'comfort_index' in current_data.columns:
        district_summary = current_data.groupby('federal_district').agg(
            avg_temperature=('temperature', 'mean'), # Используем 'temperature'
            comfortable_cities=('comfort_index', lambda x: (x > 50).sum()),
            total_cities=('city_name', 'count'),
            avg_comfort=('comfort_index', 'mean')
        ).reset_index()
        
        district_summary['comfort_ratio'] = (district_summary['comfortable_cities'] / 
                                          district_summary['total_cities'] * 100).round(1)
        
        district_summary['general_recommendation'] = district_summary['avg_comfort'].apply(
            lambda x: "Активно продавать туры" if x > 60 else 
                     ("Умеренно продавать" if x > 40 else "Сосредоточиться на внутреннем туризме")
        )
    else:
        district_summary = pd.DataFrame(columns=['federal_district', 'avg_temperature', 'comfortable_cities', 'total_cities', 'comfort_ratio', 'general_recommendation'])
    
    district_summary.to_csv(f"{AGGREGATED_DIR}/federal_districts_summary.csv", index=False)
    
    # Витрина 3: Отчет для турагентств
    travel_recommendations = []
    
    if not current_data.empty and 'comfort_index' in current_data.columns:
        # Топ-3 города
        if 'comfort_index' in current_data.columns:
            top_cities = current_data.nlargest(3, 'comfort_index')
            for _, city in top_cities.iterrows():
                travel_recommendations.append({
                    'category': 'top_destination',
                    'city': city['city_name'],
                    'reason': f"Высокий комфорт-индекс ({city['comfort_index']})",
                    'recommendation': f"Активно продавать туры, акцент на {city['recommended_activity']}"
                })
        
        # Города для домашнего отдыха
        if 'comfort_index' in current_data.columns:
            low_comfort = current_data[current_data['comfort_index'] < 40]
            for _, city in low_comfort.iterrows():
                reason_parts = []
                if 'temperature' in city and city['temperature'] < 0:
                    reason_parts.append("низкая температура")
                if 'wind_speed' in city and city['wind_speed'] > 10:
                    reason_parts.append("сильный ветер")
                if 'humidity' in city and city['humidity'] > 80:
                    reason_parts.append("высокая влажность")
                if 'weather_code' in city and city['weather_code'] in [61, 63, 65, 71, 73, 75, 95, 96, 99]:
                    reason_parts.append("неблагоприятные погодные условия")
                
                reason = ", ".join(reason_parts) if reason_parts else "низкий комфорт"
                
                travel_recommendations.append({
                    'category': 'stay_home',
                    'city': city['city_name'],
                    'reason': reason,
                    'recommendation': "Рекомендовать перенос поездки или предложить альтернативные направления"
                })
    
    # Специальные рекомендации
    if not current_data.empty:
        # Проверяем осадки
        if 'precipitation' in current_data.columns:
            rainy_cities = current_data[current_data['precipitation'] > 0.5]
            if not rainy_cities.empty:
                travel_recommendations.append({
                    'category': 'special',
                    'city': 'Некоторые города',
                    'reason': "обнаружены осадки",
                    'recommendation': "Рекомендовать туристам взять зонт/дождевик"
                })
        
        # Проверяем температуру
        if 'temperature' in current_data.columns:
            cold_cities = current_data[current_data['temperature'] < 0]
            if not cold_cities.empty:
                travel_recommendations.append({
                    'category': 'special',
                    'city': 'Холодные регионы',
                    'reason': "отрицательная температура",
                    'recommendation': "Рекомендовать теплую одежду"
                })
    
    pd.DataFrame(travel_recommendations).to_csv(
        f"{AGGREGATED_DIR}/travel_recommendations.csv", 
        index=False
    )

if __name__ == "__main__":
    os.makedirs(AGGREGATED_DIR, exist_ok=True)
    create_reports()