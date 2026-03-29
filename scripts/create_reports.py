# scripts/create_reports.py
import pandas as pd
import os
import sqlite3
from datetime import datetime
from pathlib import Path

# Пути согласно заданию
DB_PATH = "./rp5/weather_enriched/enriched.db"
TABLE_NAME = "enriched_observations"  # Исправлено имя таблицы
AGGREGATED_DIR = "../data/aggregated/"

# Параметры батч-обработки
BATCH_SIZE = 100000  # Количество строк за один раз

def _save_empty_reports():
    """Вспомогательная функция для создания пустых отчетов"""
    os.makedirs(AGGREGATED_DIR, exist_ok=True)
    empty_df = pd.DataFrame()
    empty_df.to_csv(f"{AGGREGATED_DIR}/city_tourism_rating.csv", index=False)
    empty_df.to_csv(f"{AGGREGATED_DIR}/federal_districts_summary.csv", index=False)
    empty_df.to_csv(f"{AGGREGATED_DIR}/travel_recommendations.csv", index=False)

def _get_connection():
    """Создание соединения с БД"""
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _get_today_data_batched(conn, today_str):
    """
    Чтение данных за сегодня по батчам.
    Возвращает объединенный DataFrame или пустой, если данных нет.
    """
    query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE date = ?
    """
    
    chunks = []
    try:
        for chunk in pd.read_sql_query(query, conn, params=(today_str,), chunksize=BATCH_SIZE):
            if not chunk.empty:
                chunks.append(chunk)
                print(f"  Загружено батч: {len(chunk)} строк")
    except Exception as e:
        print(f"Ошибка при чтении данных за сегодня: {e}")
        return pd.DataFrame()
    
    if not chunks:
        return pd.DataFrame()
    
    return pd.concat(chunks, ignore_index=True)

def _get_latest_data_batched(conn):
    """
    Получение последних данных по каждому городу (если нет данных за сегодня).
    Использует SQL для эффективной выборки.
    """
    query = f"""
        SELECT t1.* FROM {TABLE_NAME} t1
        INNER JOIN (
            SELECT city_name, MAX(date) as max_date
            FROM {TABLE_NAME}
            GROUP BY city_name
        ) t2 ON t1.city_name = t2.city_name AND t1.date = t2.max_date
    """
    
    chunks = []
    try:
        for chunk in pd.read_sql_query(query, conn, chunksize=BATCH_SIZE):
            if not chunk.empty:
                chunks.append(chunk)
                print(f"  Загружено батч (последние данные): {len(chunk)} строк")
    except Exception as e:
        print(f"Ошибка при чтении последних данных: {e}")
        return pd.DataFrame()
    
    if not chunks:
        return pd.DataFrame()
    
    return pd.concat(chunks, ignore_index=True)

def _aggregate_district_summary(current_data):
    """Агрегация по федеральным округам"""
    if current_data.empty or 'federal_district' not in current_data.columns or 'comfort_index' not in current_data.columns:
        return pd.DataFrame(columns=[
            'federal_district', 'avg_temperature', 'comfortable_cities',
            'total_cities', 'comfort_ratio', 'general_recommendation'
        ])
    
    district_summary = current_data.groupby('federal_district').agg(
        avg_temperature=('temperature', 'mean'),
        comfortable_cities=('comfort_index', lambda x: (x > 50).sum()),
        total_cities=('city_name', 'count'),
        avg_comfort=('comfort_index', 'mean')
    ).reset_index()
    
    district_summary['comfort_ratio'] = (
        district_summary['comfortable_cities'] / district_summary['total_cities'] * 100
    ).round(1)
    
    district_summary['general_recommendation'] = district_summary['avg_comfort'].apply(
        lambda x: "Активно продавать туры" if x > 60 else
                 ("Умеренно продавать" if x > 40 else "Сосредоточиться на внутреннем туризме")
    )
    
    return district_summary

def _generate_travel_recommendations(current_data):
    """Генерация рекомендаций для турагентств"""
    travel_recommendations = []
    
    if current_data.empty or 'comfort_index' not in current_data.columns:
        return pd.DataFrame(travel_recommendations)
    
    # Топ-3 города
    top_cities = current_data.nlargest(3, 'comfort_index')
    for _, city in top_cities.iterrows():
        travel_recommendations.append({
            'category': 'top_destination',
            'city': city['city_name'],
            'reason': f"Высокий комфорт-индекс ({city['comfort_index']})",
            'recommendation': f"Активно продавать туры, акцент на {city.get('recommended_activity', 'туризм')}"
        })
    
    # Города для домашнего отдыха
    low_comfort = current_data[current_data['comfort_index'] < 40]
    for _, city in low_comfort.iterrows():
        reason_parts = []
        if city.get('temperature', 0) < 0:
            reason_parts.append("низкая температура")
        if city.get('wind_speed', 0) > 10:
            reason_parts.append("сильный ветер")
        if city.get('humidity', 0) > 80:
            reason_parts.append("высокая влажность")
        if city.get('weather_code', 0) in [61, 63, 65, 71, 73, 75, 95, 96, 99]:
            reason_parts.append("неблагоприятные погодные условия")
        
        reason = ", ".join(reason_parts) if reason_parts else "низкий комфорт"
        
        travel_recommendations.append({
            'category': 'stay_home',
            'city': city['city_name'],
            'reason': reason,
            'recommendation': "Рекомендовать перенос поездки или предложить альтернативные направления"
        })
    
    # Специальные рекомендации
    if 'precipitation' in current_data.columns:
        rainy_cities = current_data[current_data['precipitation'] > 0.5]
        if not rainy_cities.empty:
            travel_recommendations.append({
                'category': 'special',
                'city': 'Некоторые города',
                'reason': "обнаружены осадки",
                'recommendation': "Рекомендовать туристам взять зонт/дождевик"
            })
    
    if 'temperature' in current_data.columns:
        cold_cities = current_data[current_data['temperature'] < 0]
        if not cold_cities.empty:
            travel_recommendations.append({
                'category': 'special',
                'city': 'Холодные регионы',
                'reason': "отрицательная температура",
                'recommendation': "Рекомендовать теплую одежду"
            })
    
    return pd.DataFrame(travel_recommendations)

def create_reports():
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    timestamp = today.strftime("%Y%m%d")
    
    print(f"Начало создания отчетов: {today_str}")
    print(f"Путь к БД: {DB_PATH}")
    
    # Проверка существования БД
    if not os.path.exists(DB_PATH):
        print(f"Warning: Database file {DB_PATH} not found.")
        _save_empty_reports()
        return
    
    conn = _get_connection()
    if conn is None:
        print("Warning: Could not connect to database.")
        _save_empty_reports()
        return
    
    try:
        # Попытка загрузки данных за сегодня
        print(f"Загрузка данных за {today_str}...")
        current_data = _get_today_data_batched(conn, today_str)
        
        # Если нет данных за сегодня, загружаем последние по каждому городу
        if current_data.empty:
            print(f"Данных за {today_str} не найдено. Загрузка последних данных по городам...")
            current_data = _get_latest_data_batched(conn)
        
        if current_data.empty:
            print("Warning: No data found in database.")
            _save_empty_reports()
            return
        
        print(f"Всего загружено строк для обработки: {len(current_data)}")
        
        # Витрина 1: Рейтинг городов для туризма
        print("Создание витрины 1: Рейтинг городов...")
        if not current_data.empty and 'comfort_index' in current_data.columns:
            # Выбираем только существующие колонки
            base_columns = ['city_name', 'temperature', 'comfort_index', 'recommended_activity']
            city_rating = current_data[base_columns].copy()
            
            # Создаем weather_recommendation на основе comfort_index (этой колонки нет в БД)
            city_rating['weather_recommendation'] = city_rating['comfort_index'].apply(
                lambda x: "Рекомендуется" if x > 60 else "Не рекомендуется"
            )
            
            city_rating = city_rating.sort_values('comfort_index', ascending=False)
            city_rating['Рейтинг'] = range(1, len(city_rating) + 1)
            city_rating = city_rating[[
                'Рейтинг', 'city_name', 'temperature', 'comfort_index',
                'recommended_activity', 'weather_recommendation'
            ]]
        else:
            city_rating = pd.DataFrame(columns=[
                'Рейтинг', 'city_name', 'temperature', 'comfort_index',
                'recommended_activity', 'weather_recommendation'
            ])
        
        city_rating.to_csv(f"{AGGREGATED_DIR}/city_tourism_rating.csv", index=False)
        print(f"  Сохранено: {len(city_rating)} городов")
        
        # Витрина 2: Сводка по федеральным округам
        print("Создание витрины 2: Сводка по федеральным округам...")
        district_summary = _aggregate_district_summary(current_data)
        district_summary.to_csv(f"{AGGREGATED_DIR}/federal_districts_summary.csv", index=False)
        print(f"  Сохранено: {len(district_summary)} округов")
        
        # Витрина 3: Отчет для турагентств
        print("Создание витрины 3: Рекомендации для турагентств...")
        travel_recommendations = _generate_travel_recommendations(current_data)
        travel_recommendations.to_csv(f"{AGGREGATED_DIR}/travel_recommendations.csv", index=False)
        print(f"  Сохранено: {len(travel_recommendations)} рекомендаций")
        
        print("Отчеты успешно созданы!")
        
    except Exception as e:
        print(f"Error during report creation: {e}")
        import traceback
        traceback.print_exc()
        _save_empty_reports()
    finally:
        conn.close()

if __name__ == "__main__":
    os.makedirs(AGGREGATED_DIR, exist_ok=True)
    create_reports()