import pandas as pd
import json
import os
from datetime import datetime, timedelta
import logging
from pathlib import Path
import numpy as np

RAW_DIR = "data/raw/openmeteo_api"
CLEANED_DIR = "data/cleaned"

def load_city_coordinates():
    coords_path = Path("config/city_coordinates.json")
    if coords_path.exists():
        with open(coords_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def clean_data():
    today = datetime.now()
    raw_dir = f"{RAW_DIR}/{today.year}/{today.month:02d}/{today.day:02d}"
    
    # --- Добавим логирование ---
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Looking for raw data in: {raw_dir}")
    
    if not Path(raw_dir).exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")
    
    all_data_files = [f for f in os.listdir(raw_dir) if f.endswith(".json") and not f.startswith("collection_log")]
    logger.info(f"Found {len(all_data_files)} raw data files: {all_data_files}")
    
    all_data = []
    for file in all_data_files:
        file_path = f"{raw_dir}/{file}"
        logger.info(f"Processing file: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_data.append(data)
            logger.info(f"Successfully loaded data from {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON from {file_path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading {file_path}: {e}")
    
    logger.info(f"Total records loaded: {len(all_data)}")
    
    cleaned_records = []
    validation_errors = []
    
    for i, record in enumerate(all_data):
        logger.debug(f"Processing record {i+1}/{len(all_data)}")
        try:
            # --- Добавляем проверку наличия _metadata ---
            if '_metadata' not in record:
                error_msg = f"Record {i+1} missing '_metadata' key"
                validation_errors.append(error_msg)
                logger.warning(error_msg)
                continue
            
            metadata = record['_metadata']
            
            # --- Добавляем проверку наличия city_name в metadata ---
            if 'city_name' not in metadata:
                error_msg = f"Record {i+1} metadata missing 'city_name' key: {metadata.get('city_query', 'Unknown query')}"
                validation_errors.append(error_msg)
                logger.warning(error_msg)
                continue
            
            city_name = metadata['city_name']
            if not city_name:
                error_msg = f"Record {i+1} city name is empty for record with city_query: {metadata.get('city_query', 'Unknown query')}"
                validation_errors.append(error_msg)
                logger.warning(error_msg)
                continue
            
            # --- Проверяем наличие других обязательных данных ---
            current = record.get('current', {})
            hourly = record.get('hourly', {})
            daily = record.get('daily', {})
            
            # --- Проверим, есть ли вообще какие-то данные ---
            if not hourly or not daily:
                error_msg = f"Record {i+1} ({city_name}): Missing hourly or daily data blocks"
                validation_errors.append(error_msg)
                logger.warning(error_msg)
                continue
            
            # Базовая валидация
            temp = current.get('temperature_2m')
            if temp is None or not (-50 <= temp <= 60):
                error_msg = f"Record {i+1} ({city_name}): Temperature out of range: {temp}"
                validation_errors.append(error_msg)
                logger.warning(error_msg)
                continue
            
            # Обрабатываем данные для каждого дня в диапазоне
            hourly_times = hourly.get('time', [])
            daily_times = daily.get('time', [])
            
            if not daily_times:
                error_msg = f"Record {i+1} ({city_name}): No daily data found"
                validation_errors.append(error_msg)
                logger.warning(error_msg)
                continue
            
            logger.debug(f"Record {i+1} ({city_name}): Processing {len(daily_times)} daily entries")
            
            # --- Проверим, какие hourly переменные доступны ---
            available_hourly_vars = list(hourly.keys())
            logger.debug(f"Record {i+1} ({city_name}): Available hourly variables: {available_hourly_vars}")
            
            # --- Определим, какие переменные мы можем использовать ---
            has_cloud_cover = 'cloud_cover' in available_hourly_vars
            has_is_day = 'is_day' in available_hourly_vars
            has_wind_gusts = 'wind_gusts_10m' in available_hourly_vars
            has_visibility = 'visibility' in available_hourly_vars
            has_relative_humidity = 'relative_humidity_2m' in available_hourly_vars
            has_apparent_temp = 'apparent_temperature' in available_hourly_vars
            has_pressure = 'pressure_msl' in available_hourly_vars
            has_wind_speed = 'wind_speed_10m' in available_hourly_vars
            has_temperature = 'temperature_2m' in available_hourly_vars
            
            # Создаем записи для каждого дня
            for day_idx, daily_time in enumerate(daily_times):
                # Проверяем, есть ли соответствующие данные
                if day_idx >= len(daily.get('temperature_2m_max', [])):
                    logger.debug(f"Record {i+1} ({city_name}): Skipping day {day_idx} - no matching daily data")
                    continue
                    
                # Находим индекс текущего дня в hourly данных
                daily_date = daily_time[:10]  # YYYY-MM-DD
                hourly_indices_for_day = [
                    j for j, time_str in enumerate(hourly_times)
                    if time_str.startswith(daily_date)
                ]
                
                # --- Проверим, есть ли почасовые данные для этого дня ---
                if not hourly_indices_for_day:
                     logger.debug(f"Record {i+1} ({city_name}): No hourly data found for date {daily_date}")
                     # Все равно создадим запись с минимальными данными из daily, если возможно
                     cleaned = {
                         'city_name': city_name,
                         'latitude': metadata['coordinates']['lat'],
                         'longitude': metadata['coordinates']['lon'],
                         'date': daily_date,
                         'temperature': daily.get('temperature_2m_mean', [])[day_idx] if day_idx < len(daily.get('temperature_2m_mean', [])) else None,
                         'feels_like': daily.get('apparent_temperature_mean', [])[day_idx] if day_idx < len(daily.get('apparent_temperature_mean', [])) else None,
                         'humidity': daily.get('relative_humidity_2m_mean', [])[day_idx] if day_idx < len(daily.get('relative_humidity_2m_mean', [])) else None,
                         'pressure': daily.get('surface_pressure_mean', [])[day_idx] if day_idx < len(daily.get('surface_pressure_mean', [])) else None,
                         'wind_speed': daily.get('wind_speed_10m_mean', [])[day_idx] if day_idx < len(daily.get('wind_speed_10m_mean', [])) else None,
                         'wind_direction': daily.get('wind_direction_10m_dominant', [])[day_idx] if day_idx < len(daily.get('wind_direction_10m_dominant', [])) else None,
                         'precipitation': daily.get('precipitation_sum', [])[day_idx] if day_idx < len(daily.get('precipitation_sum', [])) else None,
                         'precipitation_hours': daily.get('precipitation_hours', [])[day_idx] if day_idx < len(daily.get('precipitation_hours', [])) else None,
                         'weather_code': daily.get('weather_code', [])[day_idx] if day_idx < len(daily.get('weather_code', [])) else None,
                         'uv_index_max': daily.get('uv_index_max', [])[day_idx] if day_idx < len(daily.get('uv_index_max', [])) else None,
                         'sunshine_duration': daily.get('sunshine_duration', [])[day_idx] if day_idx < len(daily.get('sunshine_duration', [])) else None,
                         'daylight_duration': daily.get('daylight_duration', [])[day_idx] if day_idx < len(daily.get('daylight_duration', [])) else None,
                         'collection_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         'current_time': daily_time,
                         'data_source': 'extended_range',
                         'daily_temp_max': daily.get('temperature_2m_max', [])[day_idx] if day_idx < len(daily.get('temperature_2m_max', [])) else None,
                         'daily_temp_min': daily.get('temperature_2m_min', [])[day_idx] if day_idx < len(daily.get('temperature_2m_min', [])) else None,
                         'daily_precipitation': daily.get('precipitation_sum', [])[day_idx] if day_idx < len(daily.get('precipitation_sum', [])) else None,
                         'daily_weather_code': daily.get('weather_code', [])[day_idx] if day_idx < len(daily.get('weather_code', [])) else None,
                         # --- Используем daily значения для отсутствующих hourly или None ---
                         'cloud_cover': daily.get('cloud_cover_mean', [])[day_idx] if day_idx < len(daily.get('cloud_cover_mean', [])) else None,
                         'is_day': daily.get('is_day_mean', [])[day_idx] if day_idx < len(daily.get('is_day_mean', [])) else None,
                         'wind_gusts': daily.get('wind_gusts_10m_max', [])[day_idx] if day_idx < len(daily.get('wind_gusts_10m_max', [])) else None,
                         'visibility': daily.get('visibility_mean', [])[day_idx] if day_idx < len(daily.get('visibility_mean', [])) else None,
                         'rain_sum': daily.get('rain_sum', [])[day_idx] if day_idx < len(daily.get('rain_sum', [])) else None,
                         'showers_sum': daily.get('showers_sum', [])[day_idx] if day_idx < len(daily.get('showers_sum', [])) else None,
                         'snowfall_sum': daily.get('snowfall_sum', [])[day_idx] if day_idx < len(daily.get('snowfall_sum', [])) else None,
                     }
                     cleaned_records.append(cleaned)
                     continue
                
                # --- Собираем средние значения за день из hourly данных, проверяя наличие переменных ---
                daily_temps = [hourly['temperature_2m'][j] for j in hourly_indices_for_day 
                              if has_temperature and j < len(hourly['temperature_2m']) and 
                                 hourly['temperature_2m'][j] is not None] if has_temperature else []
                
                daily_humidity = [hourly['relative_humidity_2m'][j] for j in hourly_indices_for_day 
                                 if has_relative_humidity and j < len(hourly['relative_humidity_2m']) and 
                                    hourly['relative_humidity_2m'][j] is not None] if has_relative_humidity else []
                
                daily_wind_speed = [hourly['wind_speed_10m'][j] for j in hourly_indices_for_day 
                                   if has_wind_speed and j < len(hourly['wind_speed_10m']) and 
                                      hourly['wind_speed_10m'][j] is not None] if has_wind_speed else []
                
                # --- НОВОЕ: Собираем cloud_cover, is_day, wind_gusts за день, проверяя наличие ---
                daily_cloud_cover = [hourly['cloud_cover'][j] for j in hourly_indices_for_day 
                                   if has_cloud_cover and j < len(hourly['cloud_cover']) and 
                                      hourly['cloud_cover'][j] is not None] if has_cloud_cover else []
                
                daily_is_day = [hourly['is_day'][j] for j in hourly_indices_for_day 
                              if has_is_day and j < len(hourly['is_day']) and 
                                 hourly['is_day'][j] is not None] if has_is_day else []
                
                daily_wind_gusts = [hourly['wind_gusts_10m'][j] for j in hourly_indices_for_day 
                                  if has_wind_gusts and j < len(hourly['wind_gusts_10m']) and 
                                     hourly['wind_gusts_10m'][j] is not None] if has_wind_gusts else []
                
                daily_visibility = [hourly['visibility'][j] for j in hourly_indices_for_day 
                                  if has_visibility and j < len(hourly['visibility']) and 
                                     hourly['visibility'][j] is not None] if has_visibility else []
                
                daily_feels_like = [hourly['apparent_temperature'][j] for j in hourly_indices_for_day 
                                  if has_apparent_temp and j < len(hourly['apparent_temperature']) and 
                                     hourly['apparent_temperature'][j] is not None] if has_apparent_temp else []
                
                daily_pressure = [hourly['pressure_msl'][j] for j in hourly_indices_for_day 
                                if has_pressure and j < len(hourly['pressure_msl']) and 
                                   hourly['pressure_msl'][j] is not None] if has_pressure else []
                
                # Создаем очищенную запись для каждого дня
                cleaned = {
                    'city_name': city_name,  # Используем city_name из метаданных
                    'latitude': metadata['coordinates']['lat'],
                    'longitude': metadata['coordinates']['lon'],
                    'date': daily_date,
                    'temperature': round(np.mean(daily_temps), 1) if daily_temps else None,
                    'feels_like': round(np.mean(daily_feels_like), 1) if daily_feels_like else None,
                    'humidity': round(np.mean(daily_humidity), 1) if daily_humidity else None,
                    'pressure': round(np.mean(daily_pressure), 1) if daily_pressure else None,
                    'wind_speed': round(np.mean(daily_wind_speed), 1) if daily_wind_speed else None,
                    'wind_direction': daily.get('wind_direction_10m_dominant', [])[day_idx] if day_idx < len(daily.get('wind_direction_10m_dominant', [])) else None,
                    'precipitation': daily.get('precipitation_sum', [])[day_idx] if day_idx < len(daily.get('precipitation_sum', [])) else None,
                    'precipitation_hours': daily.get('precipitation_hours', [])[day_idx] if day_idx < len(daily.get('precipitation_hours', [])) else None,
                    'weather_code': daily.get('weather_code', [])[day_idx] if day_idx < len(daily.get('weather_code', [])) else None,
                    'uv_index_max': daily.get('uv_index_max', [])[day_idx] if day_idx < len(daily.get('uv_index_max', [])) else None,
                    'sunshine_duration': daily.get('sunshine_duration', [])[day_idx] if day_idx < len(daily.get('sunshine_duration', [])) else None,
                    'daylight_duration': daily.get('daylight_duration', [])[day_idx] if day_idx < len(daily.get('daylight_duration', [])) else None,
                    'visibility': round(np.mean(daily_visibility), 1) if daily_visibility else None,
                    'collection_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'current_time': daily_time,
                    'data_source': 'extended_range',
                    'daily_temp_max': daily.get('temperature_2m_max', [])[day_idx] if day_idx < len(daily.get('temperature_2m_max', [])) else None,
                    'daily_temp_min': daily.get('temperature_2m_min', [])[day_idx] if day_idx < len(daily.get('temperature_2m_min', [])) else None,
                    'daily_precipitation': daily.get('precipitation_sum', [])[day_idx] if day_idx < len(daily.get('precipitation_sum', [])) else None,
                    'daily_weather_code': daily.get('weather_code', [])[day_idx] if day_idx < len(daily.get('weather_code', [])) else None,
                    # --- ДОБАВЛЕНО с проверкой ---
                    'cloud_cover': round(np.mean(daily_cloud_cover), 1) if daily_cloud_cover else None,
                    'is_day': np.mean(daily_is_day) if daily_is_day else None, # Среднее (обычно 0 или 1)
                    'wind_gusts': round(np.mean(daily_wind_gusts), 1) if daily_wind_gusts else None,
                }
                
                # Добавляем статистики по осадкам
                if day_idx < len(daily.get('rain_sum', [])):
                    cleaned['rain_sum'] = daily['rain_sum'][day_idx]
                if day_idx < len(daily.get('showers_sum', [])):
                    cleaned['showers_sum'] = daily['showers_sum'][day_idx]
                if day_idx < len(daily.get('snowfall_sum', [])):
                    cleaned['snowfall_sum'] = daily['snowfall_sum'][day_idx]
                
                cleaned_records.append(cleaned)
        
        except KeyError as e:
            # Теперь, если ошибка в другом месте, у нас есть защита
            # Получаем имя города из метаданных, если возможно, иначе используем 'Unknown'
            city_name_for_error = "Unknown"
            if '_metadata' in record and isinstance(record['_metadata'], dict):
                city_name_for_error = record['_metadata'].get('city_name', record['_metadata'].get('city_query', 'Unknown query'))
            error_msg = f"Record {i+1} ({city_name_for_error}): Unexpected KeyError - {e}"
            validation_errors.append(error_msg)
            logger.error(error_msg)
        except Exception as e:
            # Та же логика для общих исключений
            city_name_for_error = "Unknown"
            if '_metadata' in record and isinstance(record['_metadata'], dict):
                city_name_for_error = record['_metadata'].get('city_name', record['_metadata'].get('city_query', 'Unknown query'))
            error_msg = f"Record {i+1} ({city_name_for_error}): Error processing - {str(e)}"
            validation_errors.append(error_msg)
            logger.error(error_msg)
    
    logger.info(f"Total cleaned records: {len(cleaned_records)}")
    logger.info(f"Total validation errors: {len(validation_errors)}")
    
    # --- Основное изменение: Проверим, есть ли данные перед сохранением ---
    if not cleaned_records:
        logger.warning("No valid records were created. Creating an empty CSV with headers.")
        # Создаем пустой DataFrame с правильными колонками
        df_empty = pd.DataFrame(columns=[
            'city_name', 'latitude', 'longitude', 'date', 'temperature', 'feels_like',
            'humidity', 'pressure', 'wind_speed', 'wind_direction', 'precipitation',
            'precipitation_hours', 'weather_code', 'uv_index_max', 'sunshine_duration',
            'daylight_duration', 'visibility', 'collection_time', 'current_time',
            'data_source', 'daily_temp_max', 'daily_temp_min', 'daily_precipitation',
            'daily_weather_code', 'cloud_cover', 'is_day', 'wind_gusts', 'rain_sum',
            'showers_sum', 'snowfall_sum'
        ])
        df = df_empty
    else:
        logger.info("Creating DataFrame from cleaned records...")
        df = pd.DataFrame(cleaned_records)
        logger.info(f"DataFrame shape: {df.shape}")
        
        # Сортируем по городу и дате
        df = df.sort_values(['city_name', 'date'])
    
    timestamp = today.strftime("%Y%m%d")
    output_file = f"{CLEANED_DIR}/weather_cleaned_{timestamp}.csv"
    
    logger.info(f"Saving cleaned data to: {output_file}")
    df.to_csv(output_file, index=False)
    
    # Save cleaning log
    log_file_path = f"{CLEANED_DIR}/cleaning_log_{timestamp}.txt"
    with open(log_file_path, 'w') as log:
        log.write(f"Data cleaning log - {datetime.now().isoformat()}\n")
        log.write(f"Original records: {len(all_data)}\n")
        log.write(f"Cleaned records: {len(cleaned_records)} (days of data)\n")
        log.write(f"Validation errors: {len(validation_errors)}\n")
        log.write("="*50 + "\n")
        for error in validation_errors:
            log.write(error + "\n")
        log.write("\nApplied rules:\n")
        log.write("- Extended date range: Past 2 days + Next 14 days\n")
        log.write("- Daily aggregation from hourly data\n")
        log.write("- Temperature range validation (-50 to +60)\n")
        log.write("- Calculated daily averages\n")
        log.write("- Added cloud_cover, is_day, wind_gusts aggregations (if available)\n")
        log.write("- Checked for availability of hourly variables before aggregation\n")
    
    logger.info("Cleaning process completed.")

if __name__ == "__main__":
    os.makedirs(CLEANED_DIR, exist_ok=True)
    clean_data()