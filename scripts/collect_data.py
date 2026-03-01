import json
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import pandas as pd
from pathlib import Path

load_dotenv()

def load_city_coordinates():
    """Загружает координаты городов из файла конфигурации"""
    coords_path = Path("config/city_coordinates.json")
    if coords_path.exists():
        with open(coords_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def create_directory_structure():
    today = datetime.now()
    base_dir = os.getenv('RAW_DATA_DIR', 'data/raw/openmeteo_api')
    dir_path = f"{base_dir}/{today.year}/{today.month:02d}/{today.day:02d}"
    os.makedirs(dir_path, exist_ok=True)
    return dir_path

def get_extended_weather_data(lat, lon, past_days=2, forecast_days=14):
    """Получает расширенные погодные данные из Open-Meteo API"""
    start_date = (datetime.now() - timedelta(days=past_days)).strftime("%Y-%m-%d")
    end_date = (datetime.now() + timedelta(days=forecast_days)).strftime("%Y-%m-%d")
    
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,"
        f"precipitation_probability,precipitation,rain,showers,snowfall,"
        f"weather_code,pressure_msl,surface_pressure,wind_speed_10m,"
        f"wind_direction_10m,wind_gusts_10m,visibility,uv_index,cape,"
        f"evapotranspiration,et0_fao_evapotranspiration"
        f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
        f"apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,"
        f"precipitation_sum,rain_sum,showers_sum,snowfall_sum,precipitation_hours,"
        f"weather_code,sunrise,sunset,wind_speed_10m_max,wind_gusts_10m_max,"
        f"wind_direction_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration,"
        f"uv_index_max,sunshine_duration,daylight_duration"
        f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
        f"precipitation,weather_code,pressure_msl,surface_pressure,wind_speed_10m,"
        f"wind_direction_10m,wind_gusts_10m,visibility,uv_index"
        f"&timezone=auto&forecast_days={forecast_days}&past_days={past_days}"
    )
    
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()

def collect_weather_data():
    city_coords = load_city_coordinates()
    dir_path = create_directory_structure()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_entries = []
    
    for city_key, coords in city_coords.items():
        try:
            weather_data = get_extended_weather_data(
                coords['lat'], 
                coords['lon'],
                past_days=2,
                forecast_days=14
            )
            
            weather_data['_metadata'] = {
                'collection_time': datetime.now().isoformat(),
                'source': 'open-meteo.com',
                'city_query': city_key,
                'city_name': coords['name'],
                'coordinates': {'lat': coords['lat'], 'lon': coords['lon']},
                'data_range': {
                    'start_date': (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
                    'end_date': (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d"),
                    'past_days': 2,
                    'forecast_days': 14
                },
                'api_url': f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}"
            }
            
            filename = f"weather_{city_key.lower().replace(' ', '_')}_{timestamp}.json"
            with open(f"{dir_path}/{filename}", 'w', encoding='utf-8') as f:
                json.dump(weather_data, f, ensure_ascii=False, indent=2)
                
            hourly_count = len(weather_data.get('hourly', {}).get('time', []))
            daily_count = len(weather_data.get('daily', {}).get('time', []))
            
            log_entries.append(
                f"SUCCESS: {coords['name']} - {hourly_count} hourly records, {daily_count} daily records"
            )
            
        except Exception as e:
            log_entries.append(f"ERROR: {coords['name']} - {str(e)}")
    
    with open(f"{dir_path}/collection_log_{timestamp}.txt", 'w') as log_file:
        log_file.write(f"Data collection log - {datetime.now().isoformat()}\n")
        log_file.write(f"Collected for {len(city_coords)} cities\n")
        log_file.write(f"Time range: Past 2 days + Next 14 days\n")
        log_file.write("="*50 + "\n")
        for entry in log_entries:
            log_file.write(entry + "\n")
    
    return len(city_coords), len(log_entries) - len(city_coords)

if __name__ == "__main__":
    collect_weather_data()