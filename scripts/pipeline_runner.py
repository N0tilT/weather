import logging
from datetime import datetime
import time
import os
from .collect_data import collect_weather_data
from .clean_data import clean_data
from .enrich_data import enrich_data
from .create_reports import create_reports
import json
from pathlib import Path

def load_city_coordinates():
    """Загружает координаты городов из файла конфигурации"""
    coords_path = Path("config/city_coordinates.json")
    if coords_path.exists():
        with open(coords_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def run_full_pipeline():
    """Запускает полный пайплайн обработки данных"""
    start_time = time.time()
    log = []
    
    try:
        city_coords = load_city_coordinates()
        if not city_coords:
            raise ValueError("Файл координат городов пуст или отсутствует")
        
        log.append(f"[{datetime.now()}] Найдено {len(city_coords)} городов в конфигурации")
        log.append(f"[{datetime.now()}] Сбор данных за прошедшие 2 дня + 14 дней прогноза")
        
        log.append(f"[{datetime.now()}] Запуск сбора данных...")
        cities_count, errors = collect_weather_data()
        log.append(f"[{datetime.now()}] Сбор данных завершен: {cities_count} городов, {errors} ошибок")
        
        if cities_count == 0:
            raise ValueError("Не удалось собрать данные ни по одному городу")
        
        log.append(f"[{datetime.now()}] Запуск очистки данных...")
        clean_data()
        log.append(f"[{datetime.now()}] Очистка данных завершена")
        
        log.append(f"[{datetime.now()}] Запуск обогащения данных...")
        enrich_data()
        log.append(f"[{datetime.now()}] Обогащение данных завершено")
        
        log.append(f"[{datetime.now()}] Запуск агрегации данных...")
        create_reports()
        log.append(f"[{datetime.now()}] Агрегация данных завершена")
        
        duration = time.time() - start_time
        log.append(f"[{datetime.now()}] Пайплайн выполнен за {duration:.2f} секунд")
        
        log_dir = os.getenv('LOG_DIR', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/pipeline_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
        
        with open(log_file, 'w') as f:
            for entry in log:
                f.write(entry + "\n")
        
        return True, log
    
    except Exception as e:
        error_msg = f"[{datetime.now()}] Ошибка выполнения пайплайна: {str(e)}"
        log.append(error_msg)
        logging.exception("Critical error in pipeline")
        
        log_dir = os.getenv('LOG_DIR', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/pipeline_error_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
        
        with open(log_file, 'w') as f:
            for entry in log:
                f.write(entry + "\n")
        
        return False, log

if __name__ == "__main__":
    success, log = run_full_pipeline()
    for entry in log:
        print(entry)
    exit(0 if success else 1)