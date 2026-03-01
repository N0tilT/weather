from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import requests
from scripts import pipeline_runner
from pydantic import BaseModel
import shutil
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Weather Tourism API (Extended Open-Meteo)", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_date_path(date_str: str = None):
    """Возвращает путь к данным для указанной даты или текущей"""
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
            year, month, day = dt.year, dt.month, dt.day
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYYMMDD")
    else:
        now = datetime.now()
        year, month, day = now.year, now.month, now.day
    
    return year, month, day

def get_raw_data(date_str: str = None):
    year, month, day = get_date_path(date_str)
    raw_dir = Path(os.getenv('RAW_DATA_DIR', 'data/raw/openmeteo_api')) / str(year) / f"{month:02d}" / f"{day:02d}"
    
    if not raw_dir.exists():
        raise HTTPException(status_code=404, detail=f"Данные за {year}-{month}-{day} не найдены")
    
    files = list(raw_dir.glob("weather_*.json"))
    if not files:
        raise HTTPException(status_code=404, detail="RAW данные не найдены")
    
    results = []
    for file in files:
        with open(file, 'r', encoding='utf-8') as f:
            results.append(json.load(f))
    
    return {"date": f"{year}-{month:02d}-{day:02d}", "data": results}

def get_cleaned_data(date_str: str = None):
    year, month, day = get_date_path(date_str)
    timestamp = f"{year}{month:02d}{day:02d}"
    cleaned_file = Path(os.getenv('CLEANED_DATA_DIR', 'data/cleaned')) / f"weather_cleaned_{timestamp}.csv"
    
    if not cleaned_file.exists():
        raise HTTPException(status_code=404, detail="CLEANED данные не найдены")
    
    df = pd.read_csv(cleaned_file)
    return {"date": f"{year}-{month:02d}-{day:02d}", "data": df.to_dict(orient="records")}

# Добавим в api/main.py
def get_enriched_data(date_str: str = None, city: str = None, start_date: str = None, end_date: str = None):
    year, month, day = get_date_path(date_str)
    timestamp = f"{year}{month:02d}{day:02d}"
    enriched_file = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / f"weather_enriched_{timestamp}.csv"
    
    if not enriched_file.exists():
        raise HTTPException(status_code=404, detail="ENRICHED данные не найдены")
    
    df = pd.read_csv(enriched_file)
    
    # Фильтрация по городу
    if city:
        df = df[df['city_name'] == city]
    
    # Фильтрация по дате (только если колонка 'date' существует)
    if 'date' in df.columns:
        if start_date:
            df = df[pd.to_datetime(df['date']) >= start_date]
        if end_date:
            df = df[pd.to_datetime(df['date']) <= end_date]
    
    return {"date": f"{year}-{month:02d}-{day:02d}", "data": df.to_dict(orient="records")}

def get_aggregated_data(report_type: str):
    agg_dir = Path(os.getenv('AGGREGATED_DATA_DIR', 'data/aggregated'))
    file_map = {
        "city_rating": "city_tourism_rating.csv",
        "district_summary": "federal_districts_summary.csv",
        "travel_recommendations": "travel_recommendations.csv"
    }
    
    if report_type not in file_map:
        raise HTTPException(status_code=400, detail="Неверный тип отчета")
    
    file_path = agg_dir / file_map[report_type]
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Отчет {report_type} не найден")
    
    df = pd.read_csv(file_path)
    return {"report_type": report_type, "data": df.to_dict(orient="records")}

class CityCoordinates(BaseModel):
    city_name: str
    lat: float
    lon: float
    ru_name: str
    federal_district: str = "Неизвестно"
    timezone: str = "UTC+0"
    population: int = 0
    tourism_season: str = "Круглогодично"

@app.get("/raw/{date}", response_model=dict)
async def get_raw(date: str = None):
    return get_raw_data(date)

@app.get("/cleaned/{date}", response_model=dict)
async def get_cleaned(date: str = None):
    return get_cleaned_data(date)

@app.get("/cleaned", response_model=dict)
async def get_cleaned_default():
    return get_cleaned_data()

@app.get("/enriched/{date}", response_model=dict)
async def get_enriched(date: str = None, city: str = Query(None), start_date: str = Query(None), end_date: str = Query(None)):
    return get_enriched_data(date, city, start_date, end_date)

@app.get("/enriched", response_model=dict)
async def get_enriched_default(date: str = None, city: str = Query(None), start_date: str = Query(None), end_date: str = Query(None)):
    return get_enriched_data(date, city, start_date, end_date)


@app.get("/aggregated/{report_type}", response_model=dict)
async def get_aggregated(report_type: str):
    return get_aggregated_data(report_type)

@app.post("/update", response_model=dict)
async def update_data(background_tasks: BackgroundTasks):
    """Запускает обновление данных в фоновом режиме"""
    background_tasks.add_task(pipeline_runner.run_full_pipeline)
    return {"status": "success", "message": "Обновление данных запущено в фоновом режиме"}

@app.get("/status", response_model=dict)
async def get_status():
    """Возвращает статус последнего сбора данных"""
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d")
    log_file = Path(os.getenv('CLEANED_DATA_DIR', 'data/cleaned')) / f"cleaning_log_{timestamp}.txt"
    
    if log_file.exists():
        with open(log_file, 'r') as f:
            lines = f.readlines()
            last_update = lines[0].strip() if lines else "Неизвестно"
            record_count = next((line for line in lines if "Cleaned records" in line), "Неизвестно")
    else:
        last_update = "Данные не собраны"
        record_count = "0 записей"
    
    return {
        "last_update": last_update,
        "record_count": record_count,
        "current_time": datetime.now().isoformat()
    }

@app.get("/validate/city", response_model=dict)
async def validate_city(lat: float, lon: float):
    """Проверяет существование координат в Open-Meteo"""
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "valid": True,
                "message": "Координаты действительны",
                "details": {
                    "latitude": data.get('latitude'),
                    "longitude": data.get('longitude'),
                    "current_temp": data.get('current', {}).get('temperature_2m'),
                    "timezone": data.get('timezone')
                }
            }
        else:
            return {
                "valid": False,
                "message": f"Координаты недействительны (код: {response.status_code})",
                "details": response.json().get('reason', 'Неизвестная ошибка')
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке координат: {str(e)}")

@app.post("/config/city_coordinates", response_model=dict)
async def add_city_coordinates(city_data: CityCoordinates):
    """Добавляет новые координаты города и базовую информацию в справочник"""
    coords_path = Path("config/city_coordinates.json")
    ref_path = Path("config/cities_reference.json") # Путь к новому справочнику
    
    try:
        # --- Загружаем текущий маппинг координат ---
        if coords_path.exists():
            with open(coords_path, 'r', encoding='utf-8') as f:
                coords = json.load(f)
        else:
            coords = {}
        
        # Проверяем, существует ли уже такой город по ключу
        if city_data.city_name in coords:
            raise HTTPException(
                status_code=400, 
                detail=f"Город с ключом '{city_data.city_name}' уже существует"
            )
        
        # --- Загружаем текущий справочник ---
        if ref_path.exists():
            with open(ref_path, 'r', encoding='utf-8') as f:
                ref = json.load(f)
        else:
            # Если справочник не существует, создаем пустой
            ref = {}
        
        # Проверяем, существует ли уже такой город в справочнике по русскому названию
        if city_data.ru_name in ref:
            raise HTTPException(
                status_code=400, 
                detail=f"Город с русским названием '{city_data.ru_name}' уже существует в справочнике"
            )
        
        # Проверяем координаты через API (если нужно)
        # validation = await validate_city(city_data.lat, city_data.lon)
        # if not validation["valid"]:
        #     raise HTTPException(
        #         status_code=400,
        #         detail=f"Координаты недействительны: {validation['message']}"
        #     )
        
        # Добавляем новый город в координаты
        coords[city_data.city_name] = {
            "lat": city_data.lat,
            "lon": city_data.lon,
            "name": city_data.ru_name
        }
        
        # Добавляем новый город в справочник
        ref[city_data.ru_name] = {
            "federal_district": city_data.federal_district,
            "timezone": city_data.timezone,
            "population": city_data.population,
            "tourism_season": city_data.tourism_season
        }
        
        # --- Сохраняем оба файла ---
        
        # Сохраняем координаты
        with open(coords_path, 'w', encoding='utf-8') as f:
            json.dump(coords, f, ensure_ascii=False, indent=2)
        
        # Сохраняем справочник
        with open(ref_path, 'w', encoding='utf-8') as f:
            json.dump(ref, f, ensure_ascii=False, indent=2)
        
        # Создаем резервные копии
        backup_coords_path = coords_path.parent / f"city_coordinates_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        backup_ref_path = ref_path.parent / f"cities_reference_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        shutil.copy2(coords_path, backup_coords_path)
        shutil.copy2(ref_path, backup_ref_path)
        
        return {
            "status": "success", 
            "message": f"Города '{city_data.ru_name}' (ключ: {city_data.city_name}) успешно добавлены в систему",
            "city_key": city_data.city_name,
            "ru_name": city_data.ru_name
        }
    
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при добавлении города: {str(e)}"
        )

@app.get("/config/city_coordinates", response_model=dict)
async def get_city_coordinates():
    """Возвращает текущие координаты городов"""
    coords_path = Path("config/city_coordinates.json")
    
    if not coords_path.exists():
        raise HTTPException(
            status_code=404, 
            detail="Файл координат городов не найден"
        )
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        return {"coordinates": coords}
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при чтении координат: {str(e)}"
        )

@app.post("/update/city/{city_key}", response_model=dict)
async def update_city_data(city_key: str, background_tasks: BackgroundTasks):
    """Запускает сбор данных только для указанного города"""
    coords_path = Path("config/city_coordinates.json")
    
    if not coords_path.exists():
        raise HTTPException(
            status_code=404, 
            detail="Файл координат городов не найден"
        )
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            raise HTTPException(
                status_code=404, 
                detail=f"Город с ключом '{city_key}' не найден в координатах"
            )
        
        temp_coords = {city_key: coords[city_key]}
        temp_coords_path = coords_path.parent / "temp_city_coordinates.json"
        
        with open(temp_coords_path, 'w', encoding='utf-8') as f:
            json.dump(temp_coords, f, ensure_ascii=False, indent=2)
        
        def collect_city_data():
            try:
                original_coords_path = coords_path
                
                shutil.move(temp_coords_path, coords_path)
                
                pipeline_runner.run_full_pipeline()
                
                with open(original_coords_path, 'r', encoding='utf-8') as f:
                    original_data = json.load(f)
                with open(coords_path, 'w', encoding='utf-8') as f:
                    json.dump(original_data, f, ensure_ascii=False, indent=2)
                    
            except Exception as e:
                logging.error(f"Ошибка при сборе данных для города {city_key}: {str(e)}")
                if original_coords_path.exists():
                    shutil.copy2(original_coords_path, coords_path)
        
        background_tasks.add_task(collect_city_data)
        return {
            "status": "success",
            "message": f"Запущен сбор данных для города '{coords[city_key]['name']}'",
            "city_key": city_key
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при подготовке сбора данных: {str(e)}"
        )

@app.get("/weather_trends/{city_key}", response_model=dict)
async def get_weather_trends(city_key: str, days: int = Query(7, description="Количество дней для анализа трендов")):
    """Получает тренды погоды для конкретного города за указанный период"""
    logger.info(f"get_weather_trends called with city_key: {city_key}, days: {days}")
    
    coords_path = Path("config/city_coordinates.json")
    
    if not coords_path.exists():
        logger.error(f"Config file {coords_path} not found")
        raise HTTPException(
            status_code=404, 
            detail="Файл координат городов не найден"
        )
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        logger.debug(f"Loaded coordinates, keys: {list(coords.keys())}")
        
        if city_key not in coords:
            logger.warning(f"City key '{city_key}' not found in coordinates")
            raise HTTPException(
                status_code=404, 
                detail=f"Город с ключом '{city_key}' не найден"
            )
        
        city_name = coords[city_key]['name']
        logger.info(f"Found city name '{city_name}' for key '{city_key}'")
        
        # Получаем последние данные
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d")
        enriched_file = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / f"weather_enriched_{timestamp}.csv"
        
        logger.info(f"Attempting to read enriched data from: {enriched_file}")
        
        if not enriched_file.exists():
            logger.error(f"Enriched data file {enriched_file} not found")
            raise HTTPException(
                status_code=404, 
                detail="Данные не найдены"
            )
        
        # --- Добавим логирование перед чтением CSV ---
        logger.debug(f"Reading CSV file: {enriched_file}")
        df = pd.read_csv(enriched_file)
        logger.info(f"Successfully read CSV. Shape: {df.shape}, Columns: {list(df.columns)}")
        
        city_data = df[df['city_name'] == city_name].sort_values('date')
        logger.info(f"Filtered data for city '{city_name}'. Shape after filter: {city_data.shape}")
        
        # Берем последние N дней
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # --- Проверим, есть ли колонка 'date' ---
        if 'date' not in city_data.columns:
             logger.error(f"'date' column not found in enriched data for city '{city_name}'. Available columns: {list(city_data.columns)}")
             raise HTTPException(
                 status_code=500,
                 detail=f"Внутренняя ошибка: колонка 'date' отсутствует в данных для '{city_name}'"
             )
        
        # --- Проверим, содержит ли колонка 'date' данные ---
        if city_data.empty:
            logger.warning(f"No data found for city '{city_name}' in the enriched dataset.")
            raise HTTPException(
                status_code=404, 
                detail=f"Нет данных за последние {days} дней для {city_name}"
            )
        
        # --- Проверим тип данных в 'date' ---
        # Попробуем преобразовать, если нужно
        try:
            city_data['date_parsed'] = pd.to_datetime(city_data['date'])
        except Exception as e:
            logger.error(f"Error parsing 'date' column for city '{city_name}': {e}")
            raise HTTPException(
                 status_code=500,
                 detail=f"Внутренняя ошибка: невозможно обработать дату в данных для '{city_name}'"
             )
        
        recent_data = city_data[
            (city_data['date_parsed'] >= start_date.strftime('%Y-%m-%d')) &
            (city_data['date_parsed'] <= end_date.strftime('%Y-%m-%d'))
        ]
        
        logger.info(f"Data for last {days} days for city '{city_name}'. Shape: {recent_data.shape}")
        
        if recent_data.empty:
            logger.warning(f"No data found for city '{city_name}' in the last {days} days.")
            raise HTTPException(
                status_code=404, 
                detail=f"Нет данных за последние {days} дней для {city_name}"
            )
        
        # --- Проверим, есть ли нужные колонки для расчета трендов ---
        required_columns = ['temperature_avg', 'temperature_max', 'temperature_min', 'humidity_avg', 'wind_speed_avg', 'comfort_index']
        # ВНИМАНИЕ: используем те колонки, которые реально создаются clean_data.py
        # В нашем случае это 'temperature', 'daily_temp_max', 'daily_temp_min', 'humidity', 'wind_speed', 'comfort_index'
        required_columns_real = ['temperature', 'daily_temp_max', 'daily_temp_min', 'humidity', 'wind_speed', 'comfort_index']
        
        missing_cols = [col for col in required_columns_real if col not in recent_data.columns]
        if missing_cols:
            logger.warning(f"Some required columns for trends are missing: {missing_cols}. Available: {list(recent_data.columns)}")
            # Не вызываем ошибку, а просто используем доступные
        
        # Рассчитываем тренды
        # Используем правильные названия колонок
        avg_temp = recent_data['temperature'].mean() if 'temperature' in recent_data.columns else None
        max_temp = recent_data['daily_temp_max'].max() if 'daily_temp_max' in recent_data.columns else None
        min_temp = recent_data['daily_temp_min'].min() if 'daily_temp_min' in recent_data.columns else None
        avg_humidity = recent_data['humidity'].mean() if 'humidity' in recent_data.columns else None
        avg_wind = recent_data['wind_speed'].mean() if 'wind_speed' in recent_data.columns else None
        comfort_avg = recent_data['comfort_index'].mean() if 'comfort_index' in recent_data.columns else None
        
        logger.info(f"Trend calculations completed for {city_name}")
        
        # Определяем тенденции
        temp_trend = "Нет данных"
        if len(recent_data) > 1 and 'temperature' in recent_data.columns:
            first_temp_row = recent_data.iloc[0]
            last_temp_row = recent_data.iloc[-1]
            
            first_temp = first_temp_row['temperature']
            last_temp = last_temp_row['temperature']
            
            if pd.notna(first_temp) and pd.notna(last_temp):
                if last_temp > first_temp + 2:
                    temp_trend = "Повышение"
                elif last_temp < first_temp - 2:
                    temp_trend = "Понижение"
                else:
                    temp_trend = "Стабильно"
            else:
                logger.info(f"Could not determine temp trend due to NaN values: first={first_temp}, last={last_temp}")
        elif 'temperature' not in recent_data.columns:
            logger.warning("Could not determine temp trend as 'temperature' column is missing.")
        
        result = {
            "city": city_name,
            "period": f"Последние {days} дней",
            "trends": {
                "avg_temperature": round(avg_temp, 1) if pd.notna(avg_temp) else None,
                "max_temperature": round(max_temp, 1) if pd.notna(max_temp) else None,
                "min_temperature": round(min_temp, 1) if pd.notna(min_temp) else None,
                "avg_humidity": round(avg_humidity, 1) if pd.notna(avg_humidity) else None,
                "avg_wind_speed": round(avg_wind, 1) if pd.notna(avg_wind) else None,
                "avg_comfort_index": round(comfort_avg, 1) if pd.notna(comfort_avg) else None,
                "temperature_trend": temp_trend,
                "total_days": len(recent_data),
                "days_with_precipitation": len(recent_data[recent_data['precipitation'] > 0]) if 'precipitation' in recent_data.columns else 0 # Проверяем колонку precipitation
            }
        }
        
        logger.info(f"Returning trends for {city_name}")
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_weather_trends for city_key '{city_key}', days {days}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при получении трендов: {str(e)}"
        )

@app.get("/historical_data/{city_key}", response_model=dict)
async def get_historical_data(city_key: str, start_date: str, end_date: str):
    """Получает исторические данные для конкретного города за указанный период"""
    coords_path = Path("config/city_coordinates.json")
    
    if not coords_path.exists():
        raise HTTPException(
            status_code=404, 
            detail="Файл координат городов не найден"
        )
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            raise HTTPException(
                status_code=404, 
                detail=f"Город с ключом '{city_key}' не найден"
            )
        
        city_name = coords[city_key]['name']
        
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d")
        enriched_file = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / f"weather_enriched_{timestamp}.csv"
        
        if not enriched_file.exists():
            raise HTTPException(
                status_code=404, 
                detail="Данные не найдены"
            )
        
        df = pd.read_csv(enriched_file)
        city_data = df[df['city_name'] == city_name]
        
        filtered_data = city_data[
            (pd.to_datetime(city_data['date']) >= start_date) &
            (pd.to_datetime(city_data['date']) <= end_date)
        ].sort_values('date')
        
        if filtered_data.empty:
            raise HTTPException(
                status_code=404, 
                detail=f"Нет данных за период {start_date} - {end_date} для {city_name}"
            )
        
        return {
            "city": city_name,
            "date_range": f"{start_date} - {end_date}",
            "total_records": len(filtered_data),
            "data": filtered_data.to_dict(orient="records")
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Ошибка при получении исторических данных: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)