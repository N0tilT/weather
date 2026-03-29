from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import requests
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
# В начало файла, после импортов
import sqlite3
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

@contextmanager
def get_db_connection(db_path: str):
    """Контекстный менеджер для подключения к SQLite"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Возвращаем строки как словари
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Ошибка подключения к БД {db_path}: {e}")
        raise
    finally:
        if conn:
            conn.close()

def query_to_df(conn: sqlite3.Connection, query: str, params: tuple = ()) -> pd.DataFrame:
    """Выполняет SQL-запрос и возвращает результат как DataFrame"""
    return pd.read_sql_query(query, conn, params=params)

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
def get_cleaned_data(date_str: str = None):
    """Получает очищенные данные из SQLite БД"""
    year, month, day = get_date_path(date_str)
    timestamp = f"{year}{month:02d}{day:02d}"
    db_path = Path(os.getenv('CLEANED_DATA_DIR', 'data/cleaned')) / "cleaned.db"
    
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="CLEANED база данных не найдена")
    
    try:
        with get_db_connection(str(db_path)) as conn:
            # Фильтруем по дате, если в таблице есть колонка
            query = "SELECT * FROM weather_cleaned"
            params = ()
            
            # Проверяем наличие колонки date и фильтруем
            cursor = conn.execute("PRAGMA table_info(weather_cleaned)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'date' in columns:
                date_filter = f"{year}-{month:02d}-{day:02d}"
                query += " WHERE date = ?"
                params = (date_filter,)
            
            df = query_to_df(conn, query, params)
            
            if df.empty:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Данные за {year}-{month:02d}-{day:02d} не найдены в БД"
                )
            
            return {"date": f"{year}-{month:02d}-{day:02d}", "data": df.to_dict(orient="records")}
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_cleaned_data: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")

def get_enriched_data(date_str: str = None, city: str = None, start_date: str = None, end_date: str = None):
    """Получает обогащённые данные из SQLite БД с фильтрацией"""
    year, month, day = get_date_path(date_str)
    timestamp = f"{year}{month:02d}{day:02d}"
    db_path = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / "enriched.db"
    
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="ENRICHED база данных не найдена")
    
    try:
        with get_db_connection(str(db_path)) as conn:
            # Проверяем структуру таблицы
            cursor = conn.execute("PRAGMA table_info(weather_enriched)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Строим запрос с условиями
            conditions = []
            params = []
            
            if 'date' in columns and date_str:
                conditions.append("date = ?")
                params.append(f"{year}-{month:02d}-{day:02d}")
            
            if city and 'city_name' in columns:
                conditions.append("city_name = ?")
                params.append(city)
            
            if start_date and 'date' in columns:
                conditions.append("date >= ?")
                params.append(start_date)
            
            if end_date and 'date' in columns:
                conditions.append("date <= ?")
                params.append(end_date)
            
            query = "SELECT * FROM weather_enriched"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            df = query_to_df(conn, query, tuple(params))
            
            if df.empty and date_str:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Данные не найдены с указанными фильтрами"
                )
            
            return {
                "date": f"{year}-{month:02d}-{day:02d}" if date_str else "all",
                "filters": {"city": city, "start_date": start_date, "end_date": end_date},
                "data": df.to_dict(orient="records")
            }
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_enriched_data: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")

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
@app.get("/status", response_model=dict)
async def get_status():
    """Возвращает статус последнего сбора данных"""
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d")
    
    # Проверяем БД enriched
    enriched_db = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / "enriched.db"
    cleaned_db = Path(os.getenv('CLEANED_DATA_DIR', 'data/cleaned')) / "cleaned.db"
    
    status_info = {
        "last_update": "Данные не собраны",
        "record_count": "0 записей",
        "current_time": datetime.now().isoformat(),
        "databases": {}
    }
    
    if enriched_db.exists():
        try:
            with get_db_connection(str(enriched_db)) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM weather_enriched")
                count = cursor.fetchone()[0]
                status_info["databases"]["enriched"] = {"records": count, "status": "ok"}
                
                # Получаем последнюю дату
                cursor = conn.execute("SELECT MAX(date) FROM weather_enriched WHERE date IS NOT NULL")
                last_date = cursor.fetchone()[0]
                if last_date:
                    status_info["last_update"] = last_date
        except Exception as e:
            status_info["databases"]["enriched"] = {"status": "error", "message": str(e)}
    
    if cleaned_db.exists():
        try:
            with get_db_connection(str(cleaned_db)) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM weather_cleaned")
                count = cursor.fetchone()[0]
                status_info["databases"]["cleaned"] = {"records": count, "status": "ok"}
        except Exception as e:
            status_info["databases"]["cleaned"] = {"status": "error", "message": str(e)}
    
    return status_info

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
    """Получает тренды погоды для конкретного города за указанный период из БД"""
    logger.info(f"get_weather_trends called with city_key: {city_key}, days: {days}")
    
    coords_path = Path("config/city_coordinates.json")
    if not coords_path.exists():
        raise HTTPException(status_code=404, detail="Файл координат городов не найден")
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            raise HTTPException(status_code=404, detail=f"Город с ключом '{city_key}' не найден")
        
        city_name = coords[city_key]['name']
        
        # Получаем данные из БД
        db_path = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / "enriched.db"
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="База данных enriched не найдена")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        with get_db_connection(str(db_path)) as conn:
            # Проверяем колонки
            cursor = conn.execute("PRAGMA table_info(weather_enriched)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'date' not in columns or 'city_name' not in columns:
                raise HTTPException(status_code=500, detail="Неверная структура таблицы weather_enriched")
            
            # Запрос с фильтрацией
            query = """
                SELECT * FROM weather_enriched 
                WHERE city_name = ? AND date >= ? AND date <= ?
                ORDER BY date
            """
            df = query_to_df(conn, query, (city_name, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Нет данных за последние {days} дней для {city_name}")
        
        # --- Расчет трендов (аналогично оригиналу, но с правильными колонками) ---
        required_cols = ['temperature', 'daily_temp_max', 'daily_temp_min', 'humidity', 'wind_speed', 'comfort_index']
        
        avg_temp = df['temperature'].mean() if 'temperature' in df.columns else None
        max_temp = df['daily_temp_max'].max() if 'daily_temp_max' in df.columns else None
        min_temp = df['daily_temp_min'].min() if 'daily_temp_min' in df.columns else None
        avg_humidity = df['humidity'].mean() if 'humidity' in df.columns else None
        avg_wind = df['wind_speed'].mean() if 'wind_speed' in df.columns else None
        comfort_avg = df['comfort_index'].mean() if 'comfort_index' in df.columns else None
        
        # Определяем температурный тренд
        temp_trend = "Нет данных"
        if len(df) > 1 and 'temperature' in df.columns:
            first_temp = df.iloc[0]['temperature']
            last_temp = df.iloc[-1]['temperature']
            if pd.notna(first_temp) and pd.notna(last_temp):
                if last_temp > first_temp + 2:
                    temp_trend = "Повышение"
                elif last_temp < first_temp - 2:
                    temp_trend = "Понижение"
                else:
                    temp_trend = "Стабильно"
        
        return {
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
                "total_days": len(df),
                "days_with_precipitation": len(df[df['precipitation'] > 0]) if 'precipitation' in df.columns else 0
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weather_trends: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении трендов: {str(e)}")
        
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
    """Получает исторические данные из БД"""
    coords_path = Path("config/city_coordinates.json")
    if not coords_path.exists():
        raise HTTPException(status_code=404, detail="Файл координат городов не найден")
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            raise HTTPException(status_code=404, detail=f"Город с ключом '{city_key}' не найден")
        
        city_name = coords[city_key]['name']
        db_path = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / "enriched.db"
        
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="База данных enriched не найдена")
        
        with get_db_connection(str(db_path)) as conn:
            query = """
                SELECT * FROM weather_enriched 
                WHERE city_name = ? AND date >= ? AND date <= ?
                ORDER BY date
            """
            df = query_to_df(conn, query, (city_name, start_date, end_date))
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Нет данных за период {start_date} - {end_date} для {city_name}")
        
        return {
            "city": city_name,
            "date_range": f"{start_date} - {end_date}",
            "total_records": len(df),
            "data": df.to_dict(orient="records")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_historical_data: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при получении исторических данных: {str(e)}")


@app.get("/config/cities_reference", response_model=dict)
async def get_cities_reference():
    """Возвращает справочник городов с дополнительной информацией"""
    ref_path = Path("config/cities_reference.json")
    
    if not ref_path.exists():
        # Возвращаем пустой справочник, если файл не найден
        return {"reference": {}}
    
    try:
        with open(ref_path, 'r', encoding='utf-8') as f:
            ref = json.load(f)
        return {"reference": ref}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при чтении справочника: {str(e)}")

@app.get("/config/cities/full", response_model=dict)
async def get_full_cities_config():
    """Возвращает объединённые данные: координаты + справочник"""
    coords_path = Path("config/city_coordinates.json")
    ref_path = Path("config/cities_reference.json")
    
    result = {"coordinates": {}, "reference": {}}
    
    if coords_path.exists():
        try:
            with open(coords_path, 'r', encoding='utf-8') as f:
                result["coordinates"] = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load coordinates: {e}")
    
    if ref_path.exists():
        try:
            with open(ref_path, 'r', encoding='utf-8') as f:
                result["reference"] = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load reference: {e}")
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)