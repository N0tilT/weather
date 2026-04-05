from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import requests
from pydantic import BaseModel, Field
import shutil
import sqlite3
import time
import hashlib
from contextlib import contextmanager, asynccontextmanager
from typing import Optional, List, Dict, Any, Generator
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start_time = time.time()
        request_id = hashlib.md5(f"{request.url.path}{time.time()}".encode()).hexdigest()[:8]
        
        logger.info(f"[{request_id}] STARTED {request.method} {request.url.path} from {request.client.host if request.client else 'unknown'}")
        
        try:
            response = await call_next(request)
            elapsed = time.time() - start_time
            logger.info(f"[{request_id}] COMPLETED {request.method} {request.url.path} {response.status_code} in {elapsed:.3f}s")
            return response
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"[{request_id}] ERROR {request.method} {request.url.path} in {elapsed:.3f}s: {str(e)}", exc_info=True)
            raise

class DatabaseManager:
    _connections: Dict[str, sqlite3.Connection] = {}
    _locks: Dict[str, threading.Lock] = {}
    
    @classmethod
    def configure_db(cls, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=-64000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA mmap_size=268435456")
        cursor.execute("PRAGMA busy_timeout=30000")
        conn.commit()
        
        return conn
    
    @classmethod
    @contextmanager
    def get_connection(cls, db_path: str) -> Generator[sqlite3.Connection, None, None]:
        db_key = str(Path(db_path).resolve())
        
        if db_key not in cls._locks:
            cls._locks[db_key] = threading.Lock()
        
        with cls._locks[db_key]:
            conn = None
            try:
                conn = cls.configure_db(db_path)
                yield conn
            except sqlite3.Error as e:
                logger.error(f"Database error for {db_path}: {e}", exc_info=True)
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception as e:
                        logger.warning(f"Error closing connection: {e}")

    @classmethod
    def query_to_df(cls, conn: sqlite3.Connection, query: str, params: tuple = (), 
                   chunk_size: int = 10000, timeout: float = 60.0) -> pd.DataFrame:
        start_time = time.time()
        query_hash = hashlib.md5(f"{query}{params}".encode()).hexdigest()[:12]
        
        logger.debug(f"[DB:{query_hash}] Executing query with {len(params)} params")
        
        try:
            chunks = []
            for chunk in pd.read_sql_query(query, conn, params=params, chunksize=chunk_size):
                chunks.append(chunk)
                if len(chunks) % 10 == 0:
                    logger.debug(f"[DB:{query_hash}] Loaded {len(chunks) * chunk_size} rows...")
            
            if not chunks:
                return pd.DataFrame()
            
            df = pd.concat(chunks, ignore_index=True)
            elapsed = time.time() - start_time
            
            logger.info(f"[DB:{query_hash}] Query completed: {len(df)} rows in {elapsed:.3f}s")
            return df
            
        except pd.errors.DatabaseError as e:
            logger.error(f"[DB:{query_hash}] Pandas database error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"[DB:{query_hash}] Unexpected error: {e}", exc_info=True)
            raise

class DataCache:
    def __init__(self, ttl_seconds: int = 300, max_size: int = 100):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._timestamps: Dict[str, float] = {}
        self._ttl = ttl_seconds
        self._max_size = max_size
        self._lock = threading.Lock()
    
    def _is_valid(self, key: str) -> bool:
        if key not in self._timestamps:
            return False
        return time.time() - self._timestamps[key] < self._ttl
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if self._is_valid(key):
                logger.debug(f"[CACHE] HIT: {key}")
                return self._cache[key]
            if key in self._cache:
                logger.debug(f"[CACHE] EXPIRED: {key}")
                del self._cache[key]
                del self._timestamps[key]
            return None
    
    def set(self, key: str, value: Any):
        with self._lock:
            if len(self._cache) >= self._max_size:
                oldest = min(self._timestamps, key=self._timestamps.get)
                del self._cache[oldest]
                del self._timestamps[oldest]
                logger.debug(f"[CACHE] EVICTED: {oldest}")
            
            self._cache[key] = value
            self._timestamps[key] = time.time()
            logger.debug(f"[CACHE] SET: {key}")
    
    def invalidate(self, pattern: str = None):
        with self._lock:
            if pattern:
                keys_to_remove = [k for k in self._cache if pattern in k]
                for k in keys_to_remove:
                    del self._cache[k]
                    del self._timestamps[k]
                logger.info(f"[CACHE] INVALIDATED {len(keys_to_remove)} entries matching '{pattern}'")
            else:
                count = len(self._cache)
                self._cache.clear()
                self._timestamps.clear()
                logger.info(f"[CACHE] CLEARED all {count} entries")

app_cache = DataCache(ttl_seconds=300, max_size=50)
db_cache = DataCache(ttl_seconds=60, max_size=200)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup: initializing resources")
    start = time.time()
    
    enriched_db = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched')) / "enriched.db"
    if enriched_db.exists():
        try:
            with DatabaseManager.get_connection(str(enriched_db)) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='enriched_observations'")
                indexes = [row[0] for row in cursor.fetchall()]
                
                if 'idx_enriched_city_date' not in indexes:
                    logger.info("Creating index idx_enriched_city_date on enriched_observations")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_enriched_city_date ON enriched_observations(city_name, date)")
                if 'idx_enriched_date' not in indexes:
                    logger.info("Creating index idx_enriched_date on enriched_observations")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_enriched_date ON enriched_observations(date)")
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
    
    logger.info(f"Startup completed in {time.time() - start:.3f}s")
    yield
    logger.info("Application shutdown: cleaning up resources")
    app_cache.invalidate()
    db_cache.invalidate()

app = FastAPI(
    title="Weather Tourism API (Extended Open-Meteo)", 
    version="3.1.2",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.add_middleware(RequestLoggingMiddleware)

API_URL = os.getenv('API_URL', "http://api:8000")
RAW_DATA_DIR = Path(os.getenv('RAW_DATA_DIR', 'data/raw/openmeteo_api'))
CLEANED_DATA_DIR = Path(os.getenv('CLEANED_DATA_DIR', 'data/cleaned'))
ENRICHED_DATA_DIR = Path(os.getenv('ENRICHED_DATA_DIR', 'data/enriched'))
AGGREGATED_DATA_DIR = Path(os.getenv('AGGREGATED_DATA_DIR', 'data/aggregated'))
CONFIG_DIR = Path("config")

def _get_date_components(date_str: Optional[str] = None) -> tuple:
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
            return dt.year, dt.month, dt.day
        except ValueError as e:
            logger.error(f"Invalid date format '{date_str}': {e}")
            raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYYMMDD")
    now = datetime.now()
    return now.year, now.month, now.day

def _build_cache_key(prefix: str, **kwargs) -> str:
    key_parts = [prefix] + [f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None]
    return ":".join(key_parts)

@app.get("/raw/{date}", response_model=dict)
async def get_raw(date: Optional[str] = None):
    start = time.time()
    cache_key = _build_cache_key("raw", date=date)
    
    cached = app_cache.get(cache_key)
    if cached:
        logger.info(f"[RAW] Cache hit for date={date}")
        return cached
    
    try:
        year, month, day = _get_date_components(date)
        raw_dir = RAW_DATA_DIR / str(year) / f"{month:02d}" / f"{day:02d}"
        
        if not raw_dir.exists():
            logger.warning(f"RAW directory not found: {raw_dir}")
            raise HTTPException(status_code=404, detail=f"Данные за {year}-{month}-{day} не найдены")
        
        files = list(raw_dir.glob("weather_*.json"))
        if not files:
            logger.warning(f"No RAW files found in {raw_dir}")
            raise HTTPException(status_code=404, detail="RAW данные не найдены")
        
        logger.info(f"Loading {len(files)} RAW files from {raw_dir}")
        results = []
        for file in files:
            with open(file, 'r', encoding='utf-8') as f:
                results.append(json.load(f))
        
        response = {"date": f"{year}-{month:02d}-{day:02d}", "data": results, "file_count": len(files)}
        app_cache.set(cache_key, response)
        
        elapsed = time.time() - start
        logger.info(f"[RAW] Completed for date={date}: {len(results)} records in {elapsed:.3f}s")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_raw: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении RAW данных: {str(e)}")

@app.get("/cleaned/{date}", response_model=dict)
async def get_cleaned(date: Optional[str] = None):
    start = time.time()
    cache_key = _build_cache_key("cleaned", date=date)
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    year, month, day = _get_date_components(date)
    db_path = CLEANED_DATA_DIR / "cleaned.db"
    
    if not db_path.exists():
        logger.warning(f"Cleaned DB not found: {db_path}")
        raise HTTPException(status_code=404, detail="CLEANED база данных не найдена")
    
    try:
        with DatabaseManager.get_connection(str(db_path)) as conn:
            cursor = conn.execute("PRAGMA table_info(weather_cleaned)")
            columns = [row[1] for row in cursor.fetchall()]
            
            query = "SELECT * FROM weather_cleaned"
            params = ()
            
            if 'date' in columns:
                date_filter = f"{year}-{month:02d}-{day:02d}"
                query += " WHERE date = ?"
                params = (date_filter,)
                logger.debug(f"Filtering cleaned data by date: {date_filter}")
            
            df = DatabaseManager.query_to_df(conn, query, params)
            
            if df.empty:
                logger.warning(f"No cleaned data found for date={date}")
                raise HTTPException(status_code=404, detail=f"Данные за {year}-{month:02d}-{day:02d} не найдены в БД")
            
            response = {"date": f"{year}-{month:02d}-{day:02d}", "data": df.to_dict(orient="records"), "record_count": len(df)}
            app_cache.set(cache_key, response)
            
            elapsed = time.time() - start
            logger.info(f"[CLEANED] Completed for date={date}: {len(df)} rows in {elapsed:.3f}s")
            return response
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_cleaned: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_cleaned: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении очищенных данных: {str(e)}")

@app.get("/enriched", response_model=dict)
@app.get("/enriched/{date}", response_model=dict)
async def get_enriched(
    date: Optional[str] = None, 
    city: Optional[str] = Query(None), 
    start_date: Optional[str] = Query(None), 
    end_date: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=100000)
):
    start = time.time()
    cache_key = _build_cache_key("enriched", date=date, city=city, start_date=start_date, end_date=end_date, limit=limit)
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    year, month, day = _get_date_components(date)
    db_path = ENRICHED_DATA_DIR / "enriched.db"
    
    if not db_path.exists():
        logger.warning(f"Enriched DB not found: {db_path}")
        raise HTTPException(status_code=404, detail="ENRICHED база данных не найдена")
    
    try:
        with DatabaseManager.get_connection(str(db_path)) as conn:
            cursor = conn.execute("PRAGMA table_info(enriched_observations)")
            columns = [row[1] for row in cursor.fetchall()]
            
            conditions = []
            params = []
            
            if 'date' in columns and date:
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
            
            query = "SELECT * FROM enriched_observations"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY date DESC"
            
            if limit:
                query += f" LIMIT {int(limit)}"
            
            logger.debug(f"Enriched query: {query} with params: {params}")
            df = DatabaseManager.query_to_df(conn, query, tuple(params))
            
            if df.empty and date:
                logger.warning(f"No enriched data found with filters: date={date}, city={city}")
                raise HTTPException(status_code=404, detail=f"Данные не найдены с указанными фильтрами")
            
            response = {
                "date": f"{year}-{month:02d}-{day:02d}" if date else "all",
                "filters": {"city": city, "start_date": start_date, "end_date": end_date, "limit": limit},
                "data": df.to_dict(orient="records"),
                "record_count": len(df)
            }
            app_cache.set(cache_key, response)
            
            elapsed = time.time() - start
            logger.info(f"[ENRICHED] Completed: {len(df)} rows in {elapsed:.3f}s with filters city={city}, date={date}")
            return response
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_enriched: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_enriched: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении обогащенных данных: {str(e)}")

def _load_aggregated_csv(filename: str) -> Optional[Dict[str, Any]]:
    path = AGGREGATED_DATA_DIR / f"{filename}.csv"
    
    if not path.exists():
        logger.warning(f"Aggregated file not found: {path}")
        return None
    
    try:
        start = time.time()
        df = pd.read_csv(path)
        elapsed = time.time() - start
        logger.info(f"Loaded aggregated CSV {filename}: {len(df)} rows in {elapsed:.3f}s")
        return {"data": df.to_dict(orient="records"), "record_count": len(df)}
    except Exception as e:
        logger.error(f"Error reading aggregated CSV {filename}: {e}", exc_info=True)
        return None

@app.get("/aggregated/{report_type}", response_model=dict)
async def get_aggregated(report_type: str):
    start = time.time()
    cache_key = _build_cache_key("aggregated", report_type=report_type)
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    valid_types = ["city_rating", "district_summary", "travel_recommendations"]
    if report_type not in valid_types:
        logger.warning(f"Invalid report_type: {report_type}")
        raise HTTPException(status_code=400, detail=f"Недопустимый тип отчета. Допустимые: {', '.join(valid_types)}")
    
    filename_map = {
        "city_rating": "city_tourism_rating",
        "district_summary": "federal_districts_summary", 
        "travel_recommendations": "travel_recommendations"
    }
    
    result = _load_aggregated_csv(filename_map[report_type])
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"Отчет '{report_type}' не найден")
    
    response = {"report_type": report_type, **result}
    app_cache.set(cache_key, response)
    
    elapsed = time.time() - start
    logger.info(f"[AGGREGATED] Completed report_type={report_type}: {result['record_count']} rows in {elapsed:.3f}s")
    return response

@app.get("/status", response_model=dict)
async def get_status():
    start = time.time()
    status_info = {
        "current_time": datetime.now().isoformat(),
        "cache_stats": {
            "app_cache_size": len(app_cache._cache),
            "db_cache_size": len(db_cache._cache)
        },
        "databases": {}
    }
    
    for db_name, db_path, table_name in [
        ("enriched", ENRICHED_DATA_DIR / "enriched.db", "enriched_observations"),
        ("cleaned", CLEANED_DATA_DIR / "cleaned.db", "weather_cleaned")
    ]:
        if db_path.exists():
            try:
                db_start = time.time()
                with DatabaseManager.get_connection(str(db_path)) as conn:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    
                    cursor = conn.execute(f"SELECT MAX(date) FROM {table_name} WHERE date IS NOT NULL")
                    last_date = cursor.fetchone()[0]
                    
                    cursor = conn.execute(f"SELECT MIN(date) FROM {table_name} WHERE date IS NOT NULL")
                    first_date = cursor.fetchone()[0]
                
                db_elapsed = time.time() - db_start
                status_info["databases"][db_name] = {
                    "records": count,
                    "date_range": f"{first_date} to {last_date}" if last_date else "N/A",
                    "status": "ok",
                    "query_time_ms": round(db_elapsed * 1000, 2)
                }
                logger.debug(f"Status check for {db_name}: {count} records in {db_elapsed:.3f}s")
            except Exception as e:
                logger.error(f"Error checking {db_name} database: {e}", exc_info=True)
                status_info["databases"][db_name] = {"status": "error", "message": str(e)}
        else:
            status_info["databases"][db_name] = {"status": "not_found"}
    
    elapsed = time.time() - start
    logger.info(f"[STATUS] Completed in {elapsed:.3f}s")
    return status_info

@app.get("/validate/city", response_model=dict)
async def validate_city(lat: float = Query(..., ge=-90, le=90), lon: float = Query(..., ge=-180, le=180)):
    start = time.time()
    cache_key = _build_cache_key("validate_city", lat=lat, lon=lon)
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m&timezone=auto"
        api_start = time.time()
        response = requests.get(url, timeout=10)
        api_elapsed = time.time() - api_start
        
        if response.status_code == 200:
            data = response.json()
            result = {
                "valid": True,
                "message": "Координаты действительны",
                "details": {
                    "latitude": data.get('latitude'),
                    "longitude": data.get('longitude'),
                    "current_temp": data.get('current', {}).get('temperature_2m'),
                    "timezone": data.get('timezone')
                },
                "api_response_time_ms": round(api_elapsed * 1000, 2)
            }
        else:
            logger.warning(f"Open-Meteo validation failed for ({lat}, {lon}): {response.status_code}")
            result = {
                "valid": False,
                "message": f"Координаты недействительны (код: {response.status_code})",
                "details": response.json().get('reason', 'Неизвестная ошибка') if response.content else "No response body"
            }
        
        app_cache.set(cache_key, result)
        elapsed = time.time() - start
        logger.info(f"[VALIDATE] City ({lat}, {lon}) valid={result['valid']} in {elapsed:.3f}s (API: {result.get('api_response_time_ms', 'N/A')}ms)")
        return result
        
    except requests.Timeout:
        logger.error(f"Timeout validating city ({lat}, {lon})")
        raise HTTPException(status_code=504, detail="Таймаут при проверке координат")
    except requests.RequestException as e:
        logger.error(f"Request error validating city ({lat}, {lon}): {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Ошибка соединения с гео-сервисом: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error validating city ({lat}, {lon}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке координат: {str(e)}")

class CityCoordinates(BaseModel):
    city_name: str = Field(..., min_length=1, max_length=100)
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    ru_name: str = Field(..., min_length=1, max_length=100)
    federal_district: str = "Неизвестно"
    timezone: str = "UTC+0"
    population: int = Field(default=0, ge=0)
    tourism_season: str = "Круглогодично"

@app.post("/config/city_coordinates", response_model=dict)
async def add_city_coordinates(city_: CityCoordinates):
    start = time.time()
    coords_path = CONFIG_DIR / "city_coordinates.json"
    ref_path = CONFIG_DIR / "cities_reference.json"
    
    try:
        coords_path.parent.mkdir(parents=True, exist_ok=True)
        
        coords = {}
        if coords_path.exists():
            with open(coords_path, 'r', encoding='utf-8') as f:
                coords = json.load(f)
        
        if city_.city_name in coords:
            logger.warning(f"Duplicate city key attempt: {city_.city_name}")
            raise HTTPException(status_code=400, detail=f"Город с ключом '{city_.city_name}' уже существует")
        
        ref = {}
        if ref_path.exists():
            with open(ref_path, 'r', encoding='utf-8') as f:
                ref = json.load(f)
        
        if city_.ru_name in ref:
            logger.warning(f"Duplicate Russian name attempt: {city_.ru_name}")
            raise HTTPException(status_code=400, detail=f"Город с русским названием '{city_.ru_name}' уже существует в справочнике")
        
        coords[city_.city_name] = {"lat": city_.lat, "lon": city_.lon, "name": city_.ru_name}
        ref[city_.ru_name] = {
            "federal_district": city_.federal_district,
            "timezone": city_.timezone,
            "population": city_.population,
            "tourism_season": city_.tourism_season
        }
        
        backup_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        if coords_path.exists():
            shutil.copy2(coords_path, coords_path.parent / f"city_coordinates_backup_{backup_suffix}.json")
        if ref_path.exists():
            shutil.copy2(ref_path, ref_path.parent / f"cities_reference_backup_{backup_suffix}.json")
        
        with open(coords_path, 'w', encoding='utf-8') as f:
            json.dump(coords, f, ensure_ascii=False, indent=2)
        with open(ref_path, 'w', encoding='utf-8') as f:
            json.dump(ref, f, ensure_ascii=False, indent=2)
        
        app_cache.invalidate(pattern="cities")
        
        elapsed = time.time() - start
        logger.info(f"[CONFIG] Added city '{city_.ru_name}' (key: {city_.city_name}) in {elapsed:.3f}s")
        
        return {
            "status": "success", 
            "message": f"Город '{city_.ru_name}' успешно добавлен",
            "city_key": city_.city_name,
            "ru_name": city_.ru_name,
            "processing_time_ms": round(elapsed * 1000, 2)
        }
    
    except HTTPException:
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in config files: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка формата конфигурационного файла: {str(e)}")
    except Exception as e:
        logger.error(f"Error adding city coordinates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при добавлении города: {str(e)}")

@app.get("/config/city_coordinates", response_model=dict)
async def get_city_coordinates():
    start = time.time()
    cache_key = "config:city_coordinates"
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    coords_path = CONFIG_DIR / "city_coordinates.json"
    
    if not coords_path.exists():
        logger.warning(f"City coordinates file not found: {coords_path}")
        raise HTTPException(status_code=404, detail="Файл координат городов не найден")
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        result = {"coordinates": coords, "count": len(coords)}
        app_cache.set(cache_key, result)
        
        elapsed = time.time() - start
        logger.info(f"[CONFIG] Retrieved {len(coords)} city coordinates in {elapsed:.3f}s")
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in city_coordinates.json: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка формата файла координат: {str(e)}")
    except Exception as e:
        logger.error(f"Error reading city coordinates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при чтении координат: {str(e)}")

@app.post("/update/city/{city_key}", response_model=dict)
async def update_city_data(city_key: str, background_tasks: BackgroundTasks):
    start = time.time()
    coords_path = CONFIG_DIR / "city_coordinates.json"
    
    if not coords_path.exists():
        raise HTTPException(status_code=404, detail="Файл координат городов не найден")
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            logger.warning(f"City key not found: {city_key}")
            raise HTTPException(status_code=404, detail=f"Город с ключом '{city_key}' не найден")
        
        city_name = coords[city_key]['name']
        logger.info(f"Queuing data collection for city: {city_name} (key: {city_key})")
        
        def collect_city_data():
            task_start = time.time()
            temp_coords_path = coords_path.parent / "temp_city_coordinates.json"
            original_backup = None
            
            try:
                if coords_path.exists():
                    original_backup = coords_path.parent / f"coords_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    shutil.copy2(coords_path, original_backup)
                
                temp_coords = {city_key: coords[city_key]}
                with open(temp_coords_path, 'w', encoding='utf-8') as f:
                    json.dump(temp_coords, f, ensure_ascii=False, indent=2)
                
                shutil.move(temp_coords_path, coords_path)
                
                try:
                    from pipeline_runner import run_full_pipeline
                    run_full_pipeline()
                    logger.info(f"Pipeline completed for city {city_name}")
                except ImportError:
                    logger.warning("pipeline_runner not available, skipping data collection")
                except Exception as pipeline_error:
                    logger.error(f"Pipeline error for city {city_name}: {pipeline_error}", exc_info=True)
                    raise
                
                if original_backup and original_backup.exists():
                    shutil.copy2(original_backup, coords_path)
                    original_backup.unlink()
                
                app_cache.invalidate(pattern="cities")
                app_cache.invalidate(pattern="enriched")
                
                task_elapsed = time.time() - task_start
                logger.info(f"[BACKGROUND] Data collection for {city_name} completed in {task_elapsed:.3f}s")
                
            except Exception as e:
                logger.error(f"Background task error for city {city_name}: {e}", exc_info=True)
                if original_backup and original_backup.exists():
                    shutil.copy2(original_backup, coords_path)
            finally:
                if temp_coords_path.exists():
                    temp_coords_path.unlink(missing_ok=True)
        
        background_tasks.add_task(collect_city_data)
        elapsed = time.time() - start
        
        logger.info(f"[UPDATE] Queued city update for {city_name} in {elapsed:.3f}s")
        return {
            "status": "queued",
            "message": f"Запущен сбор данных для города '{city_name}'",
            "city_key": city_key,
            "city_name": city_name,
            "processing_time_ms": round(elapsed * 1000, 2)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error preparing city update for {city_key}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при подготовке сбора данных: {str(e)}")
@app.get("/weather_trends/{city_key}", response_model=dict)
async def get_weather_trends(city_key: str, days: int = Query(7, ge=1, le=90)):
    start = time.time()
    cache_key = _build_cache_key("trends", city_key=city_key, days=days)
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    coords_path = CONFIG_DIR / "city_coordinates.json"
    if not coords_path.exists():
        raise HTTPException(status_code=404, detail="Файл координат городов не найден")
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            raise HTTPException(status_code=404, detail=f"Город с ключом '{city_key}' не найден")
        
        city_name = coords[city_key]['name']
        db_path = ENRICHED_DATA_DIR / "enriched.db"
        
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="База данных enriched не найдена")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        with DatabaseManager.get_connection(str(db_path)) as conn:
            cursor = conn.execute("PRAGMA table_info(enriched_observations)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'date' not in columns or 'city_name' not in columns:
                raise HTTPException(status_code=500, detail="Неверная структура таблицы enriched_observations")
            
            query = """
                SELECT date, temperature, temperature_max, temperature_min, humidity, wind_speed, comfort_index, precipitation
                FROM enriched_observations 
                WHERE city_name = ? AND date >= ? AND date <= ?
                ORDER BY date
            """
            df = DatabaseManager.query_to_df(conn, query, (city_name, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        if df.empty:
            logger.warning(f"No trend data for {city_name} over {days} days")
            raise HTTPException(status_code=404, detail=f"Нет данных за последние {days} дней для {city_name}")
        
        avg_temp = df['temperature'].mean() if 'temperature' in df.columns and df['temperature'].notna().any() else None
        max_temp = df['temperature_max'].max() if 'temperature_max' in df.columns and df['temperature_max'].notna().any() else None
        min_temp = df['temperature_min'].min() if 'temperature_min' in df.columns and df['temperature_min'].notna().any() else None
        avg_humidity = df['humidity'].mean() if 'humidity' in df.columns and df['humidity'].notna().any() else None
        avg_wind = df['wind_speed'].mean() if 'wind_speed' in df.columns and df['wind_speed'].notna().any() else None
        comfort_avg = df['comfort_index'].mean() if 'comfort_index' in df.columns and df['comfort_index'].notna().any() else None
        
        temp_trend = "Нет данных"
        if len(df) > 1 and 'temperature' in df.columns and df['temperature'].notna().any():
            valid_temps = df['temperature'].dropna()
            if len(valid_temps) >= 2:
                first_temp, last_temp = valid_temps.iloc[0], valid_temps.iloc[-1]
                if last_temp > first_temp + 2:
                    temp_trend = "Повышение"
                elif last_temp < first_temp - 2:
                    temp_trend = "Понижение"
                else:
                    temp_trend = "Стабильно"
        
        days_with_precip = len(df[df['precipitation'] > 0]) if 'precipitation' in df.columns else 0
        
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
                "total_days": len(df),
                "days_with_precipitation": days_with_precip
            }
        }
        
        app_cache.set(cache_key, result)
        elapsed = time.time() - start
        logger.info(f"[TRENDS] Retrieved trends for {city_name} ({days} days): {len(df)} records in {elapsed:.3f}s")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weather_trends for {city_key}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении трендов: {str(e)}")
   
@app.get("/historical_data/{city_key}", response_model=dict)
async def get_historical_data(city_key: str, start_date: str, end_date: str):
    start = time.time()
    cache_key = _build_cache_key("historical", city_key=city_key, start_date=start_date, end_date=end_date)
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    coords_path = CONFIG_DIR / "city_coordinates.json"
    if not coords_path.exists():
        raise HTTPException(status_code=404, detail="Файл координат городов не найден")
    
    try:
        with open(coords_path, 'r', encoding='utf-8') as f:
            coords = json.load(f)
        
        if city_key not in coords:
            raise HTTPException(status_code=404, detail=f"Город с ключом '{city_key}' не найден")
        
        city_name = coords[city_key]['name']
        db_path = ENRICHED_DATA_DIR / "enriched.db"
        
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="База данных enriched не найдена")
        
        with DatabaseManager.get_connection(str(db_path)) as conn:
            query = """
                SELECT * FROM enriched_observations 
                WHERE city_name = ? AND date >= ? AND date <= ?
                ORDER BY date
            """
            df = DatabaseManager.query_to_df(conn, query, (city_name, start_date, end_date))
        
        if df.empty:
            logger.warning(f"No historical data for {city_name} from {start_date} to {end_date}")
            raise HTTPException(status_code=404, detail=f"Нет данных за период {start_date} - {end_date} для {city_name}")
        
        result = {
            "city": city_name,
            "date_range": f"{start_date} - {end_date}",
            "total_records": len(df),
            "data": df.to_dict(orient="records")
        }
        
        app_cache.set(cache_key, result)
        elapsed = time.time() - start
        logger.info(f"[HISTORICAL] Retrieved {len(df)} records for {city_name} in {elapsed:.3f}s")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_historical_data for {city_key}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при получении исторических данных: {str(e)}")

@app.get("/config/cities_reference", response_model=dict)
async def get_cities_reference():
    start = time.time()
    cache_key = "config:cities_reference"
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    ref_path = CONFIG_DIR / "cities_reference.json"
    
    if not ref_path.exists():
        logger.debug(f"Reference file not found: {ref_path}, returning empty")
        return {"reference": {}}
    
    try:
        with open(ref_path, 'r', encoding='utf-8') as f:
            ref = json.load(f)
        
        result = {"reference": ref, "count": len(ref)}
        app_cache.set(cache_key, result)
        
        elapsed = time.time() - start
        logger.info(f"[CONFIG] Retrieved {len(ref)} city references in {elapsed:.3f}s")
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in cities_reference.json: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка формата справочника: {str(e)}")
    except Exception as e:
        logger.error(f"Error reading cities reference: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка при чтении справочника: {str(e)}")

@app.get("/config/cities/full", response_model=dict)
async def get_full_cities_config():
    start = time.time()
    cache_key = "config:cities:full"
    
    cached = app_cache.get(cache_key)
    if cached:
        return cached
    
    coords_path = CONFIG_DIR / "city_coordinates.json"
    ref_path = CONFIG_DIR / "cities_reference.json"
    
    result = {"coordinates": {}, "reference": {}}
    
    if coords_path.exists():
        try:
            with open(coords_path, 'r', encoding='utf-8') as f:
                result["coordinates"] = json.load(f)
            logger.debug(f"Loaded {len(result['coordinates'])} city coordinates")
        except Exception as e:
            logger.warning(f"Could not load coordinates from {coords_path}: {e}")
    
    if ref_path.exists():
        try:
            with open(ref_path, 'r', encoding='utf-8') as f:
                result["reference"] = json.load(f)
            logger.debug(f"Loaded {len(result['reference'])} city references")
        except Exception as e:
            logger.warning(f"Could not load reference from {ref_path}: {e}")
    
    result["counts"] = {
        "coordinates": len(result["coordinates"]),
        "reference": len(result["reference"])
    }
    
    app_cache.set(cache_key, result)
    elapsed = time.time() - start
    logger.info(f"[CONFIG:FULL] Retrieved full city config: {result['counts']} in {elapsed:.3f}s")
    return result

@app.get("/health", response_model=dict)
async def health_check():
    health = {"status": "healthy", "timestamp": datetime.now().isoformat(), "checks": {}}
    
    for name, db_path in [("enriched_db", ENRICHED_DATA_DIR / "enriched.db"), ("cleaned_db", CLEANED_DATA_DIR / "cleaned.db")]:
        if db_path.exists():
            try:
                check_start = time.time()
                with DatabaseManager.get_connection(str(db_path)) as conn:
                    cursor = conn.execute("SELECT 1")
                    cursor.fetchone()
                check_elapsed = time.time() - check_start
                health["checks"][name] = {"status": "ok", "response_time_ms": round(check_elapsed * 1000, 2)}
            except Exception as e:
                health["checks"][name] = {"status": "error", "message": str(e)}
                health["status"] = "degraded"
        else:
            health["checks"][name] = {"status": "not_found"}
            health["status"] = "degraded"
    
    return health

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(f"HTTP {exc.status_code} on {request.url.path}: {exc.detail}")
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception on {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Внутренняя ошибка сервера"})

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting server on {API_URL}")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info", access_log=True)