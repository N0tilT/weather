import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import time

st.set_page_config(page_title="Weather Tourism API", layout="wide", page_icon="🌤️")

API_BASE = "http://api:8000"

@st.cache_data(ttl=300)
def fetch_json(endpoint: str, params: dict = None, timeout: int = 30):
    try:
        url = f"{API_BASE}/{endpoint.lstrip('/')}"
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        st.error("Таймаут при подключении к серверу")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка подключения: {str(e)}")
        return None
    except json.JSONDecodeError:
        st.error("Ошибка парсинга ответа сервера")
        return None

@st.cache_data(ttl=600)
def get_cities_config():
    result = fetch_json("/config/cities/full")
    if result and "coordinates" in result:
        return result["coordinates"], result.get("reference", {})
    return {}, {}

def post_json(endpoint: str, data: dict, timeout: int = 30):
    try:
        url = f"{API_BASE}/{endpoint.lstrip('/')}"
        response = requests.post(url, json=data, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка: {str(e)}")
        return None

def display_dataframe(df: pd.DataFrame, title: str = None, max_rows: int = 1000):
    if df is None or df.empty:
        st.warning("Нет данных для отображения")
        return
    if title:
        st.subheader(title)
    if len(df) > max_rows:
        st.info(f"Показано первых {max_rows} записей из {len(df)}")
        df = df.head(max_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

def format_metric(value, decimals: int = 1):
    if value is None or pd.isna(value):
        return "N/A"
    return f"{round(float(value), decimals)}"

def main():
    st.title("🌤️ Weather Tourism Dashboard")
    
    cities_coords, cities_ref = get_cities_config()
    city_options = {f"{data['name']} ({key})": key for key, data in cities_coords.items()}
    
    with st.sidebar:
        st.header("⚙️ Настройки")
        selected_city_label = st.selectbox("Выберите город", options=list(city_options.keys()), index=0 if city_options else None)
        selected_city_key = city_options[selected_city_label] if selected_city_label else None
        
        st.divider()
        nav = st.radio("Раздел", ["📊 Статус", "📈 Тренды погоды", "📅 Исторические данные", "📑 Агрегированные отчеты", "🔍 Поиск данных", "⚙️ Конфигурация"])
        
        st.divider()
        if st.button("🔄 Обновить данные"):
            st.cache_data.clear()
            st.rerun()
    
    if nav == "📊 Статус":
        st.header("📊 Статус системы")
        status = fetch_json("/status")
        health = fetch_json("/health")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Health Check")
            if health:
                status_badge = "🟢 Healthy" if health.get("status") == "healthy" else "🟡 Degraded"
                st.markdown(f"**Status:** {status_badge}")
                st.markdown(f"**Time:** {health.get('timestamp', 'N/A')}")
                for check_name, check_data in health.get("checks", {}).items():
                    badge = "🟢" if check_data.get("status") == "ok" else "🔴" if check_data.get("status") == "error" else "⚪"
                    st.markdown(f"{badge} **{check_name}**: {check_data.get('status')}"+ (f" ({check_data.get('response_time_ms')}ms)" if 'response_time_ms' in check_data else ''))
            else:
                st.warning("Не удалось получить health check")
        
        with col2:
            st.subheader("Database Stats")
            if status and "databases" in status:
                for db_name, db_info in status["databases"].items():
                    with st.expander(f"🗄️ {db_name}", expanded=True):
                        st.markdown(f"**Status:** {db_info.get('status', 'unknown')}")
                        if db_info.get("status") == "ok":
                            st.markdown(f"📊 Records: {db_info.get('records', 0):,}")
                            st.markdown(f"📅 Range: {db_info.get('date_range', 'N/A')}")
                            st.markdown(f"⚡ Query time: {db_info.get('query_time_ms', 0)}ms")
            else:
                st.warning("Не удалось получить статистику БД")
        
        st.divider()
        st.subheader("📦 Cache Stats")
        if status and "cache_stats" in status:
            c1, c2 = st.columns(2)
            c1.metric("App Cache", status["cache_stats"].get("app_cache_size", 0))
            c2.metric("DB Cache", status["cache_stats"].get("db_cache_size", 0))
    
    elif nav == "📈 Тренды погоды" and selected_city_key:
        st.header(f"📈 Тренды погоды: {cities_coords[selected_city_key]['name']}")
        
        days = st.slider("Период (дни)", min_value=1, max_value=90, value=7)
        
        if st.button("Загрузить тренды", type="primary"):
            with st.spinner("Загрузка данных..."):
                trends = fetch_json(f"/weather_trends/{selected_city_key}", params={"days": days})
                if trends and "trends" in trends:
                    t = trends["trends"]
                    
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Средняя температура", f"{format_metric(t.get('avg_temperature'))}°C")
                    m2.metric("Макс. температура", f"{format_metric(t.get('max_temperature'))}°C")
                    m3.metric("Мин. температура", f"{format_metric(t.get('min_temperature'))}°C")
                    m4.metric("Тренд", t.get("temperature_trend", "N/A"))
                    
                    m5, m6, m7, m8 = st.columns(4)
                    m5.metric("Влажность", f"{format_metric(t.get('avg_humidity'))}%")
                    m6.metric("Ветер", f"{format_metric(t.get('avg_wind_speed'))} м/с")
                    m7.metric("Комфорт", f"{format_metric(t.get('avg_comfort_index'))}")
                    m8.metric("Дней с осадками", t.get("days_with_precipitation", 0))
                    
                    st.info(f"📊 Всего записей: {t.get('total_records', 0)} | Период: {trends.get('period', 'N/A')}")
                    
                    raw_data = fetch_json("/enriched", params={
                        "city": cities_coords[selected_city_key]["name"],
                        "start_date": (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
                        "end_date": datetime.now().strftime("%Y-%m-%d"),
                        "limit": 1000
                    })
                    if raw_data and "data" in raw_data and raw_data["data"]:
                        df = pd.DataFrame(raw_data["data"])
                        date_cols = [c for c in df.columns if "date" in c.lower()]
                        if date_cols:
                            df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors="coerce")
                        
                        chart_cols = [c for c in ["temperature", "humidity", "wind_speed", "comfort_index", "precipitation"] if c in df.columns]
                        if chart_cols:
                            st.subheader("📉 Графики")
                            for col in chart_cols:
                                chart_df = df[[date_cols[0], col]].dropna().set_index(date_cols[0])
                                st.line_chart(chart_df, y=col, use_container_width=True)
                else:
                    st.warning("Не удалось загрузить тренды или нет данных")
    
    elif nav == "📅 Исторические данные" and selected_city_key:
        st.header(f"📅 Исторические данные: {cities_coords[selected_city_key]['name']}")
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Дата начала", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("Дата окончания", value=datetime.now())
        
        limit = st.slider("Лимит записей", min_value=100, max_value=50000, value=5000, step=100)
        
        if st.button("Загрузить данные", type="primary"):
            if start_date > end_date:
                st.error("Дата начала не может быть позже даты окончания")
            else:
                with st.spinner("Загрузка исторических данных..."):
                    data = fetch_json(f"/historical_data/{selected_city_key}", params={
                        "start_date": start_date.strftime("%Y-%m-%d"),
                        "end_date": end_date.strftime("%Y-%m-%d")
                    })
                    if data and "data" in data:
                        st.success(f"Загружено {data.get('total_records', 0)} записей")
                        df = pd.DataFrame(data["data"])
                        
                        st.subheader("📊 Статистика")
                        num_cols = df.select_dtypes(include="number").columns.tolist()
                        if num_cols:
                            st.dataframe(df[num_cols].describe(), use_container_width=True)
                        
                        display_dataframe(df, "📋 Данные", max_rows=limit)
                        
                        if "date" in df.columns and "temperature" in df.columns:
                            st.subheader("🌡️ Температура по дням")
                            chart_df = df[["date", "temperature"]].dropna().set_index("date")
                            st.line_chart(chart_df, use_container_width=True)
                    else:
                        st.warning("Нет данных за выбранный период")
    
    elif nav == "📑 Агрегированные отчеты":
        st.header("📑 Агрегированные отчеты")
        
        report_types = {
            "city_rating": "🏆 Рейтинг городов",
            "district_summary": "🗺️ Сводка по округам",
            "travel_recommendations": "✈️ Рекомендации для путешествий"
        }
        
        selected_report = st.selectbox("Выберите тип отчета", options=list(report_types.keys()), format_func=lambda x: report_types[x])
        
        if st.button("Сформировать отчет", type="primary"):
            with st.spinner("Генерация отчета..."):
                result = fetch_json(f"/aggregated/{selected_report}")
                if result and "data" in result:
                    st.subheader(report_types[selected_report])
                    df = pd.DataFrame(result["data"])
                    display_dataframe(df, max_rows=2000)
                    
                    if "rating" in df.columns or "score" in df.columns or "comfort_index" in df.columns:
                        metric_col = next((c for c in ["rating", "score", "comfort_index", "tourism_score"] if c in df.columns), None)
                        if metric_col and "city_name" in df.columns:
                            st.subheader("📊 Топ городов")
                            top_df = df.nlargest(10, metric_col)[["city_name", metric_col]]
                            st.bar_chart(top_df.set_index("city_name"), use_container_width=True)
                else:
                    st.warning("Не удалось загрузить отчет")
    
    elif nav == "🔍 Поиск данных":
        st.header("🔍 Поиск и фильтрация данных")
        
        tab1, tab2, tab3 = st.tabs(["✨ Обогащенные данные", "🧹 Очищенные данные", "📦 Сырые данные"])
        
        with tab1:
            st.subheader("Enriched Observations")
            col1, col2, col3 = st.columns(3)
            with col1:
                search_city = st.text_input("Город", placeholder="Москва")
            with col2:
                search_start = st.date_input("С даты", value=None)
            with col3:
                search_end = st.date_input("По дату", value=None)
            
            search_limit = st.slider("Лимит", 100, 100000, 1000, 100)
            
            if st.button("Поиск", type="primary", key="search_enriched"):
                params = {"limit": search_limit}
                if search_city:
                    params["city"] = search_city
                if search_start:
                    params["start_date"] = search_start.strftime("%Y-%m-%d")
                if search_end:
                    params["end_date"] = search_end.strftime("%Y-%m-%d")
                
                with st.spinner("Поиск..."):
                    result = fetch_json("/enriched", params=params)
                    if result and "data" in result:
                        st.success(f"Найдено {result.get('record_count', 0)} записей")
                        df = pd.DataFrame(result["data"])
                        display_dataframe(df, max_rows=search_limit)
                    else:
                        st.warning("Нет результатов")
        
        with tab2:
            st.subheader("Cleaned Weather Data")
            clean_date = st.date_input("Дата", value=datetime.now(), key="clean_date")
            if st.button("Загрузить очищенные данные", type="primary", key="load_cleaned"):
                with st.spinner("Загрузка..."):
                    result = fetch_json(f"/cleaned/{clean_date.strftime('%Y%m%d')}")
                    if result and "data" in result:
                        st.success(f"Загружено {result.get('record_count', 0)} записей")
                        df = pd.DataFrame(result["data"])
                        display_dataframe(df, max_rows=5000)
                    else:
                        st.warning("Нет данных за выбранную дату")
        
        with tab3:
            st.subheader("Raw API Data")
            raw_date = st.date_input("Дата", value=datetime.now(), key="raw_date")
            if st.button("Загрузить сырые данные", type="primary", key="load_raw"):
                with st.spinner("Загрузка..."):
                    result = fetch_json(f"/raw/{raw_date.strftime('%Y%m%d')}")
                    if result and "data" in result:
                        st.success(f"Загружено файлов: {result.get('file_count', 0)}")
                        with st.expander("📄 Просмотр данных", expanded=False):
                            st.json(result["data"][:5] if isinstance(result["data"], list) else result["data"])
                    else:
                        st.warning("Нет данных за выбранную дату")
    
    elif nav == "⚙️ Конфигурация" and selected_city_key:
        st.header("⚙️ Управление конфигурацией")
        
        tab1, tab2 = st.tabs(["📋 Справочник городов", "🔄 Обновление данных"])
        
        with tab1:
            st.subheader("Координаты городов")
            if cities_coords:
                coords_df = pd.DataFrame([
                    {"key": k, "name": v.get("name"), "lat": v.get("lat"), "lon": v.get("lon")}
                    for k, v in cities_coords.items()
                ])
                st.dataframe(coords_df, use_container_width=True, hide_index=True)
            
            st.divider()
            st.subheader("Доп. информация")
            if cities_ref:
                ref_df = pd.DataFrame([
                    {"city": k, **v} for k, v in cities_ref.items()
                ])
                st.dataframe(ref_df, use_container_width=True, hide_index=True)
        
        with tab2:
            st.subheader(f"Обновить данные для: {cities_coords[selected_city_key]['name']}")
            st.warning("⚠️ Эта операция может занять несколько минут")
            
            if st.button("🚀 Запустить сбор данных", type="primary"):
                with st.spinner("Запуск задачи в фоне..."):
                    result = post_json(f"/update/city/{selected_city_key}", {})
                    if result and result.get("status") == "queued":
                        st.success(f"✅ {result.get('message')}")
                        st.info(f"⏱️ Время обработки: {result.get('processing_time_ms', 0)}ms")
                    else:
                        st.error("Не удалось запустить задачу")
            
            st.divider()
            st.subheader("🌐 Проверка координат")
            col1, col2 = st.columns(2)
            with col1:
                test_lat = st.number_input("Широта", value=float(cities_coords[selected_city_key]["lat"]), step=0.01)
            with col2:
                test_lon = st.number_input("Долгота", value=float(cities_coords[selected_city_key]["lon"]), step=0.01)
            
            if st.button("Проверить"):
                with st.spinner("Проверка..."):
                    result = fetch_json("/validate/city", params={"lat": test_lat, "lon": test_lon})
                    if result:
                        if result.get("valid"):
                            st.success("✅ Координаты действительны")
                            details = result.get("details", {})
                            st.json({
                                "lat": details.get("latitude"),
                                "lon": details.get("longitude"),
                                "temp": details.get("current_temp"),
                                "timezone": details.get("timezone")
                            })
                        else:
                            st.error(f"❌ {result.get('message')}")
    
    elif selected_city_key is None and nav != "📊 Статус" and nav != "📑 Агрегированные отчеты":
        st.info("👈 Выберите город в боковой панели для работы с данными")

if __name__ == "__main__":
    main()