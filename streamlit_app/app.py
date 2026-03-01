# streamlit_app/app.py
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import json
import os
import time

# Настройки
API_URL = os.getenv('API_URL', "http://localhost:8000")
DATE_FORMAT = "%Y-%m-%d"

def fetch_data(endpoint, params=None):
    try:
        response = requests.get(f"{API_URL}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Ошибка при получении данных: {str(e)}")
        return None

def validate_city_coordinates(lat, lon):
    try:
        response = requests.get(
            f"{API_URL}/validate/city",
            params={"lat": lat, "lon": lon},
            timeout=5
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        try:
            error_msg = response.json().get("detail", error_msg)
        except:
            pass
        return {"valid": False, "message": f"Ошибка соединения: {error_msg}"}

def add_city_coordinates(city_name, lat, lon, ru_name, federal_district, timezone, population, tourism_season):
    try:
        response = requests.post(
            f"{API_URL}/config/city_coordinates",
            json={
                "city_name": city_name,
                "lat": lat,
                "lon": lon,
                "ru_name": ru_name,
                "federal_district": federal_district,
                "timezone": timezone,
                "population": population,
                "tourism_season": tourism_season
            },
            timeout=10
        )
        response.raise_for_status()
        return True, response.json().get("message", "Город успешно добавлен")
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        try:
            error_msg = response.json().get("detail", error_msg)
        except:
            pass
        return False, f"Ошибка при добавлении города: {error_msg}"

def get_city_coordinates():
    return fetch_data("/config/city_coordinates")

def get_weather_trends(city_key, days=7):
    return fetch_data(f"/weather_trends/{city_key}", params={"days": days})

def get_historical_data(city_key, start_date, end_date):
    return fetch_data(f"/historical_data/{city_key}", params={
        "start_date": start_date,
        "end_date": end_date
    })

def main():
    st.set_page_config(
        page_title="Погодный туризм (Расширенный Open-Meteo)",
        page_icon="🌤️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Заголовок
    st.title("🌤️ Система анализа погоды для туристической компании (Расширенный Open-Meteo)")
    
    # Боковая панель
    st.sidebar.header("🌍 Управление городами")
    
    # Получение текущих координат городов
    city_coords = get_city_coordinates()
    city_names = []
    if city_coords and 'coordinates' in city_coords:
        city_names = [info['name'] for info in city_coords['coordinates'].values()]
        city_keys = {info['name']: key for key, info in city_coords['coordinates'].items()}
    
    # Выбор города для просмотра деталей
    selected_city = st.sidebar.selectbox(
        "Выберите город для детального просмотра",
        options=city_names,
        index=0 if city_names else 0
    )
    
    # Кнопка добавления города
    if st.sidebar.button("➕ Добавить новый город", use_container_width=True):
        st.session_state.show_add_city = True
    
    # Кнопка обновления
    if st.sidebar.button("🔄 Обновить данные сейчас", use_container_width=True):
        with st.spinner("Запуск обновления данных..."):
            response = requests.post(f"{API_URL}/update")
            if response.status_code == 200:
                st.sidebar.success("Обновление запущено! Данные будут доступны через несколько минут.")
            else:
                st.sidebar.error("Не удалось запустить обновление данных.")
    
    # Получение статуса
    status = fetch_data("/status")
    if status:
        st.sidebar.info(f"**Последнее обновление:**\n{status.get('last_update', 'Неизвестно')}")
        st.sidebar.info(f"**Записей обработано:**\n{status.get('record_count', '0')}")
    
    # Основное содержимое
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Обзор", 
        "🌤️ Погода по городам", 
        "📈 Агрегированные отчеты",
        "📈 Тренды и история",
        "⚙️ Настройки"
    ])
    
    # Переменная состояния для формы добавления города
    if 'show_add_city' not in st.session_state:
        st.session_state.show_add_city = False
    
    with tab1:
        st.header("Общая информация")
        
        # Получение данных
        enriched_data = fetch_data("/enriched", params={"date": datetime.now().strftime("%Y%m%d")})
        if enriched_data and 'data' in enriched_data:
            df = pd.DataFrame(enriched_data['data'])
            
            # Проверяем, есть ли колонка 'date' и фильтруем по сегодняшней дате
            if 'date' in df.columns:
                today_str = datetime.now().strftime("%Y-%m-%d")
                current_df = df[df['date'] == today_str]
            else:
                # Если колонки 'date' нет, используем все данные
                current_df = df
            
            if not current_df.empty:
                # Краткая статистика
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("🌍 Города", len(current_df))
                with col2:
                    # Используем колонку 'temperature' вместо 'temperature_avg'
                    avg_temp = current_df['temperature'].mean()
                    st.metric("🌡️ Ср. температура", f"{avg_temp:.1f}°C")
                with col3:
                    max_comfort = current_df['comfort_index'].max()
                    st.metric("😊 Макс. комфорт", f"{max_comfort:.0f}")
                with col4:
                    active_season = len(current_df[current_df['tourist_season_match']])
                    st.metric("✈️ Активный сезон", f"{active_season}/{len(current_df)}")
                
                # График комфортности
                st.subheader("Индекс комфортности по городам")
                if 'city_name' in current_df.columns and 'comfort_index' in current_df.columns:
                    fig = px.bar(
                        current_df, 
                        x='city_name', 
                        y='comfort_index',
                        color='comfort_index',
                        color_continuous_scale='RdYlGn',
                        range_color=[0, 100],
                        labels={'comfort_index': 'Индекс комфортности', 'city_name': 'Город'}
                    )
                    fig.update_layout(
                        xaxis_title="Город",
                        yaxis_title="Индекс комфортности",
                        coloraxis_colorbar_title="Комфорт"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Нет необходимых колонок для отображения графика комфортности")
            
            # Топ рекомендаций
            st.subheader("Топ рекомендации для турагентств")
            recommendations = fetch_data("/aggregated/travel_recommendations")
            if recommendations and 'data' in recommendations:
                for rec in recommendations['data']:
                    if rec['category'] == 'top_destination':
                        st.success(f"✅ **{rec['city']}**: {rec['recommendation']}")
                    elif rec['category'] == 'stay_home':
                        st.warning(f"⚠️ **{rec['city']}**: {rec['recommendation']}")
                    elif rec['category'] == 'special':
                        st.info(f"ℹ️ **{rec['city']}**: {rec['recommendation']}")
        
        # Если нет данных
        else:
            st.warning("Данные о погоде не найдены. Нажмите кнопку 'Обновить данные сейчас' в боковой панели.")
    
    with tab2:
        st.header("Детали по погоде в городах")
        
        # Проверяем, есть ли выбранный город
        if selected_city and selected_city in city_names:
            city_key = city_keys[selected_city]
            
            # Попробуем получить данные за последние 7 дней для выбранного города
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")
            
            # Сначала пробуем получить расширенные данные за период
            enriched_data_period = fetch_data("/enriched", params={
                "date": datetime.now().strftime("%Y%m%d"),
                "city": selected_city,
                "start_date": start_date,
                "end_date": end_date
            })
            
            if enriched_data_period and 'data' in enriched_data_period:
                df_period = pd.DataFrame(enriched_data_period['data'])
                
                if not df_period.empty and 'date' in df_period.columns:
                    # Проверяем, есть ли данные за несколько дней
                    unique_dates = df_period['date'].nunique()
                    
                    if unique_dates > 1:
                        # Отображение информации о выбранном городе за несколько дней
                        st.subheader(f"🌤️ Погода в {selected_city} (Последние 7 дней)")
                        
                        # График температуры
                        if 'date' in df_period.columns and 'temperature' in df_period.columns:
                            temp_fig = px.line(
                                df_period,
                                x='date',
                                y='temperature',
                                title="Температура за последние 7 дней",
                                labels={'temperature': 'Температура (°C)', 'date': 'Дата'}
                            )
                            st.plotly_chart(temp_fig, use_container_width=True)
                        
                        # График комфортности
                        if 'date' in df_period.columns and 'comfort_index' in df_period.columns:
                            comfort_fig = px.line(
                                df_period,
                                x='date',
                                y='comfort_index',
                                title="Индекс комфортности за последние 7 дней",
                                markers=True,
                                labels={'comfort_index': 'Индекс комфортности', 'date': 'Дата'}
                            )
                            st.plotly_chart(comfort_fig, use_container_width=True)
                        
                        # Таблица данных за неделю
                        st.subheader("Детали за неделю")
                        # Выбираем нужные колонки
                        display_cols = ['date', 'temperature', 'feels_like', 'humidity', 'wind_speed', 'comfort_index', 'weather_description', 'recommended_activity']
                        available_cols = [col for col in display_cols if col in df_period.columns]
                        display_df = df_period[available_cols].copy()
                        display_df = display_df.sort_values('date', ascending=False)
                        
                        st.dataframe(
                            display_df,
                            hide_index=True,
                            column_config={
                                "temperature": st.column_config.NumberColumn(
                                    "Темп.",
                                    format="%.1f°C"
                                ),
                                "feels_like": st.column_config.NumberColumn(
                                    "Ощущается как",
                                    format="%.1f°C"
                                ),
                                "humidity": st.column_config.NumberColumn(
                                    "Влажность (%)"
                                ),
                                "wind_speed": st.column_config.NumberColumn(
                                    "Ветер (м/с)",
                                    format="%.1f"
                                ),
                                "comfort_index": st.column_config.ProgressColumn(
                                    "Комфортность",
                                    format="%.0f",
                                    min_value=0,
                                    max_value=100
                                )
                            }
                        )
                    
                    # Отображение текущей информации (если есть)
                    current_data = df_period[df_period['date'] == datetime.now().strftime("%Y-%m-%d")]
                    if not current_data.empty:
                        st.subheader(f"Текущая погода в {selected_city}")
                        current_row = current_data.iloc[0]
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Температра", f"{current_row.get('temperature', 'N/A')}°C")
                        with col2:
                            st.metric("Влажность", f"{current_row.get('humidity', 'N/A')}%")
                        with col3:
                            st.metric("Ветер", f"{current_row.get('wind_speed', 'N/A')} м/с")
                        
                        comfort_val = current_row.get('comfort_index', 0)
                        st.progress(int(comfort_val) if not pd.isna(comfort_val) else 0, text=f"Индекс комфортности: {comfort_val if not pd.isna(comfort_val) else 0}/100")
                        st.info(f"**Описание погоды:** {current_row.get('weather_description', 'N/A')}")
                        st.info(f"**Рекомендуемая активность:** {current_row.get('recommended_activity', 'N/A')}")
                else:
                    st.warning(f"Нет данных за выбранный период для {selected_city}")
            else:
                # Если расширенный API не поддерживает фильтрацию по дате, получаем текущие данные
                enriched_data = fetch_data("/enriched", params={"date": datetime.now().strftime("%Y%m%d")})
                if enriched_data and 'data' in enriched_data:
                    df = pd.DataFrame(enriched_data['data'])
                    city_data = df[df['city_name'] == selected_city]
                    
                    if not city_data.empty:
                        st.subheader(f"Текущая погода в {selected_city}")
                        current_row = city_data.iloc[0]
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Температура", f"{current_row.get('temperature', 'N/A')}°C")
                        with col2:
                            st.metric("Влажность", f"{current_row.get('humidity', 'N/A')}%")
                        with col3:
                            st.metric("Ветер", f"{current_row.get('wind_speed', 'N/A')} м/с")
                        
                        comfort_val = current_row.get('comfort_index', 0)
                        st.progress(int(comfort_val) if not pd.isna(comfort_val) else 0, text=f"Индекс комфортности: {comfort_val if not pd.isna(comfort_val) else 0}/100")
                        st.info(f"**Описание погоды:** {current_row.get('weather_description', 'N/A')}")
                        st.info(f"**Рекомендуемая активность:** {current_row.get('recommended_activity', 'N/A')}")
                    else:
                        st.warning(f"Нет данных для {selected_city}")
                else:
                    st.warning(f"Нет данных для {selected_city}")
        else:
            st.warning("Выберите город для просмотра деталей")
    
    with tab3:
        st.header("Агрегированные отчеты")
        
        report_type = st.selectbox(
            "Выберите тип отчета",
            ["city_rating", "district_summary", "travel_recommendations"],
            format_func=lambda x: {
                "city_rating": "Рейтинг городов для туризма",
                "district_summary": "Сводка по федеральным округам",
                "travel_recommendations": "Рекомендации для турагентств"
            }.get(x, x)
        )
        
        report_data = fetch_data(f"/aggregated/{report_type}")
        if report_data and 'data' in report_data:
            if report_type == "city_rating":
                st.subheader("🏆 Рейтинг городов для туризма")
                df = pd.DataFrame(report_data['data'])
                
                # Проверяем, есть ли колонка 'Рейтинг', если нет - добавляем
                if 'Рейтинг' not in df.columns:
                    df['Рейтинг'] = range(1, len(df) + 1)
                
                # Проверяем, есть ли колонка 'temperature', если нет - используем 'temperature_avg' или другую
                temp_col = 'temperature' if 'temperature' in df.columns else ('temperature_avg' if 'temperature_avg' in df.columns else None)
                
                display_cols = ['Рейтинг', 'city_name']
                if temp_col:
                    display_cols.append(temp_col)
                display_cols.extend(['comfort_index', 'recommended_activity', 'weather_recommendation'])
                
                available_display_cols = [col for col in display_cols if col in df.columns]
                
                st.data_editor(
                    df[available_display_cols],
                    hide_index=True,
                    column_config={
                        "comfort_index": st.column_config.ProgressColumn(
                            "Комфортность",
                            format="%.0f",
                            min_value=0,
                            max_value=100
                        ),
                        "Рейтинг": st.column_config.NumberColumn(
                            "Позиция",
                            help="Позиция в рейтинге"
                        )
                    }
                )
                
                if 'city_name' in df.columns and 'comfort_index' in df.columns:
                    fig = px.pie(
                        df,
                        names='city_name',
                        values='comfort_index',
                        title="Доля комфортности по городам",
                        hole=0.4
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Недостаточно данных для построения диаграммы")
            
            elif report_type == "district_summary":
                st.subheader("📊 Сводка по федеральным округам")
                df = pd.DataFrame(report_data['data'])
                
                st.data_editor(
                    df,
                    hide_index=True,
                    column_config={
                        "comfort_ratio": st.column_config.ProgressColumn(
                            "Доля комфортных городов",
                            format="%.1f%%",
                            min_value=0,
                            max_value=100
                        ),
                        "avg_temperature": st.column_config.NumberColumn(
                            "Средняя температура",
                            format="%.1f °C"
                        )
                    }
                )
                
                if 'federal_district' in df.columns and 'comfort_ratio' in df.columns:
                    fig = px.bar(
                        df,
                        x='federal_district',
                        y='comfort_ratio',
                        color='general_recommendation',
                        title="Комфортность по федеральным округам",
                        labels={
                            'federal_district': 'Федеральный округ',
                            'comfort_ratio': 'Доля комфортных городов (%)'
                        }
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Недостаточно данных для построения графика")
            
            elif report_type == "travel_recommendations":
                st.subheader("📝 Рекомендации для турагентств")
                df = pd.DataFrame(report_data['data'])
                
                # Группировка по категориям
                categories = {
                    "top_destination": "🏆 Топ направления",
                    "stay_home": "🏠 Оставайтесь дома",
                    "special": "ℹ️ Специальные рекомендации"
                }
                
                for category, title in categories.items():
                    category_df = df[df['category'] == category]
                    if not category_df.empty:
                        st.subheader(title)
                        for _, row in category_df.iterrows():
                            with st.expander(f"**{row['city']}**: {row['reason']}"):
                                st.write(row['recommendation'])
    
    with tab4:
        st.header("📈 Тренды и исторические данные")
        
        if selected_city and selected_city in city_names:
            city_key = city_keys[selected_city]
            
            col1, col2 = st.columns(2)
            with col1:
                days = st.slider("Количество дней для анализа трендов", 1, 30, 7)
            
            with col2:
                if st.button("📊 Получить тренды погоды", use_container_width=True):
                    with st.spinner("Получение данных о трендах..."):
                        trends = get_weather_trends(city_key, days)
                        
                        if trends:
                            st.subheader(f"Тренды погоды в {selected_city} за последние {days} дней")
                            
                            # Отображение трендов
                            trends_data = trends['trends']
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                avg_temp = trends_data.get('avg_temperature', 'N/A')
                                temp_trend = trends_data.get('temperature_trend', 'Нет данных')
                                st.metric("Средняя темп.", f"{avg_temp}°C", 
                                         f"Тренд: {temp_trend}")
                                st.metric("Макс. темп.", f"{trends_data.get('max_temperature', 'N/A')}°C")
                            with col2:
                                st.metric("Средняя влажность", f"{trends_data.get('avg_humidity', 'N/A')}%")
                                st.metric("Средний ветер", f"{trends_data.get('avg_wind_speed', 'N/A')} м/с")
                            with col3:
                                st.metric("Средний комфорт", f"{trends_data.get('avg_comfort_index', 'N/A')}/100")
                                st.metric("Дней с осадками", f"{trends_data.get('days_with_precipitation', 'N/A')}/{trends_data.get('total_days', 'N/A')}")
                            
                            # График трендов если есть данные
                            if all(key in trends_data for key in ['avg_temperature', 'max_temperature', 'avg_comfort_index']):
                                trend_df = pd.DataFrame({
                                    'Метрика': ['Средняя темп.', 'Мин. темп.', 'Макс. темп.', 'Комфортность'],
                                    'Значение': [
                                        trends_data.get('avg_temperature', 0),
                                        trends_data.get('min_temperature', 0),
                                        trends_data.get('max_temperature', 0),
                                        trends_data.get('avg_comfort_index', 0)
                                    ]
                                })
                                
                                fig = px.bar(trend_df, x='Метрика', y='Значение', 
                                            title="Основные показатели за период")
                                st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning("Нет данных о трендах для выбранного города")
            
            # Исторические данные
            st.subheader("📊 Исторические данные")
            col1, col2 = st.columns(2)
            with col1:
                start_date_hist = st.date_input("Начальная дата", 
                                               value=datetime.now() - timedelta(days=7))
            with col2:
                end_date_hist = st.date_input("Конечная дата", 
                                             value=datetime.now())
            
            if st.button("🔍 Получить исторические данные", use_container_width=True):
                with st.spinner("Получение исторических данных..."):
                    hist_data = get_historical_data(
                        city_key, 
                        start_date_hist.strftime("%Y-%m-%d"), 
                        end_date_hist.strftime("%Y-%m-%d")
                    )
                    
                    if hist_data and 'data' in hist_data:
                        st.subheader(f"Исторические данные для {selected_city}")
                        st.info(f"Период: {hist_data['date_range']}, Записей: {hist_data['total_records']}")
                        
                        hist_df = pd.DataFrame(hist_data['data'])
                        
                        if not hist_df.empty:
                            # Графики
                            col1, col2 = st.columns(2)
                            with col1:
                                if 'date' in hist_df.columns and 'temperature' in hist_df.columns:
                                    temp_fig = px.line(
                                        hist_df,
                                        x='date',
                                        y='temperature',
                                        title="Температура за выбранный период"
                                    )
                                    st.plotly_chart(temp_fig, use_container_width=True)
                                else:
                                    st.warning("Нет данных для графика температуры")
                            
                            with col2:
                                if 'date' in hist_df.columns and 'comfort_index' in hist_df.columns:
                                    comfort_fig = px.line(
                                        hist_df,
                                        x='date',
                                        y='comfort_index',
                                        title="Комфортность за выбранный период",
                                        markers=True
                                    )
                                    st.plotly_chart(comfort_fig, use_container_width=True)
                                else:
                                    st.warning("Нет данных для графика комфортности")
                            
                            # Таблица
                            st.subheader("Детали")
                            # Выбираем доступные колонки для отображения
                            display_hist_cols = ['date', 'temperature', 'feels_like', 'humidity', 'wind_speed', 'comfort_index', 'precipitation', 'weather_description']
                            available_hist_cols = [col for col in display_hist_cols if col in hist_df.columns]
                            
                            if available_hist_cols:
                                display_hist_df = hist_df[available_hist_cols].copy()
                                display_hist_df = display_hist_df.sort_values('date', ascending=False)
                                
                                st.dataframe(
                                    display_hist_df,
                                    hide_index=True,
                                    column_config={
                                        "temperature": st.column_config.NumberColumn(
                                            "Темп.",
                                            format="%.1f°C"
                                        ),
                                        "feels_like": st.column_config.NumberColumn(
                                            "Ощущается",
                                            format="%.1f°C"
                                        ),
                                        "humidity": st.column_config.NumberColumn(
                                            "Влажность (%)"
                                        ),
                                        "wind_speed": st.column_config.NumberColumn(
                                            "Ветер (м/с)",
                                            format="%.1f"
                                        ),
                                        "comfort_index": st.column_config.ProgressColumn(
                                            "Комфортность",
                                            format="%.0f",
                                            min_value=0,
                                            max_value=100
                                        ),
                                        "precipitation": st.column_config.NumberColumn(
                                            "Осадки (мм)",
                                            format="%.1f"
                                        )
                                    }
                                )
                            else:
                                st.warning("Нет подходящих колонок для отображения таблицы")
                        else:
                            st.warning("Нет исторических данных для отображения")
                    else:
                        st.warning("Нет исторических данных за указанный период")
        else:
            st.warning("Выберите город для просмотра трендов и исторических данных")
    
    with tab5:
        st.header("⚙️ Настройки системы")
        
        if st.session_state.show_add_city:
            st.subheader("➕ Добавить новый город")
            
            with st.form("add_city_form"):
                col1, col2 = st.columns(2)
                with col1:
                    ru_name = st.text_input(
                        "Русское название города*", 
                        help="Например: Москва",
                        placeholder="Введите русское название города"
                    )
                    city_name = st.text_input(
                        "Ключ для API*", 
                        help="Например: Moscow (латиницей, уникальный)",
                        placeholder="Введите уникальный ключ для API"
                    )
                    federal_district = st.text_input(
                        "Федеральный округ", 
                        value="Неизвестно",
                        help="Например: Центральный"
                    )
                    population = st.number_input(
                        "Население", 
                        value=0,
                        help="Численность населения (опционально)"
                    )
                with col2:
                    lat = st.number_input(
                        "Широта (latitude)*", 
                        value=55.7558,
                        help="Координаты широты",
                        format="%.4f"
                    )
                    lon = st.number_input(
                        "Долгота (longitude)*", 
                        value=37.6176,
                        help="Координаты долготы",
                        format="%.4f"
                    )
                    timezone = st.text_input(
                        "Часовой пояс", 
                        value="UTC+0",
                        help="Например: UTC+3"
                    )
                    tourism_season = st.text_input(
                        "Сезон туризма", 
                        value="Круглогодично",
                        help="Например: Май-Сентябрь"
                    )
                
                # Пометим обязательные поля в комментариях или добавим проверки в submit
                st.caption("* - Обязательные поля")
                
                col1, col2 = st.columns([1, 4])
                with col1:
                    check_button = st.form_submit_button("Проверить координаты")
                with col2:
                    submit_button = st.form_submit_button("Добавить город")
                
                # Проверка координат при нажатии кнопки проверки
                if check_button:
                    if not (lat and lon):
                        st.warning("Пожалуйста, введите координаты")
                    else:
                        with st.spinner("Проверка координат в Open-Meteo..."):
                            validation = validate_city_coordinates(lat, lon)
                        
                        if validation.get('valid', False):
                            st.success(validation.get('message', 'Координаты действительны!'))
                            if 'details' in validation:
                                details = validation['details']
                                st.info(f"**Текущая погода:** {details.get('current_temp', 'N/A')}°C, "
                                    f"Часовой пояс: {details.get('timezone', 'N/A')}")
                        else:
                            st.error(validation.get('message', 'Координаты недействительны'))
                
                # Добавление города при нажатии основной кнопки
                if submit_button:
                    # Проверим обязательные поля
                    if not (ru_name and city_name and lat and lon):
                        st.error("Пожалуйста, заполните все обязательные поля (*)")
                    else:
                        with st.spinner("Проверка координат в Open-Meteo..."):
                            validation = validate_city_coordinates(lat, lon)
                        
                        if not validation.get('valid', False):
                            st.error(validation.get('message', 'Координаты недействительны'))
                        else:
                            with st.spinner("Добавление города в систему..."):
                                success, message = add_city_coordinates(
                                    city_name, lat, lon, ru_name,
                                    federal_district, timezone, population, tourism_season
                                )
                            
                            if success:
                                st.success(message)
                                st.balloons()
                                
                                # Предложение собрать данные для нового города
                                st.subheader("Что дальше?")
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    if st.button("Собрать данные для этого города сейчас", use_container_width=True):
                                        with st.spinner("Запуск сбора данных..."):
                                            response = requests.post(f"{API_URL}/update/city/{city_name}")
                                            if response.status_code == 200:
                                                result = response.json()
                                                st.success(result.get('message', 'Сбор данных запущен'))
                                                st.info("Данные будут доступны через несколько минут")
                                            else:
                                                st.error("Не удалось запустить сбор данных")
                                
                                with col2:
                                    if st.button("Обновить все данные", use_container_width=True):
                                        with st.spinner("Запуск обновления данных..."):
                                            response = requests.post(f"{API_URL}/update")
                                            if response.status_code == 200:
                                                st.success("Запущен сбор данных для всех городов, включая новый")
                                                st.info("Данные будут доступны через несколько минут")
                                            else:
                                                st.error("Не удалось запустить сбор данных")
                            
                            else:
                                st.error(message)
            
            # Кнопка для закрытия формы
            if st.button("Закрыть форму добавления", use_container_width=True):
                st.session_state.show_add_city = False
                st.rerun()

        else:
            st.subheader("Управление городами")
            
            # Отображение текущих городов
            if city_coords and 'coordinates' in city_coords:
                st.write("Текущие города в системе:")
                
                # Создаем DataFrame для отображения
                cities_df = pd.DataFrame([
                    {
                        "Город": info['name'],
                        "Ключ": key,
                        "Широта": info['lat'],
                        "Долгота": info['lon']
                    } 
                    for key, info in city_coords['coordinates'].items()
                ])
                
                st.data_editor(
                    cities_df,
                    hide_index=True,
                    column_config={
                        "Город": st.column_config.TextColumn(
                            "Город",
                            width="medium"
                        ),
                        "Ключ": st.column_config.TextColumn(
                            "Ключ",
                            width="medium"
                        ),
                        "Широта": st.column_config.NumberColumn(
                            "Широта",
                            format="%.4f",
                            width="small"
                        ),
                        "Долгота": st.column_config.NumberColumn(
                            "Долгота",
                            format="%.4f",
                            width="small"
                        )
                    }
                )
                
                st.info(f"Всего городов в системе: {len(cities_df)}")
            else:
                st.warning("Не удалось загрузить список городов")
                st.info("Возможно, файл конфигурации поврежден или отсутствует")
            
            # Кнопка для отображения формы добавления
            if st.button("➕ Добавить новый город", use_container_width=True):
                st.session_state.show_add_city = True
                st.rerun()
        
        st.divider()
        
        st.subheader("Техническая информация")
        status = fetch_data("/status")
        if status:
            st.json({
                "last_update": status.get('last_update', 'Неизвестно'),
                "record_count": status.get('record_count', '0'),
                "current_time": status.get('current_time', datetime.now().isoformat())
            })
        
        st.subheader("API Endpoints")
        st.markdown("""
        - `GET /raw/{date}` - RAW данные
        - `GET /cleaned/{date}` - Очищенные данные
        - `GET /enriched/{date}` - Обогащенные данные (фильтрация по городу/дате)
        - `GET /aggregated/{report_type}` - Агрегированные отчеты
        - `POST /update` - Запуск обновления данных
        - `GET /status` - Статус системы
        - `GET /validate/city` - Проверка координат
        - `POST /config/city_coordinates` - Добавление нового города
        - `POST /update/city/{city_key}` - Сбор данных для конкретного города
        - `GET /weather_trends/{city_key}` - Тренды погоды
        - `GET /historical_data/{city_key}` - Исторические данные
        """)
        
        st.subheader("Системные настройки")
        env_vars = {
            "API_URL": API_URL,
            "OPENMETEO_API": "https://api.open-meteo.com",
            "DATA_DIRS": {
                "RAW": os.getenv('RAW_DATA_DIR', 'data/raw/openmeteo_api'),
                "CLEANED": os.getenv('CLEANED_DATA_DIR', 'data/cleaned'),
                "ENRICHED": os.getenv('ENRICHED_DATA_DIR', 'data/enriched'),
                "AGGREGATED": os.getenv('AGGREGATED_DATA_DIR', 'data/aggregated')
            },
            "EXTENDED_DATA": {
                "PAST_DAYS": 2,
                "FORECAST_DAYS": 14,
                "TOTAL_RANGE": 16  # дней
            }
        }
        st.json(env_vars)

if __name__ == "__main__":
    main()