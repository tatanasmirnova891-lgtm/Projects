import requests
import datetime
import pandas as pd

def get_weather_data(lat, lon, api_key):
    url = 'https://api.openweathermap.org/data/2.5/forecast'
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'lang': 'ru',
        'units': 'metric'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса к API: {e}")
        return None


def process_weather_data(data):
    dt = [item['dt'] for item in data['list']]
    time_f = [datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S') for t in dt]
    temp = [item['main']['temp'] for item in data['list']]
    wind_sp = [item['wind']['speed'] for item in data['list']]

    weather_info = pd.DataFrame({
        'dt': pd.to_datetime(time_f),
        'temp': temp,
        'speed_wind_s': wind_sp
    })
    weather_info['date'] = weather_info['dt'].dt.date
    weather_info['time'] = weather_info['dt'].dt.time.astype(str)

    weather_info['time_since_midnight'] = weather_info['dt'].dt.floor('D')
    weather_info['time_since_midnight'] = weather_info['dt'] - weather_info['time_since_midnight']
    weather_info['time_seconds'] = weather_info['time_since_midnight'].dt.total_seconds()

    return weather_info.drop(columns=['dt'])
