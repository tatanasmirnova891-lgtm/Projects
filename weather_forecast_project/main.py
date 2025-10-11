from pathlib import Path
from dotenv import load_dotenv
import os
from utils.weather_api import get_weather_data, process_weather_data
from utils.visualization import plot_weather

def main():
    # загрузка ключа API из файла конфигурации
    base_dir = Path.cwd()
    dotenv_path = base_dir / 'config.env'
    load_dotenv(dotenv_path=dotenv_path)
    api_key = os.getenv('API_KEY')
    # проверка наличие API ключа
    if not api_key:
        print("Ошибка: Не найден API ключ в config.env")
        return

    lat = '59.9386'  # Санкт-Петербург
    lon = '30.3141'

    data = get_weather_data(lat, lon, api_key)
    if data:
        weather_info = process_weather_data(data)
        plot_weather(weather_info)
    else:
        print("Не удалось получить данные о погоде.")

if __name__ == '__main__':
    main()
