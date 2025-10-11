import pandas as pd
from datetime import datetime
import ast
import logging


def parse_date(date_str):
    # Парсинг даты в нужный формат
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S.%f")


def flatten_api_data(raw_json):
    # Преобразование вложенных данных в "плоский" DataFrame
    logger = logging.getLogger(__name__)
    flattened_data = []

    for entry in raw_json:
        try:
            nested_dict = ast.literal_eval(entry.get('passback_params', '{}'))
        except Exception:
            nested_dict = {}
        flat_entry = {k: v for k, v in entry.items() if k != 'passback_params'}
        flat_entry.update(nested_dict)
        flattened_data.append(flat_entry)

    df = pd.DataFrame(flattened_data)
    # Переименование колонок, обработка дат и типов тут по необходимости
    logger.info("API данные преобразованы в DataFrame")
    return df
