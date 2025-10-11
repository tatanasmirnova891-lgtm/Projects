import logging
from data_pipeline.utils import parse_date, flatten_api_data
from data_pipeline.google_sheets import GoogleSheetsUploader
from data_pipeline.database import DatabaseUploader
from data_pipeline.email_sender import EmailSender
import requests
import pandas as pd


class DataPipeline:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.sheets_uploader = GoogleSheetsUploader(config)
        self.db_uploader = DatabaseUploader(config)
        self.email_sender = EmailSender(config)

    def fetch_data(self):
        self.logger.info("Начинается скачивание данных")
        params = {
            'client': self.config['client'],
            'client_key': self.config['client_key'],
            'start': parse_date(self.config['start_date']),
            'end': parse_date(self.config['end_date']),
        }
        response = requests.get(self.config['api_url'], params=params)
        response.raise_for_status()
        self.logger.info("Данные успешно получены")
        return response.json()
    
    def calculate_metrics(self, df):
        self.logger.info("Проводится расчет необходимых метрик")
        # Расчет метрик по дням
        total_attempts_per_day = df.groupby(df['created_at'].dt.date).size()
        successful_attempts_per_day = df[df['is_correct'] == 1.0].groupby(df['created_at'].dt.date).size()
        unique_users_per_day = df.groupby(df['created_at'].dt.date)['user_id'].nunique()

        summary_df = total_attempts_per_day.to_frame(name='Всего попыток')
        summary_df['Успешных попыток'] = successful_attempts_per_day.reindex(summary_df.index, fill_value=0)
        summary_df['Уникальных пользователей'] = unique_users_per_day.reindex(summary_df.index, fill_value=0)

        summary_df.reset_index(inplace=True)
        summary_df.rename(columns={'created_at': 'Дата'}, inplace=True)

        self.logger.info("Метрики рассчитаны")

        return summary_df

    def process_data(self, raw_json):
        df = flatten_api_data(raw_json)
        df['created_at'] = df['created_at'].astype('datetime64[ns]')
        # переименование колонок
        df.columns = ['user_id', 'is_correct', 'attempt_type', 'created_at',
            'oauth_consumer_key', 'lis_result_sourcedid',
            'lis_outcome_service_url']
        # изменение структуры датафрейма (меняем колонки местами)
        df = df[['user_id', 'oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url',
            'is_correct', 'attempt_type',
            'created_at']]
        df = df.reset_index(names = 'id')

        self.logger.info("Начинается разведочный анализ данных")

        # Размерность
        shape_df = df.shape
        self.logger.info(f"Размерность данных: {shape_df}")
        
        #Типы данных
        type_df = df.dtypes
        self.logger.info(f"Размерность данных: {type_df}")

        # Проверка дубликатов по всем колонкам
        duplicates = df.duplicated().sum()
        self.logger.info(f"Найдено дубликатов строк: {duplicates}")

        # Проверка уникальности id (индекса)
        unique_ids = df['id'].is_unique
        self.logger.info(f"Все значения 'id' уникальны: {unique_ids}")

        # Проверка допустимых значений в attempt_type (пример значений)
        valid_attempt_types = {'submit', 'run', 'test'}  # заменить на реальные допустимые значения
        invalid_attempt_types = df.loc[~df['attempt_type'].isin(valid_attempt_types), 'attempt_type'].unique()
        if len(invalid_attempt_types) > 0:
            self.logger.warning(f"Обнаружены недопустимые значения в attempt_type: {invalid_attempt_types}")
        else:
            self.logger.info("Все значения attempt_type корректны")

        # Проверка диапазона is_correct
        invalid_is_correct = df.loc[(~df['is_correct'].isin([0.0, 1.0])) & (df['is_correct'].notna()), 'is_correct'].unique()
        if len(invalid_is_correct) > 0:
            self.logger.warning(f"Обнаружены недопустимые значения в is_correct: {invalid_is_correct}")
        else:
            self.logger.info("Все значения is_correct корректны")

        # Проверка диапазонов дат (например, нет будущих дат)
        max_date = df['created_at'].max()
        if max_date > pd.Timestamp.now():
            self.logger.warning(f"Обнаружена дата в будущем: {max_date}")
        else:
            self.logger.info("Даты в поле created_at соответствуют текущему времени")

        self.logger.info("Разведочный анализ завершен")

        return df
    
    
    def run(self):
        try:
            raw_data = self.fetch_data()
            df = self.process_data(raw_data)
            self.db_uploader.upload(df)
            summary_df = self.calculate_metrics(df)
            self.sheets_uploader.upload(summary_df)
            self.email_sender.send()
            self.logger.info("Пайплайн выполнен успешно")
        except Exception as e:
            self.logger.exception("Ошибка в пайплайне")
            raise