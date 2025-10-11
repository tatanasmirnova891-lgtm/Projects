import gspread
from google.oauth2.service_account import Credentials
import gspread_dataframe as gd
import logging

class GoogleSheetsUploader:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        self.creds = Credentials.from_service_account_file(config['service_account_file'], scopes=scopes)
        self.client = gspread.authorize(self.creds)
        self.spreadsheet_id = config['spreadsheet_id']

    def upload(self, summary_df):
        try:
            self.logger.info("Загрузка рассчитанных метрик в Google Sheets")
            spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            worksheet = spreadsheet.worksheet('RawData')
            worksheet.clear()
            gd.set_with_dataframe(worksheet, summary_df)
            self.logger.info("Данные загружены в Google Sheets")
        except Exception as e:
            self.logger.error(f"Ошибка загрузки в Google Sheets: {e}")
            raise
