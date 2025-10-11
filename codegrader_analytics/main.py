import logging
import os
from dotenv import load_dotenv
from data_pipeline.pipeline import DataPipeline
from data_pipeline.logging_config import setup_logging


def main():
    load_dotenv(dotenv_path=r'C:\Users\User\Downloads\SIMULATIVE\Итоговая работа Python\project_root\config\config.env')  # Загрузка переменных из .env

    config = {
        'start_date': os.getenv('START_DATE'),
        'end_date': os.getenv('END_DATE'),
        'log_dir': os.getenv('LOG_DIR'),

        'client': os.getenv('CLIENT'),
        'client_key': os.getenv('CLIENT_KEY'),
        'api_url': os.getenv('API_URL'),

        'service_account_file': os.getenv('SERVICE_ACCOUNT_FILE'),
        'spreadsheet_id': os.getenv('SPREADSHEET_ID'),

        'db_params': {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
        },

        'smtp_server': os.getenv('SMTP_SERVER'),
        'smtp_port': int(os.getenv('SMTP_PORT')),
        'sender_email': os.getenv('SENDER_EMAIL'),
        'email_password': os.getenv('EMAIL_PASSWORD'),
        'receiver_email': os.getenv('RECEIVER_EMAIL'),
        'email_subject': os.getenv('EMAIL_SUBJECT'),
        'email_body': os.getenv('EMAIL_BODY'),
    }

    setup_logging(config['log_dir'])
    logger = logging.getLogger(__name__)
    logger.info("Запуск программы")

    pipeline = DataPipeline(config)
    try:
        pipeline.run()
    except Exception:
        logger.exception("Ошибка при выполнении пайплайна")


if __name__ == "__main__":
    logger = setup_logging()
    main()