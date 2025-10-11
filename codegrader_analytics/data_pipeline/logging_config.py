import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

def clean_old_logs(log_dir="logs", retention_days=3):
    """
    Создаёт папку для логов, если она не существует,
    и удаляет логи старше retention_days дней (по имени файла YYYY-MM-DD.log).
    """
    os.makedirs(log_dir, exist_ok=True)
    now = datetime.now()

    for filename in os.listdir(log_dir):
        if filename.endswith(".log"):
            filepath = os.path.join(log_dir, filename)
            try:
                file_date = datetime.strptime(filename[:-4], "%Y-%m-%d")
                if now - file_date > timedelta(days=retention_days):
                    os.remove(filepath)
                    logging.info(f"Удалён старый лог: {filename}")
            except ValueError:
                # Файлы с некорректным именем игнорируются
                pass

def setup_logging(log_dir="logs", retention_days=3):
    """
    Настраивает логирование с ротацией файлов и очисткой старых логов.
    
    Логи записываются в папку log_dir с именами файлов по дате (YYYY-MM-DD.log),
    размер файла ограничен 5 МБ, хранится до 3 резервных копий.
    """
    clean_old_logs(log_dir, retention_days)

    now = datetime.now()
    log_file = os.path.join(log_dir, now.strftime("%Y-%m-%d") + ".log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')

    rotating_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5 МБ
        backupCount=3,
        encoding='utf-8'
    )
    rotating_handler.setFormatter(formatter)
    logger.addHandler(rotating_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
