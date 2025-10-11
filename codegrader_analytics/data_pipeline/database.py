import psycopg2
from psycopg2.extras import execute_values
import logging

class DatabaseUploader:
    def __init__(self, config):
        self.db_params = config['db_params']
        self.logger = logging.getLogger(__name__)

    def upload(self, df):
        try:
            self.logger.info("Загрузка данных в базу данных")
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_config (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT,
                    oauth_consumer_key TEXT,
                    lis_result_sourcedid TEXT,
                    lis_outcome_service_url TEXT,
                    is_correct REAL,
                    attempt_type TEXT,
                    created_at TIMESTAMP
                )
            ''')
            conn.commit()

            data_tuples = [tuple(row) for row in df.to_numpy()]
            insert_query = '''
                INSERT INTO user_config (
                    id, user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
                    is_correct, attempt_type, created_at
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    user_id=EXCLUDED.user_id,
                    oauth_consumer_key=EXCLUDED.oauth_consumer_key,
                    lis_result_sourcedid=EXCLUDED.lis_result_sourcedid,
                    lis_outcome_service_url=EXCLUDED.lis_outcome_service_url,
                    is_correct=EXCLUDED.is_correct,
                    attempt_type=EXCLUDED.attempt_type,
                    created_at=EXCLUDED.created_at
            '''
            execute_values(cursor, insert_query, data_tuples)
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info("Данные записаны в базу данных")
        except Exception as e:
            self.logger.error(f"Ошибка при записи в БД: {e}")
            raise
