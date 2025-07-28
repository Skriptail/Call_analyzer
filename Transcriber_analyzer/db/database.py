import sqlite3
import json
from datetime import datetime
import logging
import os

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path=None):
        """Инициализация подключения к базе данных"""
        # Используем путь из переменной окружения или значение по умолчанию
        self.db_path = db_path or os.environ.get('DB_PATH', 'calls.db')
        # Создаем директорию для базы данных, если её нет
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        logger.info(f"Используется база данных: {self.db_path}")
        self.init_db()

    def get_connection(self):
        """Создает новое подключение к БД"""
        return sqlite3.connect(self.db_path)

    def init_db(self):
        """Инициализация структуры базы данных"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Таблица звонков с поддержкой архивирования
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS calls (
                    communication_id TEXT PRIMARY KEY,
                    call_date TIMESTAMP,
                    client_phone TEXT,
                    staff_phone TEXT,
                    duration INTEGER,
                    client_audio_path TEXT,
                    staff_audio_path TEXT,
                    transcript_path TEXT,
                    metadata JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_archived INTEGER DEFAULT 0,
                    archive_path TEXT,
                    archive_date TIMESTAMP
                )
            ''')
            conn.commit()

    def add_call(self, communication_id: str, call_data: dict = None):
        """
        Добавление нового звонка в БД
        Args:
            communication_id: ID звонка
            call_data: Данные о звонке из API UIS
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Подготовка данных
            call_date = datetime.now()  
            metadata = json.dumps(call_data) if call_data else None
            
            cursor.execute('''
                INSERT OR IGNORE INTO calls (
                    communication_id,
                    call_date,
                    metadata
                ) VALUES (?, ?, ?)
            ''', (
                communication_id,
                call_date,
                metadata
            ))
            conn.commit()

    def update_call_paths(self, communication_id: str, client_path: str = None, 
                         staff_path: str = None, transcript_path: str = None):
        """Обновление путей к файлам звонка"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            update_fields = []
            params = []
            
            if client_path:
                update_fields.append("client_audio_path = ?")
                params.append(client_path)
            if staff_path:
                update_fields.append("staff_audio_path = ?")
                params.append(staff_path)
            if transcript_path:
                update_fields.append("transcript_path = ?")
                params.append(transcript_path)
                
            if update_fields:
                query = f'''
                    UPDATE calls 
                    SET {", ".join(update_fields)}
                    WHERE communication_id = ?
                '''
                params.append(communication_id)
                cursor.execute(query, params)
                conn.commit()

    def get_call(self, communication_id: str) -> dict:
        """Получение информации о звонке"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    communication_id,
                    call_date,
                    client_phone,
                    staff_phone,
                    duration,
                    client_audio_path,
                    staff_audio_path,
                    transcript_path,
                    metadata,
                    created_at
                FROM calls
                WHERE communication_id = ?
            ''', (communication_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'communication_id': row[0],
                    'call_date': row[1],
                    'client_phone': row[2],
                    'staff_phone': row[3],
                    'duration': row[4],
                    'client_audio_path': row[5],
                    'staff_audio_path': row[6],
                    'transcript_path': row[7],
                    'metadata': json.loads(row[8]) if row[8] else None,
                    'created_at': row[9]
                }
            return None

    def get_processed_communication_ids(self):
        """Возвращает список communication_id, у которых есть transcript_path (обработанные звонки, кроме NO_WAV)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT communication_id FROM calls WHERE transcript_path IS NOT NULL AND transcript_path != '' AND transcript_path != 'NO_WAV'
            ''')
            rows = cursor.fetchall()
            return [row[0] for row in rows]

    def get_calls_older_than(self, days: int):
        """Получает звонки старше указанного количества дней"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM calls 
                WHERE call_date < datetime('now', '-{} days') 
                AND is_archived = 0
            '''.format(days))
            
            rows = cursor.fetchall()
            return [{
                'communication_id': row[0],
                'call_date': row[1],
                'client_phone': row[2],
                'staff_phone': row[3],
                'duration': row[4],
                'client_audio_path': row[5],
                'staff_audio_path': row[6],
                'transcript_path': row[7],
                'metadata': json.loads(row[8]) if row[8] else None,
                'created_at': row[9],
                'is_archived': row[10],
                'archive_path': row[11],
                'archive_date': row[12]
            } for row in rows]

    def mark_as_archived(self, communication_id: str, archive_path: str):
        """Помечает звонок как архивированный"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE calls 
                SET is_archived = 1, archive_path = ?, archive_date = CURRENT_TIMESTAMP
                WHERE communication_id = ?
            ''', (archive_path, communication_id))
            conn.commit()

    def get_calls_for_analysis(self, date_from: str, date_to: str):
        """Получает звонки для анализа за период"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM calls 
                WHERE call_date BETWEEN ? AND ?
                ORDER BY call_date
            ''', (date_from, date_to))
            
            rows = cursor.fetchall()
            return [{
                'communication_id': row[0],
                'call_date': row[1],
                'client_phone': row[2],
                'staff_phone': row[3],
                'duration': row[4],
                'client_audio_path': row[5],
                'staff_audio_path': row[6],
                'transcript_path': row[7],
                'metadata': json.loads(row[8]) if row[8] else None,
                'created_at': row[9],
                'is_archived': row[10],
                'archive_path': row[11],
                'archive_date': row[12]
            } for row in rows]

# Создаем экземпляр базы данных
db = Database() 