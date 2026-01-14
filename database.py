"""
Модуль для работы с базой данных
"""
import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional
from config import DB_CONFIG
import logging

logger = logging.getLogger(__name__)


class Database:
    """Класс для работы с базой данных"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Подключение к базе данных"""
        try:
            self.connection = mysql.connector.connect(**DB_CONFIG)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                logger.info("Успешное подключение к базе данных")
                return True
        except Error as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return False
    
    def disconnect(self):
        """Отключение от базы данных"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Отключение от базы данных")
    
    def get_table_schema(self, table_name: str = 'fact_click_month') -> Optional[List[Dict[str, Any]]]:
        """Получает схему таблицы"""
        try:
            self.cursor.execute(f"DESCRIBE {table_name}")
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"Ошибка получения схемы таблицы {table_name}: {e}")
            return None
    
    def upsert_fact_click_month(self, data: List[Dict[str, Any]], account_id: Optional[str] = None):
        """
        Выполняет upsert данных в таблицу fact_click_month
        
        Args:
            data: Список словарей с данными для вставки
            account_id: Идентификатор аккаунта (для масштабирования)
        """
        if not data:
            logger.warning("Нет данных для загрузки")
            return
        
        try:
            # Получаем схему таблицы для определения полей
            schema = self.get_table_schema()
            if not schema:
                logger.error("Не удалось получить схему таблицы")
                return
            
            # Извлекаем имена колонок и их типы
            columns = [col['Field'] for col in schema]
            column_types = {col['Field']: col['Type'] for col in schema}
            
            # Определяем ключевые поля для upsert
            # Проверяем наличие полей в таблице
            key_fields = []
            if 'period_date' in columns:
                key_fields.append('period_date')
            elif 'period' in columns:
                key_fields.append('period')
            elif 'date' in columns:
                key_fields.append('date')
            
            if 'clickid' in columns:
                key_fields.append('clickid')
            elif 'click_id' in columns:
                key_fields.append('click_id')
            
            # source является частью первичного ключа
            if 'source' in columns:
                key_fields.append('source')
            
            if 'account_id' in columns and account_id:
                key_fields.append('account_id')
            
            if not key_fields:
                logger.error("Не найдены ключевые поля для upsert (period_date/period/date, clickid)")
                return
            
            # Маппинг полей из данных к полям БД
            field_mapping = {
                'period_date': ['period_date', 'period', 'date'],
                'clickid': ['clickid', 'click_id'],
                'ftd': ['ftd', 'ftd_count', 'first_deposits_count'],
                'dep_cnt': ['dep_cnt', 'deposits_count', 'dep_count'],
                'dep_sum': ['dep_sum', 'deposits_sum', 'dep_sum_amount'],
                'ngr': ['ngr'],
                'cpa': ['cpa', 'partner_income', 'clean_net_revenue'],
            }
            
            # Формируем SQL для upsert
            # Используем только те колонки, которые есть в данных или нужны для ключа
            insert_columns = []
            for col in columns:
                # Включаем колонку если она есть в данных или это специальные поля
                if col == 'account_id' and account_id:
                    insert_columns.append(col)
                elif col == 'source':
                    # source всегда включаем (часть первичного ключа)
                    insert_columns.append(col)
                elif any(col in mapping for mapping in field_mapping.values()):
                    insert_columns.append(col)
                elif col in key_fields:
                    insert_columns.append(col)
            
            if not insert_columns:
                logger.error("Не найдено полей для вставки")
                return
            
            placeholders = ', '.join(['%s'] * len(insert_columns))
            columns_str = ', '.join(insert_columns)
            
            # Для ON DUPLICATE KEY UPDATE обновляем все поля кроме ключевых
            update_fields = [f"{col} = VALUES({col})" for col in insert_columns if col not in key_fields]
            if not update_fields:
                # Если нет полей для обновления, просто обновляем все
                update_fields = [f"{col} = VALUES({col})" for col in insert_columns if col not in key_fields]
            update_str = ', '.join(update_fields) if update_fields else f"{insert_columns[0]} = VALUES({insert_columns[0]})"
            
            sql = f"""
                INSERT INTO fact_click_month ({columns_str})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_str}
            """
            
            # Подготавливаем данные для вставки
            values_list = []
            for row in data:
                values = []
                for col in insert_columns:
                    value = None
                    
                    # Маппинг значений
                    if col == 'account_id':
                        value = account_id
                    elif col == 'source':
                        # Для Affilka всегда используем 'affilka'
                        value = 'affilka'
                    elif col == 'ngr':
                        # NGR может быть не заполнен, устанавливаем 0 по умолчанию
                        value = row.get('ngr', 0) or 0
                    elif col in row:
                        value = row[col]
                    else:
                        # Пробуем найти значение через маппинг
                        for db_field, source_fields in field_mapping.items():
                            if col in source_fields:
                                for source_field in source_fields:
                                    if source_field in row:
                                        value = row[source_field]
                                        break
                                break
                    
                    values.append(value)
                values_list.append(tuple(values))
            
            # Выполняем batch insert
            self.cursor.executemany(sql, values_list)
            self.connection.commit()
            
            logger.info(f"Успешно загружено {len(data)} записей в fact_click_month")
            
        except Error as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def enrich_dims_from_keitaro(self, period_date_start: Optional[str] = None, period_date_end: Optional[str] = None):
        """
        Обогащает fact_click_month данными из Keitaro через v_click_dims
        Обновляет buyer_id, offer_id, creative_id для записей из Affilka
        
        v_click_dims связывает clickid с buyer_id, offer_id, creative_id из Keitaro через fact_conversions
        Использует самую раннюю конверсию по каждому clickid для стабильного маппинга
        
        Args:
            period_date_start: Начальная дата периода (опционально, для ограничения обновления)
            period_date_end: Конечная дата периода (опционально, для ограничения обновления)
        
        Returns:
            Количество обновленных записей
        """
        try:
            # Проверяем наличие view v_click_dims
            self.cursor.execute("SHOW TABLES LIKE 'v_click_dims'")
            if not self.cursor.fetchone():
                logger.warning("View v_click_dims не найдена, пропускаем обогащение из Keitaro")
                return 0
            
            # Получаем схему таблицы для проверки наличия полей
            schema = self.get_table_schema('fact_click_month')
            if not schema:
                logger.error("Не удалось получить схему таблицы fact_click_month")
                return 0
            
            columns = [col['Field'] for col in schema]
            
            # Формируем список полей для обновления
            update_fields = []
            if 'buyer_id' in columns:
                update_fields.append('f.buyer_id = v.buyer_id')
            if 'offer_id' in columns:
                update_fields.append('f.offer_id = v.offer_id')
            if 'creative_id' in columns:
                update_fields.append('f.creative_id = v.creative_id')
            
            if not update_fields:
                logger.warning("Не найдено полей для обогащения (buyer_id, offer_id, creative_id)")
                return 0
            
            # Формируем WHERE условие для периода
            # Обновляем только записи, где хотя бы одно из полей NULL
            where_conditions = ["f.source = 'affilka'"]
            where_conditions.append("(" + " OR ".join([
                f"f.{field.split('=')[0].split('.')[-1].strip()} IS NULL" 
                for field in update_fields
            ]) + ")")
            
            params = []
            if period_date_start:
                where_conditions.append("f.period_date >= %s")
                params.append(period_date_start)
            
            if period_date_end:
                where_conditions.append("f.period_date <= %s")
                params.append(period_date_end)
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Обновляем поля через JOIN с v_click_dims
            # Используем LOWER(TRIM()) для нормализации clickid при сравнении (как в v_click_dims)
            update_sql = f"""
                UPDATE fact_click_month f
                INNER JOIN v_click_dims v ON LOWER(TRIM(f.clickid)) = LOWER(TRIM(v.clickid))
                SET {', '.join(update_fields)}
                {where_clause}
                    AND v.clickid IS NOT NULL
            """
            
            self.cursor.execute(update_sql, params)
            updated_count = self.cursor.rowcount
            self.connection.commit()
            
            if updated_count > 0:
                fields_str = ', '.join([f.split('=')[0].split('.')[-1].strip() for f in update_fields])
                logger.info(f"Обновлено {updated_count} записей с полями ({fields_str}) из Keitaro через v_click_dims")
            else:
                logger.debug("Нет записей для обогащения из Keitaro")
            
            return updated_count
            
        except Error as e:
            logger.error(f"Ошибка при обогащении из Keitaro: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def __enter__(self):
        """Контекстный менеджер для автоматического подключения"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер для автоматического отключения"""
        self.disconnect()
