"""
Скрипт для проверки загруженных данных в БД
"""
from database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_loaded_data():
    """Проверяет загруженные данные в БД"""
    db = Database()
    
    if not db.connect():
        print("Не удалось подключиться к БД")
        return
    
    try:
        # Получаем последние 10 записей
        query = """
            SELECT * FROM fact_click_month 
            ORDER BY period_date DESC, clickid DESC 
            LIMIT 10
        """
        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        
        if not rows:
            print("В таблице нет данных")
            return
        
        print("=" * 60)
        print(f"Последние {len(rows)} записей в fact_click_month:")
        print("=" * 60)
        
        for i, row in enumerate(rows, 1):
            print(f"\nЗапись {i}:")
            for key, value in row.items():
                if value is not None:
                    print(f"  {key}: {value}")
        
        # Статистика
        db.cursor.execute("SELECT COUNT(*) as total FROM fact_click_month")
        total = db.cursor.fetchone()['total']
        print("\n" + "=" * 60)
        print(f"Всего записей в таблице: {total}")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Ошибка при проверке данных: {e}")
    finally:
        db.disconnect()

if __name__ == '__main__':
    check_loaded_data()
