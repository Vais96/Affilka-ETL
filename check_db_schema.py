"""
Вспомогательный скрипт для проверки структуры БД
"""
from database import Database
import json

def main():
    db = Database()
    if db.connect():
        schema = db.get_table_schema('fact_click_month')
        if schema:
            print("Структура таблицы fact_click_month:")
            print(json.dumps(schema, indent=2, default=str))
        else:
            print("Не удалось получить схему таблицы")
        db.disconnect()
    else:
        print("Не удалось подключиться к БД")

if __name__ == '__main__':
    main()
