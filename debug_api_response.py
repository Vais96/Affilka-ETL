"""
Скрипт для отладки структуры ответа API
"""
import json
from datetime import datetime, timedelta
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_api_response():
    """Выводит полную структуру ответа API для отладки"""
    accounts = get_affilka_accounts()
    
    if not accounts:
        print("Не найдено аккаунтов")
        return
    
    account = accounts[0]
    api = AffilkaAPI(account['token'], account['url'])
    
    # Получаем отчет
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=7)
    
    report = api.fetch_report(
        from_date=str(from_date),
        to_date=str(to_date),
        columns=['first_deposits_count', 'deposits_count', 'deposits_sum'],
        group_by=['day', 'player']
    )
    
    if not report:
        print("Не удалось получить отчет")
        return
    
    print("=" * 60)
    print("СТРУКТУРА ОТВЕТА API")
    print("=" * 60)
    print(f"\nТип отчета: {report.get('report_type')}")
    print(f"\nКлючи верхнего уровня: {list(report.keys())}")
    
    if 'rows' in report:
        rows = report['rows']
        print(f"\nКлючи в 'rows': {list(rows.keys()) if isinstance(rows, dict) else 'rows is not a dict'}")
        
        if 'data' in rows:
            data = rows['data']
            print(f"\nКоличество строк в data: {len(data)}")
            
            if data:
                print("\n" + "=" * 60)
                print("ПЕРВАЯ СТРОКА (полная структура):")
                print("=" * 60)
                first_row = data[0]
                print(json.dumps(first_row, indent=2, default=str))
                
                print("\n" + "=" * 60)
                print("АНАЛИЗ ПЕРВОЙ СТРОКИ:")
                print("=" * 60)
                for i, field in enumerate(first_row):
                    if isinstance(field, dict):
                        name = field.get('name', 'N/A')
                        value = field.get('value', 'N/A')
                        field_type = field.get('type', 'N/A')
                        print(f"\nПоле {i}:")
                        print(f"  name: {name}")
                        print(f"  value: {value} (type: {type(value).__name__})")
                        print(f"  type: {field_type}")
                        if isinstance(value, dict):
                            print(f"  value keys: {list(value.keys())}")
                            print(f"  value content: {json.dumps(value, indent=4, default=str)}")
                
                print("\n" + "=" * 60)
                print("ВСЕ УНИКАЛЬНЫЕ ИМЕНА ПОЛЕЙ В ПЕРВОЙ СТРОКЕ:")
                print("=" * 60)
                field_names = [f.get('name') for f in first_row if isinstance(f, dict) and f.get('name')]
                for name in field_names:
                    print(f"  - {name}")

if __name__ == '__main__':
    debug_api_response()
