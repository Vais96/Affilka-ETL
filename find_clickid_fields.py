"""
Поиск полей visit_id, sub_id и других идентификаторов визита в API
"""
import json
from datetime import datetime, timedelta
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_clickid_fields():
    """Ищет поля для clickid в различных вариантах запросов"""
    accounts = get_affilka_accounts()
    
    if not accounts:
        print("Не найдено аккаунтов")
        return
    
    account = accounts[0]
    api = AffilkaAPI(account['token'], account['url'])
    
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=3)
    
    # Получаем список всех доступных колонок
    print("=" * 60)
    print("ДОСТУПНЫЕ КОЛОНКИ В API:")
    print("=" * 60)
    columns = api.get_available_columns()
    if columns:
        # Ищем колонки, связанные с visit_id, sub_id, clickid
        relevant = [c for c in columns if any(term in c.lower() for term in ['visit', 'sub', 'click', 'tag', 'id'])]
        print(f"\nВсего колонок: {len(columns)}")
        print(f"\nРелевантные колонки (visit/sub/click/tag/id):")
        for col in relevant:
            print(f"  - {col}")
        print(f"\nВсе колонки:")
        for col in columns:
            print(f"  - {col}")
    
    # Тестируем различные варианты group_by
    test_cases = [
        {'group_by': ['day'], 'columns': ['first_deposits_count', 'deposits_count']},
        {'group_by': ['day', 'campaign'], 'columns': ['first_deposits_count', 'deposits_count']},
        {'group_by': ['day', 'player'], 'columns': ['first_deposits_count', 'deposits_count']},
        {'group_by': ['day'], 'columns': ['first_deposits_count', 'deposits_count', 'visits_count']},
    ]
    
    # Если есть колонки с visit/sub, пробуем их
    if columns:
        visit_cols = [c for c in columns if 'visit' in c.lower()]
        if visit_cols:
            test_cases.append({
                'group_by': ['day'],
                'columns': ['first_deposits_count', 'deposits_count'] + visit_cols[:2]
            })
    
    for i, test_case in enumerate(test_cases, 1):
        print("\n" + "=" * 60)
        print(f"ТЕСТ {i}: group_by={test_case['group_by']}, columns={test_case['columns']}")
        print("=" * 60)
        
        try:
            report = api.fetch_report(
                from_date=str(from_date),
                to_date=str(to_date),
                columns=test_case['columns'],
                group_by=test_case['group_by']
            )
            
            if report and 'rows' in report and 'data' in report['rows']:
                if report['rows']['data']:
                    first_row = report['rows']['data'][0]
                    field_names = [f.get('name') for f in first_row if isinstance(f, dict) and f.get('name')]
                    print(f"Поля в ответе: {field_names}")
                    
                    # Ищем поля с visit, sub, click, tag
                    relevant_fields = []
                    for field in first_row:
                        if isinstance(field, dict):
                            name = field.get('name', '')
                            if any(term in name.lower() for term in ['visit', 'sub', 'click', 'tag']):
                                relevant_fields.append((name, field.get('value')))
                    
                    if relevant_fields:
                        print(f"\nНайдены релевантные поля:")
                        for name, value in relevant_fields:
                            print(f"  - {name}: {value}")
                    else:
                        print(f"\nРелевантные поля не найдены")
                else:
                    print("Нет данных в ответе")
        except Exception as e:
            print(f"Ошибка: {e}")

if __name__ == '__main__':
    find_clickid_fields()
