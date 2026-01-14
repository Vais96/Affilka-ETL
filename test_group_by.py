"""
Тестирование различных вариантов group_by для получения player_id
"""
import json
from datetime import datetime, timedelta
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_group_by_variants():
    """Тестирует различные варианты group_by"""
    accounts = get_affilka_accounts()
    
    if not accounts:
        print("Не найдено аккаунтов")
        return
    
    account = accounts[0]
    api = AffilkaAPI(account['token'], account['url'])
    
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=3)
    
    # Вариант 1: только player в group_by
    print("=" * 60)
    print("ВАРИАНТ 1: group_by=['day', 'player']")
    print("=" * 60)
    report1 = api.fetch_report(
        from_date=str(from_date),
        to_date=str(to_date),
        columns=['first_deposits_count', 'deposits_count'],
        group_by=['day', 'player']
    )
    if report1 and 'rows' in report1 and 'data' in report1['rows']:
        if report1['rows']['data']:
            first_row = report1['rows']['data'][0]
            field_names = [f.get('name') for f in first_row if isinstance(f, dict)]
            print(f"Поля в ответе: {field_names}")
    
    # Вариант 2: попробуем добавить player в columns
    print("\n" + "=" * 60)
    print("ВАРИАНТ 2: columns включают player, group_by=['day', 'player']")
    print("=" * 60)
    report2 = api.fetch_report(
        from_date=str(from_date),
        to_date=str(to_date),
        columns=['first_deposits_count', 'deposits_count', 'player'],
        group_by=['day', 'player']
    )
    if report2 and 'rows' in report2 and 'data' in report2['rows']:
        if report2['rows']['data']:
            first_row = report2['rows']['data'][0]
            field_names = [f.get('name') for f in first_row if isinstance(f, dict)]
            print(f"Поля в ответе: {field_names}")
            print("\nПервая строка:")
            print(json.dumps(first_row, indent=2, default=str))
    
    # Вариант 3: только day (без player)
    print("\n" + "=" * 60)
    print("ВАРИАНТ 3: group_by=['day'] (без player)")
    print("=" * 60)
    report3 = api.fetch_report(
        from_date=str(from_date),
        to_date=str(to_date),
        columns=['first_deposits_count', 'deposits_count'],
        group_by=['day']
    )
    if report3 and 'rows' in report3 and 'data' in report3['rows']:
        if report3['rows']['data']:
            first_row = report3['rows']['data'][0]
            field_names = [f.get('name') for f in first_row if isinstance(f, dict)]
            print(f"Поля в ответе: {field_names}")
    
    # Вариант 4: попробуем campaign вместо player
    print("\n" + "=" * 60)
    print("ВАРИАНТ 4: group_by=['day', 'campaign']")
    print("=" * 60)
    report4 = api.fetch_report(
        from_date=str(from_date),
        to_date=str(to_date),
        columns=['first_deposits_count', 'deposits_count'],
        group_by=['day', 'campaign']
    )
    if report4 and 'rows' in report4 and 'data' in report4['rows']:
        if report4['rows']['data']:
            first_row = report4['rows']['data'][0]
            field_names = [f.get('name') for f in first_row if isinstance(f, dict)]
            print(f"Поля в ответе: {field_names}")
            print("\nПервая строка:")
            print(json.dumps(first_row, indent=2, default=str))

if __name__ == '__main__':
    test_group_by_variants()
