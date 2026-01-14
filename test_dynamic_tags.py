"""
Тестирование dynamic_tag для получения visit_id/sub_id
"""
import json
from datetime import datetime, timedelta
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_dynamic_tags():
    """Тестирует dynamic_tag в group_by"""
    accounts = get_affilka_accounts()
    
    if not accounts:
        print("Не найдено аккаунтов")
        return
    
    account = accounts[0]
    api = AffilkaAPI(account['token'], account['url'])
    
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=3)
    
    # Получаем список доступных groupers
    print("=" * 60)
    print("ПОЛУЧЕНИЕ ДОСТУПНЫХ GROUPERS:")
    print("=" * 60)
    
    try:
        # Пробуем получить список доступных groupers через attributes endpoint
        url = f"{api.base_url}/api/customer/v1/partner/report/attributes"
        response = api.session.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'available_groupers' in data:
                groupers = data['available_groupers']
                print(f"\nДоступные groupers ({len(groupers)}):")
                for grouper in groupers:
                    print(f"  - {grouper}")
                
                # Ищем dynamic_tag
                dynamic_tags = [g for g in groupers if 'dynamic_tag' in g.lower() or 'tag' in g.lower()]
                if dynamic_tags:
                    print(f"\nНайдены dynamic_tag groupers:")
                    for tag in dynamic_tags:
                        print(f"  - {tag}")
                    
                    # Пробуем использовать первый dynamic_tag
                    if dynamic_tags:
                        test_tag = dynamic_tags[0]
                        print(f"\n" + "=" * 60)
                        print(f"ТЕСТ: group_by=['day', '{test_tag}']")
                        print("=" * 60)
                        
                        report = api.fetch_report(
                            from_date=str(from_date),
                            to_date=str(to_date),
                            columns=['first_deposits_count', 'deposits_count'],
                            group_by=['day', test_tag]
                        )
                        
                        if report and 'rows' in report and 'data' in report['rows']:
                            if report['rows']['data']:
                                first_row = report['rows']['data'][0]
                                field_names = [f.get('name') for f in first_row if isinstance(f, dict) and f.get('name')]
                                print(f"Поля в ответе: {field_names}")
                                
                                # Выводим первую строку полностью
                                print(f"\nПервая строка:")
                                print(json.dumps(first_row, indent=2, default=str))
            else:
                print("Нет available_groupers в ответе")
                print(f"Ответ: {json.dumps(data, indent=2, default=str)}")
        else:
            print(f"Ошибка запроса: {response.status_code}")
            print(f"Ответ: {response.text}")
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_dynamic_tags()
