"""
Скрипт для проверки данных в БД
Сравнивает с данными из API
"""
from datetime import datetime, date
from database import Database
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_db_data():
    """Проверяет данные в БД и сравнивает с API"""
    accounts = get_affilka_accounts()
    
    if not accounts:
        print("❌ Не найдено аккаунтов")
        return
    
    account = accounts[0]
    api = AffilkaAPI(account['token'], account['url'])
    
    # Определяем текущий месяц
    today = datetime.now().date()
    month_start = date(today.year, today.month, 1)
    
    print("=" * 60)
    print(f"🔍 ПРОВЕРКА ДАННЫХ В БД ЗА {month_start.strftime('%B %Y')}")
    print("=" * 60)
    print(f"Период: {month_start} - {today}")
    print()
    
    # Получаем данные из API для сравнения
    print("1. Получение данных из API...")
    report = api.fetch_report(
        from_date=str(month_start),
        to_date=str(today),
        columns=['first_deposits_count', 'deposits_count', 'deposits_sum', 'partner_income', 'ngr'],
        group_by=['day', 'dynamic_tag_visit_id'],
        conversion_currency='EUR'
    )
    
    if not report:
        print("❌ Не удалось получить данные из API")
        return
    
    parsed_data = api.parse_report_data(report)
    
    # Агрегируем данные из API по visit_id
    api_aggregated = {}
    for record in parsed_data:
        clickid = record.get('clickid')
        if not clickid:
            continue
        
        if clickid not in api_aggregated:
            api_aggregated[clickid] = {
                'ftd': 0,
                'dep_cnt': 0,
                'dep_sum': 0.0,
                'ngr': 0.0,
                'cpa': 0.0
            }
        
        api_aggregated[clickid]['ftd'] = max(api_aggregated[clickid]['ftd'], record.get('ftd', 0) or 0)
        api_aggregated[clickid]['dep_cnt'] += record.get('dep_cnt', 0) or 0
        api_aggregated[clickid]['dep_sum'] += float(record.get('dep_sum', 0) or 0)
        api_aggregated[clickid]['ngr'] += float(record.get('ngr', 0) or 0)
        api_aggregated[clickid]['cpa'] += float(record.get('cpa', 0) or 0)
    
    print(f"   ✓ API: {len(api_aggregated)} уникальных игроков")
    
    # Получаем данные из БД
    print("\n2. Получение данных из БД...")
    db = Database()
    
    if not db.connect():
        print("❌ Не удалось подключиться к БД")
        return
    
    try:
        query = """
            SELECT 
                clickid,
                SUM(ftd) as ftd,
                SUM(dep_cnt) as dep_cnt,
                SUM(dep_sum) as dep_sum,
                SUM(ngr) as ngr,
                SUM(cpa) as cpa
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
            GROUP BY clickid
        """
        
        db.cursor.execute(query, (month_start, today))
        db_records = db.cursor.fetchall()
        
        db_aggregated = {}
        for record in db_records:
            clickid = record['clickid']
            db_aggregated[clickid] = {
                'ftd': float(record['ftd'] or 0),
                'dep_cnt': float(record['dep_cnt'] or 0),
                'dep_sum': float(record['dep_sum'] or 0),
                'ngr': float(record['ngr'] or 0),
                'cpa': float(record['cpa'] or 0)
            }
        
        print(f"   ✓ БД: {len(db_aggregated)} уникальных игроков")
        
        # Сравнение
        print("\n" + "=" * 60)
        print("📊 СРАВНЕНИЕ ДАННЫХ")
        print("=" * 60)
        
        # Игроки только в API
        only_api = set(api_aggregated.keys()) - set(db_aggregated.keys())
        # Игроки только в БД
        only_db = set(db_aggregated.keys()) - set(api_aggregated.keys())
        # Общие игроки
        common = set(api_aggregated.keys()) & set(db_aggregated.keys())
        
        print(f"\nИгроки только в API: {len(only_api)}")
        print(f"Игроки только в БД: {len(only_db)}")
        print(f"Общие игроки: {len(common)}")
        
        # Сравнение метрик для общих игроков
        if common:
            print("\n" + "=" * 60)
            print("📈 СРАВНЕНИЕ МЕТРИК (для общих игроков)")
            print("=" * 60)
            
            api_ftd = sum(api_aggregated[cid]['ftd'] for cid in common)
            db_ftd = sum(db_aggregated[cid]['ftd'] for cid in common)
            api_dep_sum = sum(api_aggregated[cid]['dep_sum'] for cid in common)
            db_dep_sum = sum(db_aggregated[cid]['dep_sum'] for cid in common)
            api_ngr = sum(api_aggregated[cid]['ngr'] for cid in common)
            db_ngr = sum(db_aggregated[cid]['ngr'] for cid in common)
            api_cpa = sum(api_aggregated[cid]['cpa'] for cid in common)
            db_cpa = sum(db_aggregated[cid]['cpa'] for cid in common)
            
            print(f"\nFTD:")
            print(f"  API: {api_ftd:,.0f}")
            print(f"  БД:  {db_ftd:,.0f}")
            print(f"  Разница: {abs(api_ftd - db_ftd):,.0f}")
            
            print(f"\nСумма депозитов:")
            print(f"  API: {api_dep_sum:,.2f}")
            print(f"  БД:  {db_dep_sum:,.2f}")
            print(f"  Разница: {abs(api_dep_sum - db_dep_sum):,.2f}")
            
            print(f"\nNGR:")
            print(f"  API: {api_ngr:,.2f}")
            print(f"  БД:  {db_ngr:,.2f}")
            print(f"  Разница: {abs(api_ngr - db_ngr):,.2f}")
            
            print(f"\nCPA (Partner income):")
            print(f"  API: {api_cpa:,.2f}")
            print(f"  БД:  {db_cpa:,.2f}")
            print(f"  Разница: {abs(api_cpa - db_cpa):,.2f}")
            
            # Проверка точности
            tolerance = 0.01
            ftd_ok = abs(api_ftd - db_ftd) < tolerance
            dep_ok = abs(api_dep_sum - db_dep_sum) < tolerance
            ngr_ok = abs(api_ngr - db_ngr) < tolerance
            cpa_ok = abs(api_cpa - db_cpa) < tolerance
            
            print("\n" + "=" * 60)
            print("✅ РЕЗУЛЬТАТ ПРОВЕРКИ")
            print("=" * 60)
            print(f"FTD: {'✓' if ftd_ok else '✗'}")
            print(f"Депозиты: {'✓' if dep_ok else '✗'}")
            print(f"NGR: {'✓' if ngr_ok else '✗'}")
            print(f"CPA: {'✓' if cpa_ok else '✗'}")
            
            if ftd_ok and dep_ok and ngr_ok and cpa_ok:
                print("\n🎉 Все данные записываются верно!")
            else:
                print("\n⚠️ Обнаружены расхождения!")
        
        # Примеры записей для проверки
        if common:
            print("\n" + "=" * 60)
            print("📋 ПРИМЕРЫ ЗАПИСЕЙ (первые 5 общих игроков)")
            print("=" * 60)
            for i, cid in enumerate(list(common)[:5], 1):
                api_data = api_aggregated[cid]
                db_data = db_aggregated[cid]
                print(f"\nИгрок {i} (clickid: {cid[:20]}...):")
                print(f"  FTD:      API={api_data['ftd']:.0f}  БД={db_data['ftd']:.0f}  {'✓' if abs(api_data['ftd'] - db_data['ftd']) < 0.01 else '✗'}")
                print(f"  Депозиты: API={api_data['dep_sum']:,.2f}  БД={db_data['dep_sum']:,.2f}  {'✓' if abs(api_data['dep_sum'] - db_data['dep_sum']) < 0.01 else '✗'}")
                print(f"  NGR:      API={api_data['ngr']:,.2f}  БД={db_data['ngr']:,.2f}  {'✓' if abs(api_data['ngr'] - db_data['ngr']) < 0.01 else '✗'}")
                print(f"  CPA:      API={api_data['cpa']:,.2f}  БД={db_data['cpa']:,.2f}  {'✓' if abs(api_data['cpa'] - db_data['cpa']) < 0.01 else '✗'}")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке данных: {e}", exc_info=True)
    finally:
        db.disconnect()


if __name__ == '__main__':
    verify_db_data()
