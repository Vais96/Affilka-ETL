"""
Скрипт для проверки данных из API за месяц без разбивки по дням
Разделяет на новых (FTD=1) и старых (FTD=0) игроков
"""
from datetime import datetime, date
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_api_data_monthly():
    """Проверяет данные из API за месяц без разбивки по дням"""
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
    print(f"📊 ДАННЫЕ ИЗ API ЗА {month_start.strftime('%B %Y')} (без разбивки по дням)")
    print("=" * 60)
    print(f"Период: {month_start} - {today}")
    print(f"URL: {account['url']}")
    print()
    
    # Получаем данные из API с группировкой по дням и visit_id
    # Потом агрегируем по visit_id в скрипте
    # Используем конвертацию в EUR для сравнения со скриншотом
    report = api.fetch_report(
        from_date=str(month_start),
        to_date=str(today),
        columns=['first_deposits_count', 'deposits_count', 'deposits_sum', 'partner_income', 'ngr'],
        group_by=['day', 'dynamic_tag_visit_id'],  # Группируем по дням и visit_id
        conversion_currency='EUR'  # Конвертируем все валюты в EUR
    )
    
    if not report:
        print("❌ Не удалось получить данные из API")
        return
    
    # Парсим данные
    parsed_data = api.parse_report_data(report)
    
    if not parsed_data:
        print("❌ Нет данных после парсинга")
        return
    
    print(f"✓ Получено {len(parsed_data)} записей из API")
    print()
    
    # Агрегируем данные по visit_id (суммируем по всем дням для каждого игрока)
    # Фильтруем только EUR для сравнения со скриншотом
    aggregated = {}
    for record in parsed_data:
        clickid = record.get('clickid')
        if not clickid:
            continue
        
        # Все валюты уже конвертированы в EUR через conversion_currency
        # Не фильтруем по валюте - просто суммируем
        
        if clickid not in aggregated:
            aggregated[clickid] = {
                'clickid': clickid,
                'ftd': 0,
                'dep_cnt': 0,
                'dep_sum': 0.0,
                'ngr': 0.0,
                'cpa': 0.0
            }
        
        # Суммируем метрики (FTD берем максимальное значение, так как это флаг)
        aggregated[clickid]['ftd'] = max(aggregated[clickid]['ftd'], record.get('ftd', 0) or 0)
        aggregated[clickid]['dep_cnt'] += record.get('dep_cnt', 0) or 0
        aggregated[clickid]['dep_sum'] += float(record.get('dep_sum', 0) or 0)
        aggregated[clickid]['ngr'] += float(record.get('ngr', 0) or 0)
        aggregated[clickid]['cpa'] += float(record.get('cpa', 0) or 0)
    
    aggregated_list = list(aggregated.values())
    print(f"✓ Агрегировано в {len(aggregated_list)} уникальных игроков")
    print()
    
    # Разделяем на новых (FTD=1) и старых (FTD=0) игроков
    new_players = [r for r in aggregated_list if r.get('ftd', 0) >= 1]
    old_players = [r for r in aggregated_list if r.get('ftd', 0) == 0]
    
    print(f"Новых игроков (FTD>=1): {len(new_players)}")
    print(f"Старых игроков (FTD=0): {len(old_players)}")
    print()
    
    # Метрики по новым игрокам
    new_players_count = len(new_players)
    new_total_deposits = sum(r.get('dep_cnt', 0) or 0 for r in new_players)
    new_total_deposits_sum = sum(float(r.get('dep_sum', 0) or 0) for r in new_players)
    new_total_ngr = sum(float(r.get('ngr', 0) or 0) for r in new_players)
    new_total_commissions = sum(float(r.get('cpa', 0) or 0) for r in new_players)
    new_total_ftd = sum(r.get('ftd', 0) or 0 for r in new_players)
    
    # Средний чек на игрока = Deposits sum / FTD (не на количество игроков!)
    new_avg_check = new_total_deposits_sum / new_total_ftd if new_total_ftd > 0 else 0
    new_avg_deposits = new_total_deposits / new_players_count if new_players_count > 0 else 0
    new_roi = (new_total_deposits_sum / new_total_commissions * 100) if new_total_commissions > 0 else 0
    
    # Метрики по всем игрокам
    all_players_count = len(aggregated_list)
    all_total_deposits = sum(r.get('dep_cnt', 0) or 0 for r in aggregated_list)
    all_total_deposits_sum = sum(float(r.get('dep_sum', 0) or 0) for r in aggregated_list)
    all_total_ngr = sum(float(r.get('ngr', 0) or 0) for r in aggregated_list)
    all_total_commissions = sum(float(r.get('cpa', 0) or 0) for r in aggregated_list)
    all_total_ftd = sum(r.get('ftd', 0) or 0 for r in aggregated_list)
    
    # Средний чек на игрока = Deposits sum / FTD для всех игроков
    all_avg_check = all_total_deposits_sum / all_total_ftd if all_total_ftd > 0 else 0
    all_avg_deposits = all_total_deposits / all_players_count if all_players_count > 0 else 0
    all_roi = (all_total_deposits_sum / all_total_commissions * 100) if all_total_commissions > 0 else 0
    
    # Вывод метрик
    print("=" * 60)
    print("📈 Метрики по новым игрокам")
    print("=" * 60)
    print(f"Кол-во новых игроков (FTD=1): {new_players_count}")
    print(f"Общая сумма депозитов: {new_total_deposits_sum:,.2f}")
    print(f"Общее количество депозитов: {int(new_total_deposits)}")
    print(f"Средний чек на игрока (Deposits sum / FTD): {new_avg_check:,.2f}")
    print(f"Среднее число депозитов на игрока: {new_avg_deposits:.2f}")
    print(f"Общий NGR (Casino): {new_total_ngr:,.2f}")
    print(f"Partner income: {new_total_commissions:,.2f}")
    print(f"ROI (Deposits / Partner income): {new_roi:.2f}%")
    print()
    
    print("=" * 60)
    print("📊 Общие метрики по всем игрокам")
    print("=" * 60)
    print(f"Кол-во игроков: {all_players_count}")
    print(f"Общая сумма депозитов: {all_total_deposits_sum:,.2f}")
    print(f"Общее количество депозитов: {int(all_total_deposits)}")
    print(f"Средний чек на игрока (Deposits sum / FTD): {all_avg_check:,.2f}")
    print(f"Среднее число депозитов на игрока: {all_avg_deposits:.2f}")
    print(f"Общий NGR (Casino): {all_total_ngr:,.2f}")
    print(f"Partner income (all): {all_total_commissions:,.2f}")
    print(f"ROI (Deposits / Partner income, all): {all_roi:.2f}%")
    print()
    
    # Дополнительная информация
    print("=" * 60)
    print("📋 ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ")
    print("=" * 60)
    print(f"Старых игроков (FTD=0): {len(old_players)}")
    old_total_deposits_sum = sum(float(r.get('dep_sum', 0) or 0) for r in old_players)
    old_total_ngr = sum(float(r.get('ngr', 0) or 0) for r in old_players)
    print(f"Сумма депозитов старых игроков: {old_total_deposits_sum:,.2f}")
    print(f"NGR старых игроков: {old_total_ngr:,.2f}")
    print()
    
    # Проверка: сумма новых + старых должна равняться общим
    print("Проверка сумм:")
    print(f"  Новые депозиты: {new_total_deposits_sum:,.2f}")
    print(f"  Старые депозиты: {old_total_deposits_sum:,.2f}")
    print(f"  Всего: {new_total_deposits_sum + old_total_deposits_sum:,.2f}")
    print(f"  Ожидалось: {all_total_deposits_sum:,.2f}")
    print(f"  Разница: {abs((new_total_deposits_sum + old_total_deposits_sum) - all_total_deposits_sum):,.2f}")
    
    # Примеры записей
    print("\n" + "=" * 60)
    print("📋 ПРИМЕРЫ ЗАПИСЕЙ")
    print("=" * 60)
    if new_players:
        print("\nНовые игроки (первые 3):")
        for i, record in enumerate(new_players[:3], 1):
            print(f"  {i}. ClickID: {record.get('clickid', 'N/A')[:20]}... | "
                  f"FTD: {record.get('ftd', 0)} | "
                  f"Депозиты: {record.get('dep_cnt', 0)} | "
                  f"Сумма: {record.get('dep_sum', 0):,.2f} | "
                  f"NGR: {record.get('ngr', 0):,.2f} | "
                  f"CPA: {record.get('cpa', 0):,.2f}")
    
    if old_players:
        print("\nСтарые игроки (первые 3):")
        for i, record in enumerate(old_players[:3], 1):
            print(f"  {i}. ClickID: {record.get('clickid', 'N/A')[:20]}... | "
                  f"FTD: {record.get('ftd', 0)} | "
                  f"Депозиты: {record.get('dep_cnt', 0)} | "
                  f"Сумма: {record.get('dep_sum', 0):,.2f} | "
                  f"NGR: {record.get('ngr', 0):,.2f} | "
                  f"CPA: {record.get('cpa', 0):,.2f}")


if __name__ == '__main__':
    check_api_data_monthly()
