"""
Скрипт для проверки данных из API без записи в БД
Показывает статистику по данным, которые возвращает API
"""
from datetime import datetime, timedelta
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_api_data():
    """Проверяет данные из API и показывает статистику"""
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
    print(f"📊 ДАННЫЕ ИЗ API ЗА {month_start.strftime('%B %Y')}")
    print("=" * 60)
    print(f"Период: {month_start} - {today}")
    print(f"URL: {account['url']}")
    print()
    
    # Получаем данные из API
    report = api.fetch_report(
        from_date=str(month_start),
        to_date=str(today),
        columns=['first_deposits_count', 'deposits_count', 'deposits_sum', 'partner_income', 'ngr'],
        group_by=['day', 'dynamic_tag_visit_id']
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
    
    # Фильтруем записи с FTD >= 1
    ftd_records = [r for r in parsed_data if r.get('ftd', 0) >= 1]
    
    if not ftd_records:
        print("⚠️ Нет записей с FTD >= 1")
        return
    
    # Подсчитываем метрики
    unique_players = len(set(r.get('clickid') for r in ftd_records if r.get('clickid')))
    total_ftd = sum(r.get('ftd', 0) or 0 for r in ftd_records)
    total_deposits = sum(r.get('dep_cnt', 0) or 0 for r in ftd_records)
    total_deposits_sum = sum(float(r.get('dep_sum', 0) or 0) for r in ftd_records)
    total_ngr = sum(float(r.get('ngr', 0) or 0) for r in ftd_records)
    total_commissions = sum(float(r.get('cpa', 0) or 0) for r in ftd_records)
    
    # Расчет средних значений
    avg_check_per_player = total_deposits_sum / total_ftd if total_ftd > 0 else 0
    avg_deposits_per_player = total_deposits / unique_players if unique_players > 0 else 0
    roi = (total_deposits_sum / total_commissions * 100) if total_commissions > 0 else 0
    
    # Вывод метрик
    print("📈 Метрики по новым игрокам (FTD=1) в этом месяце")
    print(f"Уникальных игроков: {unique_players:,}")
    print(f"FTD: {int(total_ftd):,}")
    print(f"Кол-во депозитов: {int(total_deposits):,}")
    print(f"Сумма депозитов: {total_deposits_sum:,.2f}")
    print(f"Средний чек на игрока: {avg_check_per_player:,.2f}")
    print(f"Среднее число депозитов на игрока: {avg_deposits_per_player:.2f}")
    print()
    print("💰 Финансы")
    print(f"NGR: {total_ngr:,.2f}")
    print(f"Commissions (Partner income): {total_commissions:,.2f}")
    print(f"ROI (Deposits / Commissions): {roi:.2f}%")
    print()
    
    # Дополнительная статистика
    print("=" * 60)
    print("📋 ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА")
    print("=" * 60)
    
    # Статистика по дням
    daily_stats = {}
    for record in ftd_records:
        date_key = record.get('period_date')
        if date_key:
            if date_key not in daily_stats:
                daily_stats[date_key] = {
                    'players': set(),
                    'ftd': 0,
                    'deposits': 0,
                    'deposits_sum': 0,
                    'ngr': 0,
                    'commissions': 0
                }
            clickid = record.get('clickid')
            if clickid:
                daily_stats[date_key]['players'].add(clickid)
            daily_stats[date_key]['ftd'] += record.get('ftd', 0) or 0
            daily_stats[date_key]['deposits'] += record.get('dep_cnt', 0) or 0
            daily_stats[date_key]['deposits_sum'] += float(record.get('dep_sum', 0) or 0)
            daily_stats[date_key]['ngr'] += float(record.get('ngr', 0) or 0)
            daily_stats[date_key]['commissions'] += float(record.get('cpa', 0) or 0)
    
    if daily_stats:
        print("\nСтатистика по дням (последние 10 дней с FTD):")
        print(f"{'Дата':<12} {'Игроков':<10} {'FTD':<8} {'Депозиты':<12} {'Сумма':<15} {'NGR':<15} {'Комиссии':<12}")
        print("-" * 85)
        sorted_days = sorted(daily_stats.items(), reverse=True)[:10]
        for date_key, stats in sorted_days:
            print(f"{date_key} {len(stats['players']):<10} {int(stats['ftd']):<8} "
                  f"{int(stats['deposits']):<12} {stats['deposits_sum']:<15,.2f} "
                  f"{stats['ngr']:<15,.2f} {stats['commissions']:<12,.2f}")
    
    # Общая статистика (все записи, не только FTD)
    all_unique_players = len(set(r.get('clickid') for r in parsed_data if r.get('clickid')))
    all_total_ftd = sum(r.get('ftd', 0) or 0 for r in parsed_data)
    all_total_deposits = sum(r.get('dep_cnt', 0) or 0 for r in parsed_data)
    all_total_deposits_sum = sum(float(r.get('dep_sum', 0) or 0) for r in parsed_data)
    all_total_ngr = sum(float(r.get('ngr', 0) or 0) for r in parsed_data)
    all_total_commissions = sum(float(r.get('cpa', 0) or 0) for r in parsed_data)
    
    print("\n" + "=" * 60)
    print("📊 ОБЩАЯ СТАТИСТИКА ЗА МЕСЯЦ (все игроки из API)")
    print("=" * 60)
    print(f"Всего уникальных игроков: {all_unique_players:,}")
    print(f"Всего FTD: {int(all_total_ftd):,}")
    print(f"Всего депозитов: {int(all_total_deposits):,}")
    print(f"Сумма всех депозитов: {all_total_deposits_sum:,.2f}")
    print(f"NGR: {all_total_ngr:,.2f}")
    print(f"Commissions: {all_total_commissions:,.2f}")
    
    if all_unique_players > 0:
        ftd_rate = (unique_players / all_unique_players * 100) if all_unique_players > 0 else 0
        print(f"\nКонверсия в FTD: {ftd_rate:.2f}% ({unique_players} из {all_unique_players})")
    
    # Примеры записей с NGR
    ngr_records = [r for r in parsed_data if r.get('ngr', 0) > 0]
    if ngr_records:
        print("\n" + "=" * 60)
        print(f"📋 ПРИМЕРЫ ЗАПИСЕЙ С NGR > 0 (показано {min(5, len(ngr_records))} из {len(ngr_records)})")
        print("=" * 60)
        for i, record in enumerate(ngr_records[:5], 1):
            print(f"\nЗапись {i}:")
            print(f"  Дата: {record.get('period_date')}")
            print(f"  ClickID: {record.get('clickid', 'N/A')[:20]}...")
            print(f"  FTD: {record.get('ftd', 0)}")
            print(f"  Депозиты: {record.get('dep_cnt', 0)}")
            print(f"  Сумма депозитов: {record.get('dep_sum', 0):,.2f}")
            print(f"  NGR: {record.get('ngr', 0):,.2f}")
            print(f"  CPA: {record.get('cpa', 0):,.2f}")


if __name__ == '__main__':
    from datetime import date
    check_api_data()
