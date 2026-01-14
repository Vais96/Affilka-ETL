"""
Скрипт для проверки метрик по новым игрокам (FTD=1) за текущий месяц
"""
from database import Database
from datetime import datetime, date
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_metrics():
    """Проверяет метрики по новым игрокам за текущий месяц"""
    db = Database()
    
    if not db.connect():
        print("❌ Не удалось подключиться к БД")
        return
    
    try:
        # Определяем текущий месяц
        today = date.today()
        month_start = date(today.year, today.month, 1)
        
        print("=" * 60)
        print(f"📊 МЕТРИКИ ПО НОВЫМ ИГРОКАМ (FTD=1) ЗА {month_start.strftime('%B %Y')}")
        print("=" * 60)
        print(f"Период: {month_start} - {today}")
        print()
        
        # Запрос для получения данных по игрокам с FTD=1
        # В fact_click_month данные уже агрегированы по (period_date, clickid)
        # Нужно агрегировать по clickid, чтобы получить данные по игрокам
        # FTD=1 означает, что у этого clickid был первый депозит в этом месяце
        query = """
            SELECT 
                clickid,
                MAX(ftd) as ftd,  -- FTD - это флаг, берем максимальное значение
                SUM(dep_cnt) as dep_cnt,
                SUM(dep_sum) as dep_sum,
                SUM(ngr) as ngr,
                SUM(cpa) as cpa
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
            GROUP BY clickid
            HAVING MAX(ftd) >= 1
        """
        
        db.cursor.execute(query, (month_start, today))
        results = db.cursor.fetchall()
        
        if not results:
            print("❌ Нет данных за указанный период")
            return
        
        # Агрегируем данные по игрокам
        unique_players = len(results)
        total_ftd = sum(float(r['ftd'] or 0) for r in results)
        total_deposits = sum(float(r['dep_cnt'] or 0) for r in results)
        total_deposits_sum = sum(float(r['dep_sum'] or 0) for r in results)
        total_ngr = sum(float(r['ngr'] or 0) for r in results)
        total_commissions = sum(float(r['cpa'] or 0) for r in results)
        
        # Расчет средних значений
        # Средний чек на игрока = Deposits sum / FTD (не на уникальных игроков!)
        avg_check_per_player = total_deposits_sum / total_ftd if total_ftd > 0 else 0
        avg_deposits_per_player = total_deposits / unique_players if unique_players > 0 else 0
        roi = (total_deposits_sum / total_commissions * 100) if total_commissions > 0 else 0
        
        # Вывод метрик
        print("📈 Метрики по новым игрокам")
        print("=" * 60)
        print(f"Кол-во новых игроков (FTD=1): {unique_players}")
        print(f"Общая сумма депозитов: {total_deposits_sum:,.2f}")
        print(f"Общее количество депозитов: {int(total_deposits)}")
        print(f"Средний чек на игрока (Deposits sum / FTD): {avg_check_per_player:,.2f}")
        print(f"Среднее число депозитов на игрока: {avg_deposits_per_player:.2f}")
        print(f"Общий NGR (Casino): {total_ngr:,.2f}")
        print(f"Partner income: {total_commissions:,.2f}")
        print(f"ROI (Deposits / Partner income): {roi:.2f}%")
        print()
        
        # Дополнительная статистика
        print("=" * 60)
        print("📋 ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА")
        print("=" * 60)
        
        # Статистика по дням
        daily_query = """
            SELECT 
                period_date,
                COUNT(DISTINCT clickid) as players,
                SUM(ftd) as ftd,
                SUM(dep_cnt) as deposits,
                SUM(dep_sum) as deposits_sum,
                SUM(cpa) as commissions
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
                AND ftd >= 1
            GROUP BY period_date
            ORDER BY period_date DESC
            LIMIT 10
        """
        
        db.cursor.execute(daily_query, (month_start, today))
        daily_results = db.cursor.fetchall()
        
        if daily_results:
            print("\nПоследние 10 дней с FTD:")
            print(f"{'Дата':<12} {'Игроков':<10} {'FTD':<8} {'Депозиты':<12} {'Сумма':<15} {'Комиссии':<12}")
            print("-" * 70)
            for row in daily_results:
                print(f"{row['period_date']} {row['players']:<10} {int(row['ftd']):<8} "
                      f"{int(row['deposits']):<12} {float(row['deposits_sum']):<15,.2f} "
                      f"{float(row['commissions']):<12,.2f}")
        
        # Общая статистика за месяц (все игроки, не только FTD=1)
        all_query = """
            SELECT 
                clickid,
                MAX(ftd) as ftd,
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
        
        db.cursor.execute(all_query, (month_start, today))
        all_results = db.cursor.fetchall()
        
        if all_results:
            all_players_count = len(all_results)
            all_total_ftd = sum(float(r['ftd'] or 0) for r in all_results)
            all_total_deposits = sum(float(r['dep_cnt'] or 0) for r in all_results)
            all_total_deposits_sum = sum(float(r['dep_sum'] or 0) for r in all_results)
            all_total_ngr = sum(float(r['ngr'] or 0) for r in all_results)
            all_total_commissions = sum(float(r['cpa'] or 0) for r in all_results)
            all_avg_check = all_total_deposits_sum / all_total_ftd if all_total_ftd > 0 else 0
            all_avg_deposits = all_total_deposits / all_players_count if all_players_count > 0 else 0
            all_roi = (all_total_deposits_sum / all_total_commissions * 100) if all_total_commissions > 0 else 0
            
            print("\n" + "=" * 60)
            print("📊 ОБЩАЯ СТАТИСТИКА ЗА МЕСЯЦ (все игроки)")
            print("=" * 60)
            print(f"Кол-во игроков: {all_players_count}")
            print(f"Всего FTD: {int(all_total_ftd):,}")
            print(f"Общая сумма депозитов: {all_total_deposits_sum:,.2f}")
            print(f"Общее количество депозитов: {int(all_total_deposits)}")
            print(f"Средний чек на игрока (Deposits sum / FTD): {all_avg_check:,.2f}")
            print(f"Среднее число депозитов на игрока: {all_avg_deposits:.2f}")
            print(f"Общий NGR (Casino): {all_total_ngr:,.2f}")
            print(f"Partner income (all): {all_total_commissions:,.2f}")
            print(f"ROI (Deposits / Partner income, all): {all_roi:.2f}%")
            
            if all_players_count > 0:
                ftd_rate = (unique_players / all_players_count * 100) if all_players_count > 0 else 0
                print(f"\nКонверсия в FTD: {ftd_rate:.2f}% ({unique_players} из {all_players_count})")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке метрик: {e}", exc_info=True)
    finally:
        db.disconnect()


if __name__ == '__main__':
    check_metrics()
