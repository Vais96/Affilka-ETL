"""
Скрипт для проверки офферов по которым пришли игроки
Показывает статистику по офферам из БД
"""
from datetime import datetime, date
from database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_offers():
    """Проверяет офферы по которым пришли игроки"""
    db = Database()
    
    if not db.connect():
        print("❌ Не удалось подключиться к БД")
        return
    
    try:
        # Определяем текущий месяц
        today = datetime.now().date()
        month_start = date(today.year, today.month, 1)
        
        print("=" * 60)
        print(f"📊 ОФФЕРЫ ПО КОТОРЫМ ПРИШЛИ ИГРОКИ ЗА {month_start.strftime('%B %Y')}")
        print("=" * 60)
        print(f"Период: {month_start} - {today}")
        print()
        
        # Проверяем, есть ли поле offer_id в таблице
        db.cursor.execute("DESCRIBE fact_click_month")
        columns = [col['Field'] for col in db.cursor.fetchall()]
        
        if 'offer_id' not in columns:
            print("⚠️ Поле offer_id не найдено в таблице fact_click_month")
            print(f"Доступные поля: {', '.join(columns)}")
            return
        
        # Статистика по офферам для новых игроков (FTD=1)
        print("📈 ОФФЕРЫ ДЛЯ НОВЫХ ИГРОКОВ (FTD=1)")
        print("=" * 60)
        
        query_new = """
            SELECT 
                offer_id,
                COUNT(DISTINCT clickid) as players,
                SUM(ftd) as total_ftd,
                SUM(dep_cnt) as total_deposits,
                SUM(dep_sum) as total_deposits_sum,
                SUM(ngr) as total_ngr,
                SUM(cpa) as total_cpa
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
            GROUP BY clickid
            HAVING MAX(ftd) >= 1
        """
        
        # Сначала получаем данные по игрокам, потом группируем по офферам
        query_players = """
            SELECT 
                clickid,
                offer_id,
                MAX(ftd) as ftd,
                SUM(dep_cnt) as dep_cnt,
                SUM(dep_sum) as dep_sum,
                SUM(ngr) as ngr,
                SUM(cpa) as cpa
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
            GROUP BY clickid, offer_id
            HAVING MAX(ftd) >= 1
        """
        
        db.cursor.execute(query_players, (month_start, today))
        players_data = db.cursor.fetchall()
        
        # Группируем по офферам
        offers_stats = {}
        for player in players_data:
            offer_id = player['offer_id']
            if offer_id is None:
                offer_id = 'NULL'
            
            if offer_id not in offers_stats:
                offers_stats[offer_id] = {
                    'players': set(),
                    'ftd': 0,
                    'dep_cnt': 0,
                    'dep_sum': 0.0,
                    'ngr': 0.0,
                    'cpa': 0.0
                }
            
            clickid = player['clickid']
            offers_stats[offer_id]['players'].add(clickid)
            offers_stats[offer_id]['ftd'] += float(player['ftd'] or 0)
            offers_stats[offer_id]['dep_cnt'] += float(player['dep_cnt'] or 0)
            offers_stats[offer_id]['dep_sum'] += float(player['dep_sum'] or 0)
            offers_stats[offer_id]['ngr'] += float(player['ngr'] or 0)
            offers_stats[offer_id]['cpa'] += float(player['cpa'] or 0)
        
        if offers_stats:
            print(f"{'Offer ID':<12} {'Игроков':<10} {'FTD':<8} {'Депозиты':<12} {'Сумма':<15} {'NGR':<15} {'CPA':<12}")
            print("-" * 85)
            
            # Сортируем по количеству игроков
            sorted_offers = sorted(offers_stats.items(), key=lambda x: len(x[1]['players']), reverse=True)
            
            for offer_id, stats in sorted_offers:
                players_count = len(stats['players'])
                print(f"{str(offer_id):<12} {players_count:<10} {int(stats['ftd']):<8} "
                      f"{int(stats['dep_cnt']):<12} {stats['dep_sum']:<15,.2f} "
                      f"{stats['ngr']:<15,.2f} {stats['cpa']:<12,.2f}")
            
            # Итого
            total_players = sum(len(s['players']) for s in offers_stats.values())
            total_ftd = sum(s['ftd'] for s in offers_stats.values())
            total_deposits = sum(s['dep_cnt'] for s in offers_stats.values())
            total_dep_sum = sum(s['dep_sum'] for s in offers_stats.values())
            total_ngr = sum(s['ngr'] for s in offers_stats.values())
            total_cpa = sum(s['cpa'] for s in offers_stats.values())
            
            print("-" * 85)
            print(f"{'ИТОГО':<12} {total_players:<10} {int(total_ftd):<8} "
                  f"{int(total_deposits):<12} {total_dep_sum:<15,.2f} "
                  f"{total_ngr:<15,.2f} {total_cpa:<12,.2f}")
        else:
            print("Нет данных по офферам")
        
        # Общая статистика по офферам (все игроки)
        print("\n" + "=" * 60)
        print("📊 ОФФЕРЫ ДЛЯ ВСЕХ ИГРОКОВ")
        print("=" * 60)
        
        query_all = """
            SELECT 
                clickid,
                offer_id,
                MAX(ftd) as ftd,
                SUM(dep_cnt) as dep_cnt,
                SUM(dep_sum) as dep_sum,
                SUM(ngr) as ngr,
                SUM(cpa) as cpa
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
            GROUP BY clickid, offer_id
        """
        
        db.cursor.execute(query_all, (month_start, today))
        all_players_data = db.cursor.fetchall()
        
        all_offers_stats = {}
        for player in all_players_data:
            offer_id = player['offer_id']
            if offer_id is None:
                offer_id = 'NULL'
            
            if offer_id not in all_offers_stats:
                all_offers_stats[offer_id] = {
                    'players': set(),
                    'ftd': 0,
                    'dep_cnt': 0,
                    'dep_sum': 0.0,
                    'ngr': 0.0,
                    'cpa': 0.0
                }
            
            clickid = player['clickid']
            all_offers_stats[offer_id]['players'].add(clickid)
            all_offers_stats[offer_id]['ftd'] += float(player['ftd'] or 0)
            all_offers_stats[offer_id]['dep_cnt'] += float(player['dep_cnt'] or 0)
            all_offers_stats[offer_id]['dep_sum'] += float(player['dep_sum'] or 0)
            all_offers_stats[offer_id]['ngr'] += float(player['ngr'] or 0)
            all_offers_stats[offer_id]['cpa'] += float(player['cpa'] or 0)
        
        if all_offers_stats:
            print(f"{'Offer ID':<12} {'Игроков':<10} {'FTD':<8} {'Депозиты':<12} {'Сумма':<15} {'NGR':<15} {'CPA':<12}")
            print("-" * 85)
            
            sorted_all_offers = sorted(all_offers_stats.items(), key=lambda x: len(x[1]['players']), reverse=True)
            
            for offer_id, stats in sorted_all_offers:
                players_count = len(stats['players'])
                print(f"{str(offer_id):<12} {players_count:<10} {int(stats['ftd']):<8} "
                      f"{int(stats['dep_cnt']):<12} {stats['dep_sum']:<15,.2f} "
                      f"{stats['ngr']:<15,.2f} {stats['cpa']:<12,.2f}")
            
            # Итого
            all_total_players = sum(len(s['players']) for s in all_offers_stats.values())
            all_total_ftd = sum(s['ftd'] for s in all_offers_stats.values())
            all_total_deposits = sum(s['dep_cnt'] for s in all_offers_stats.values())
            all_total_dep_sum = sum(s['dep_sum'] for s in all_offers_stats.values())
            all_total_ngr = sum(s['ngr'] for s in all_offers_stats.values())
            all_total_cpa = sum(s['cpa'] for s in all_offers_stats.values())
            
            print("-" * 85)
            print(f"{'ИТОГО':<12} {all_total_players:<10} {int(all_total_ftd):<8} "
                  f"{int(all_total_deposits):<12} {all_total_dep_sum:<15,.2f} "
                  f"{all_total_ngr:<15,.2f} {all_total_cpa:<12,.2f}")
        
        # Проверка: сколько игроков без offer_id
        null_offer_players = len([p for p in all_players_data if p['offer_id'] is None])
        if null_offer_players > 0:
            print(f"\n⚠️ Игроков без offer_id: {null_offer_players}")
        
        # Примеры записей с разными офферами
        print("\n" + "=" * 60)
        print("📋 ПРИМЕРЫ ЗАПИСЕЙ ПО ОФФЕРАМ")
        print("=" * 60)
        
        # Берем по одному игроку из каждого оффера
        examples_by_offer = {}
        for player in all_players_data[:20]:  # Первые 20 для примера
            offer_id = player['offer_id'] or 'NULL'
            if offer_id not in examples_by_offer:
                examples_by_offer[offer_id] = player
        
        for offer_id, player in list(examples_by_offer.items())[:5]:
            print(f"\nОффер {offer_id}:")
            print(f"  ClickID: {player['clickid'][:20]}...")
            print(f"  FTD: {player['ftd']}")
            print(f"  Депозиты: {player['dep_cnt']}")
            print(f"  Сумма: {player['dep_sum']:,.2f}")
            print(f"  NGR: {player['ngr']:,.2f}")
            print(f"  CPA: {player['cpa']:,.2f}")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке офферов: {e}", exc_info=True)
    finally:
        db.disconnect()


if __name__ == '__main__':
    check_offers()
