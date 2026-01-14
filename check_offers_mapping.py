"""
Скрипт для проверки маппинга офферов через clickid
Показывает статистику по офферам с использованием vw_clickid_buyer_offer
"""
from datetime import datetime, date
from database import Database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_offers_mapping():
    """Проверяет маппинг офферов через clickid"""
    db = Database()
    
    if not db.connect():
        print("❌ Не удалось подключиться к БД")
        return
    
    try:
        # Определяем текущий месяц
        today = datetime.now().date()
        month_start = date(today.year, today.month, 1)
        
        print("=" * 60)
        print(f"📊 ОФФЕРЫ ПО КОТОРЫМ ПРИШЛИ ИГРОКИ (через маппинг clickid)")
        print("=" * 60)
        print(f"Период: {month_start} - {today}")
        print()
        
        # Получаем данные из fact_click_month с маппингом через vw_clickid_buyer_offer
        # Для новых игроков (FTD=1)
        query_new = """
            SELECT 
                COALESCE(v.offer_name, 'NULL') as offer_name,
                COUNT(DISTINCT f.clickid) as players,
                SUM(f.ftd) as total_ftd,
                SUM(f.dep_cnt) as total_deposits,
                SUM(f.dep_sum) as total_deposits_sum,
                SUM(f.ngr) as total_ngr,
                SUM(f.cpa) as total_cpa
            FROM fact_click_month f
            LEFT JOIN vw_clickid_buyer_offer v ON f.clickid = v.clickid
            WHERE f.source = 'affilka'
                AND f.period_date >= %s
                AND f.period_date <= %s
            GROUP BY f.clickid
            HAVING MAX(f.ftd) >= 1
        """
        
        # Сначала получаем данные по игрокам
        query_players = """
            SELECT 
                f.clickid,
                v.offer_name,
                MAX(f.ftd) as ftd,
                SUM(f.dep_cnt) as dep_cnt,
                SUM(f.dep_sum) as dep_sum,
                SUM(f.ngr) as ngr,
                SUM(f.cpa) as cpa
            FROM fact_click_month f
            LEFT JOIN vw_clickid_buyer_offer v ON f.clickid = v.clickid
            WHERE f.source = 'affilka'
                AND f.period_date >= %s
                AND f.period_date <= %s
            GROUP BY f.clickid, v.offer_name
            HAVING MAX(f.ftd) >= 1
        """
        
        db.cursor.execute(query_players, (month_start, today))
        players_data = db.cursor.fetchall()
        
        # Группируем по офферам
        offers_stats = {}
        for player in players_data:
            offer_name = player['offer_name'] or 'NULL'
            
            if offer_name not in offers_stats:
                offers_stats[offer_name] = {
                    'players': set(),
                    'ftd': 0,
                    'dep_cnt': 0,
                    'dep_sum': 0.0,
                    'ngr': 0.0,
                    'cpa': 0.0
                }
            
            clickid = player['clickid']
            offers_stats[offer_name]['players'].add(clickid)
            offers_stats[offer_name]['ftd'] += float(player['ftd'] or 0)
            offers_stats[offer_name]['dep_cnt'] += float(player['dep_cnt'] or 0)
            offers_stats[offer_name]['dep_sum'] += float(player['dep_sum'] or 0)
            offers_stats[offer_name]['ngr'] += float(player['ngr'] or 0)
            offers_stats[offer_name]['cpa'] += float(player['cpa'] or 0)
        
        print("📈 ОФФЕРЫ ДЛЯ НОВЫХ ИГРОКОВ (FTD=1)")
        print("=" * 60)
        
        if offers_stats:
            print(f"{'Offer Name':<50} {'Игроков':<10} {'FTD':<8} {'Депозиты':<12} {'Сумма':<15} {'NGR':<15} {'CPA':<12}")
            print("-" * 120)
            
            # Сортируем по количеству игроков
            sorted_offers = sorted(offers_stats.items(), key=lambda x: len(x[1]['players']), reverse=True)
            
            for offer_name, stats in sorted_offers:
                players_count = len(stats['players'])
                offer_display = offer_name[:47] + '...' if len(offer_name) > 50 else offer_name
                print(f"{offer_display:<50} {players_count:<10} {int(stats['ftd']):<8} "
                      f"{int(stats['dep_cnt']):<12} {stats['dep_sum']:<15,.2f} "
                      f"{stats['ngr']:<15,.2f} {stats['cpa']:<12,.2f}")
            
            # Итого
            total_players = sum(len(s['players']) for s in offers_stats.values())
            total_ftd = sum(s['ftd'] for s in offers_stats.values())
            total_deposits = sum(s['dep_cnt'] for s in offers_stats.values())
            total_dep_sum = sum(s['dep_sum'] for s in offers_stats.values())
            total_ngr = sum(s['ngr'] for s in offers_stats.values())
            total_cpa = sum(s['cpa'] for s in offers_stats.values())
            
            print("-" * 120)
            print(f"{'ИТОГО':<50} {total_players:<10} {int(total_ftd):<8} "
                  f"{int(total_deposits):<12} {total_dep_sum:<15,.2f} "
                  f"{total_ngr:<15,.2f} {total_cpa:<12,.2f}")
            
            # Статистика по маппингу
            mapped = sum(1 for name in offers_stats.keys() if name != 'NULL')
            unmapped = offers_stats.get('NULL', {}).get('players', set())
            print(f"\n📋 Статистика маппинга:")
            print(f"  Офферов с маппингом: {mapped}")
            print(f"  Игроков без маппинга (NULL): {len(unmapped)}")
        else:
            print("Нет данных по офферам")
        
        # Общая статистика по всем игрокам
        print("\n" + "=" * 60)
        print("📊 ОФФЕРЫ ДЛЯ ВСЕХ ИГРОКОВ")
        print("=" * 60)
        
        query_all_players = """
            SELECT 
                f.clickid,
                v.offer_name,
                MAX(f.ftd) as ftd,
                SUM(f.dep_cnt) as dep_cnt,
                SUM(f.dep_sum) as dep_sum,
                SUM(f.ngr) as ngr,
                SUM(f.cpa) as cpa
            FROM fact_click_month f
            LEFT JOIN vw_clickid_buyer_offer v ON f.clickid = v.clickid
            WHERE f.source = 'affilka'
                AND f.period_date >= %s
                AND f.period_date <= %s
            GROUP BY f.clickid, v.offer_name
        """
        
        db.cursor.execute(query_all_players, (month_start, today))
        all_players_data = db.cursor.fetchall()
        
        all_offers_stats = {}
        for player in all_players_data:
            offer_name = player['offer_name'] or 'NULL'
            
            if offer_name not in all_offers_stats:
                all_offers_stats[offer_name] = {
                    'players': set(),
                    'ftd': 0,
                    'dep_cnt': 0,
                    'dep_sum': 0.0,
                    'ngr': 0.0,
                    'cpa': 0.0
                }
            
            clickid = player['clickid']
            all_offers_stats[offer_name]['players'].add(clickid)
            all_offers_stats[offer_name]['ftd'] += float(player['ftd'] or 0)
            all_offers_stats[offer_name]['dep_cnt'] += float(player['dep_cnt'] or 0)
            all_offers_stats[offer_name]['dep_sum'] += float(player['dep_sum'] or 0)
            all_offers_stats[offer_name]['ngr'] += float(player['ngr'] or 0)
            all_offers_stats[offer_name]['cpa'] += float(player['cpa'] or 0)
        
        if all_offers_stats:
            print(f"{'Offer Name':<50} {'Игроков':<10} {'FTD':<8} {'Депозиты':<12} {'Сумма':<15} {'NGR':<15} {'CPA':<12}")
            print("-" * 120)
            
            sorted_all_offers = sorted(all_offers_stats.items(), key=lambda x: len(x[1]['players']), reverse=True)
            
            for offer_name, stats in sorted_all_offers:
                players_count = len(stats['players'])
                offer_display = offer_name[:47] + '...' if len(offer_name) > 50 else offer_name
                print(f"{offer_display:<50} {players_count:<10} {int(stats['ftd']):<8} "
                      f"{int(stats['dep_cnt']):<12} {stats['dep_sum']:<15,.2f} "
                      f"{stats['ngr']:<15,.2f} {stats['cpa']:<12,.2f}")
            
            # Итого
            all_total_players = sum(len(s['players']) for s in all_offers_stats.values())
            all_total_ftd = sum(s['ftd'] for s in all_offers_stats.values())
            all_total_deposits = sum(s['dep_cnt'] for s in all_offers_stats.values())
            all_total_dep_sum = sum(s['dep_sum'] for s in all_offers_stats.values())
            all_total_ngr = sum(s['ngr'] for s in all_offers_stats.values())
            all_total_cpa = sum(s['cpa'] for s in all_offers_stats.values())
            
            print("-" * 120)
            print(f"{'ИТОГО':<50} {all_total_players:<10} {int(all_total_ftd):<8} "
                  f"{int(all_total_deposits):<12} {all_total_dep_sum:<15,.2f} "
                  f"{all_total_ngr:<15,.2f} {all_total_cpa:<12,.2f}")
            
            # Статистика по маппингу
            all_mapped = sum(1 for name in all_offers_stats.keys() if name != 'NULL')
            all_unmapped = all_offers_stats.get('NULL', {}).get('players', set())
            print(f"\n📋 Статистика маппинга (все игроки):")
            print(f"  Офферов с маппингом: {all_mapped}")
            print(f"  Игроков без маппинга (NULL): {len(all_unmapped)}")
            if all_total_players > 0:
                mapping_rate = ((all_total_players - len(all_unmapped)) / all_total_players * 100) if all_total_players > 0 else 0
                print(f"  Процент маппинга: {mapping_rate:.2f}%")
        
        # Примеры записей с маппингом
        print("\n" + "=" * 60)
        print("📋 ПРИМЕРЫ ЗАПИСЕЙ С МАППИНГОМ")
        print("=" * 60)
        
        mapped_examples = [p for p in all_players_data if p['offer_name']]
        if mapped_examples:
            print(f"\nПримеры игроков с маппингом (первые 5):")
            for i, player in enumerate(mapped_examples[:5], 1):
                print(f"\n{i}. ClickID: {player['clickid'][:20]}...")
                print(f"   Offer: {player['offer_name']}")
                print(f"   FTD: {player['ftd']}, Депозиты: {player['dep_cnt']}, Сумма: {player['dep_sum']:,.2f}")
        
        unmapped_examples = [p for p in all_players_data if not p['offer_name']]
        if unmapped_examples:
            print(f"\nПримеры игроков БЕЗ маппинга (первые 5):")
            for i, player in enumerate(unmapped_examples[:5], 1):
                print(f"\n{i}. ClickID: {player['clickid'][:20]}...")
                print(f"   FTD: {player['ftd']}, Депозиты: {player['dep_cnt']}, Сумма: {player['dep_sum']:,.2f}")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке офферов: {e}", exc_info=True)
    finally:
        db.disconnect()


if __name__ == '__main__':
    check_offers_mapping()
