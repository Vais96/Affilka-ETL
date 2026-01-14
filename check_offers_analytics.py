"""
Аналитика по офферам из Affilka за январь 2026
"""
from database import Database
from datetime import date

def check_offers_analytics():
    db = Database()
    if not db.connect():
        print("❌ Не удалось подключиться к БД")
        return
    
    month_start = date(2026, 1, 1)
    month_end = date(2026, 1, 31)
    dec_start = date(2025, 12, 1)
    dec_end = date(2025, 12, 31)
    
    # Получаем статистику по офферам
    query = '''
        WITH january_data AS (
            SELECT 
                f.clickid,
                f.offer_id,
                f.buyer_id,
                MAX(f.ftd) as max_ftd,
                SUM(f.dep_cnt) as dep_cnt,
                SUM(f.dep_sum) as dep_sum,
                SUM(f.ngr) as ngr,
                SUM(f.cpa) as cpa
            FROM fact_click_month f
            WHERE f.source = 'affilka'
                AND f.period_date >= %s
                AND f.period_date <= %s
            GROUP BY f.clickid, f.offer_id, f.buyer_id
        ),
        december_players AS (
            SELECT DISTINCT clickid
            FROM fact_click_month
            WHERE source = 'affilka'
                AND period_date >= %s
                AND period_date <= %s
        ),
        players_classified AS (
            SELECT 
                jd.*,
                CASE 
                    WHEN jd.max_ftd >= 1 THEN 'new'
                    WHEN dp.clickid IS NOT NULL AND (jd.dep_cnt > 0 OR jd.ngr > 0) THEN 'old'
                    ELSE 'registration_only'
                END as player_type
            FROM january_data jd
            LEFT JOIN december_players dp ON jd.clickid = dp.clickid
        ),
        offer_stats AS (
            SELECT 
                pc.offer_id,
                COUNT(DISTINCT CASE WHEN pc.player_type = 'new' THEN pc.clickid END) as new_players,
                COUNT(DISTINCT CASE WHEN pc.player_type = 'old' THEN pc.clickid END) as old_players,
                COUNT(DISTINCT CASE WHEN pc.player_type = 'registration_only' THEN pc.clickid END) as reg_only_players,
                COUNT(DISTINCT pc.clickid) as total_players,
                SUM(CASE WHEN pc.player_type = 'new' THEN pc.max_ftd ELSE 0 END) as new_ftd,
                SUM(CASE WHEN pc.player_type = 'old' THEN 0 ELSE pc.max_ftd END) as other_ftd,
                SUM(pc.max_ftd) as total_ftd,
                SUM(CASE WHEN pc.player_type = 'new' THEN pc.dep_cnt ELSE 0 END) as new_dep_cnt,
                SUM(CASE WHEN pc.player_type = 'old' THEN pc.dep_cnt ELSE 0 END) as old_dep_cnt,
                SUM(pc.dep_cnt) as total_dep_cnt,
                SUM(CASE WHEN pc.player_type = 'new' THEN pc.dep_sum ELSE 0 END) as new_dep_sum,
                SUM(CASE WHEN pc.player_type = 'old' THEN pc.dep_sum ELSE 0 END) as old_dep_sum,
                SUM(pc.dep_sum) as total_dep_sum,
                SUM(CASE WHEN pc.player_type = 'new' THEN pc.ngr ELSE 0 END) as new_ngr,
                SUM(CASE WHEN pc.player_type = 'old' THEN pc.ngr ELSE 0 END) as old_ngr,
                SUM(pc.ngr) as total_ngr,
                SUM(CASE WHEN pc.player_type = 'new' THEN pc.cpa ELSE 0 END) as new_cpa,
                SUM(CASE WHEN pc.player_type = 'old' THEN pc.cpa ELSE 0 END) as old_cpa,
                SUM(pc.cpa) as total_cpa,
                COUNT(DISTINCT CASE WHEN pc.offer_id IS NOT NULL THEN pc.clickid END) as mapped_players,
                COUNT(DISTINCT CASE WHEN pc.offer_id IS NULL THEN pc.clickid END) as unmapped_players
            FROM players_classified pc
            GROUP BY pc.offer_id
        )
        SELECT 
            os.*,
            COALESCE(do.offer_name, 'NULL') as offer_name
        FROM offer_stats os
        LEFT JOIN dim_offer do ON os.offer_id = do.offer_id
        WHERE os.offer_id IS NOT NULL  -- Показываем только офферы с маппингом
        ORDER BY os.total_ftd DESC, os.total_dep_sum DESC
    '''
    
    db.cursor.execute(query, (month_start, month_end, dec_start, dec_end))
    results = db.cursor.fetchall()
    
    print('=' * 120)
    print('📊 АНАЛИТИКА ПО ОФФЕРАМ ИЗ AFFILKA за ЯНВАРЬ 2026')
    print('=' * 120)
    print()
    
    # Заголовок
    print(f"{'Offer ID':<12} {'Оффер':<50} {'Новые':<8} {'Старые':<8} {'Всего':<8} {'FTD':<8} {'Деп.':<10} {'Сумма':<15} {'NGR':<15} {'CPA':<12}")
    print('-' * 120)
    
    total_new_players = 0
    total_old_players = 0
    total_players = 0
    total_ftd = 0
    total_dep_cnt = 0
    total_dep_sum = 0.0
    total_ngr = 0.0
    total_cpa = 0.0
    
    for r in results:
        offer_id = str(r['offer_id']) if r['offer_id'] else 'NULL'
        offer_name = (r['offer_name'] or 'NULL')[:48]
        new_players = r['new_players']
        old_players = r['old_players']
        total_pl = r['total_players']
        ftd = int(r['total_ftd'] or 0)
        dep_cnt = int(r['total_dep_cnt'] or 0)
        dep_sum = float(r['total_dep_sum'] or 0)
        ngr = float(r['total_ngr'] or 0)
        cpa = float(r['total_cpa'] or 0)
        
        print(f"{offer_id:<12} {offer_name:<50} {new_players:<8} {old_players:<8} {total_pl:<8} {ftd:<8} {dep_cnt:<10} {dep_sum:<15,.2f} {ngr:<15,.2f} {cpa:<12,.2f}")
        
        total_new_players += new_players
        total_old_players += old_players
        total_players += total_pl
        total_ftd += ftd
        total_dep_cnt += dep_cnt
        total_dep_sum += dep_sum
        total_ngr += ngr
        total_cpa += cpa
    
    print('-' * 120)
    print(f"{'ИТОГО':<12} {'':<50} {total_new_players:<8} {total_old_players:<8} {total_players:<8} {total_ftd:<8} {total_dep_cnt:<10} {total_dep_sum:<15,.2f} {total_ngr:<15,.2f} {total_cpa:<12,.2f}")
    
    # Детальная статистика по каждому офферу
    print('\n' + '=' * 120)
    print('📋 ДЕТАЛЬНАЯ СТАТИСТИКА ПО ОФФЕРАМ')
    print('=' * 120)
    
    for r in results:
        offer_id = str(r['offer_id']) if r['offer_id'] else 'NULL'
        offer_name = r['offer_name'] or 'NULL'
        new_players = r['new_players']
        old_players = r['old_players']
        reg_only = r['reg_only_players']
        total_pl = r['total_players']
        new_ftd = int(r['new_ftd'] or 0)
        other_ftd = int(r['other_ftd'] or 0)
        total_ftd = int(r['total_ftd'] or 0)
        new_dep_cnt = int(r['new_dep_cnt'] or 0)
        old_dep_cnt = int(r['old_dep_cnt'] or 0)
        total_dep_cnt = int(r['total_dep_cnt'] or 0)
        new_dep_sum = float(r['new_dep_sum'] or 0)
        old_dep_sum = float(r['old_dep_sum'] or 0)
        total_dep_sum = float(r['total_dep_sum'] or 0)
        new_ngr = float(r['new_ngr'] or 0)
        old_ngr = float(r['old_ngr'] or 0)
        total_ngr = float(r['total_ngr'] or 0)
        new_cpa = float(r['new_cpa'] or 0)
        old_cpa = float(r['old_cpa'] or 0)
        total_cpa = float(r['total_cpa'] or 0)
        mapped = r['mapped_players']
        unmapped = r['unmapped_players']
        
        if total_pl > 0:
            print(f'\n📌 Оффер {offer_id}: {offer_name}')
            print(f'   Игроки: Всего={total_pl} (Новые={new_players}, Старые={old_players}, Только регистрация={reg_only})')
            print(f'   FTD: Всего={total_ftd} (Новые={new_ftd}, Остальные={other_ftd})')
            print(f'   Депозиты: Всего={total_dep_cnt} (Новые={new_dep_cnt}, Старые={old_dep_cnt})')
            print(f'   Сумма депозитов: {total_dep_sum:,.2f} (Новые={new_dep_sum:,.2f}, Старые={old_dep_sum:,.2f})')
            print(f'   NGR: {total_ngr:,.2f} (Новые={new_ngr:,.2f}, Старые={old_ngr:,.2f})')
            print(f'   CPA: {total_cpa:,.2f} (Новые={new_cpa:,.2f}, Старые={old_cpa:,.2f})')
            if total_pl > 0:
                print(f'   Маппинг: {mapped}/{total_pl} ({mapped/total_pl*100:.2f}%)')
            if total_ftd > 0:
                avg_check = total_dep_sum / total_ftd
                print(f'   Средний чек на игрока: {avg_check:,.2f}')
    
    db.disconnect()

if __name__ == '__main__':
    check_offers_analytics()
