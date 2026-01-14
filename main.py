"""
Главный скрипт для запуска ETL процесса
Можно запускать вручную или через cron на Railway
"""
import sys
import argparse
from datetime import datetime, timedelta, date
from etl_process import process_all_accounts
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Главная функция для запуска ETL"""
    parser = argparse.ArgumentParser(description='ETL процесс для загрузки данных из Affilka API')
    
    parser.add_argument(
        '--from-date',
        type=str,
        help='Начальная дата в формате YYYY-MM-DD (по умолчанию: вчера)',
        default=None
    )
    
    parser.add_argument(
        '--to-date',
        type=str,
        help='Конечная дата в формате YYYY-MM-DD (по умолчанию: сегодня)',
        default=None
    )
    
    parser.add_argument(
        '--days-back',
        type=int,
        help='Количество дней назад для загрузки (по умолчанию: загружается весь месяц с 1 числа)',
        default=None
    )
    
    args = parser.parse_args()
    
    # Определяем диапазон дат
    today = datetime.now().date()
    
    if args.from_date and args.to_date:
        from_date = args.from_date
        to_date = args.to_date
    else:
        # По умолчанию загружаем весь месяц с 1 числа по текущее число
        month_start = date(today.year, today.month, 1)
        from_date = month_start.strftime('%Y-%m-%d')
        to_date = today.strftime('%Y-%m-%d')
    
    logger.info(f"Запуск ETL процесса для периода: {from_date} - {to_date}")
    
    try:
        # Используем стандартные колонки и группировку по месяцу
        columns = ['first_deposits_count', 'deposits_count', 'deposits_sum', 'partner_income', 'ngr']
        group_by = ['month', 'dynamic_tag_visit_id']
        process_all_accounts(from_date, to_date, columns, group_by)
        logger.info("ETL процесс завершен успешно")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Критическая ошибка в ETL процессе: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
