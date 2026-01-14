"""
Скрипт для тестирования подключения к Affilka API
"""
import sys
from datetime import datetime, timedelta
from config import get_affilka_accounts
from affilka_api import AffilkaAPI
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_api_connection():
    """Тестирует подключение к API"""
    accounts = get_affilka_accounts()
    
    if not accounts:
        logger.error("Не найдено ни одного аккаунта. Проверьте переменные окружения.")
        return False
    
    logger.info(f"Найдено {len(accounts)} аккаунтов")
    
    for i, account in enumerate(accounts, 1):
        url = account['url']
        token = account['token']
        logger.info(f"\n{'='*50}")
        logger.info(f"Тестирование аккаунта {i}/{len(accounts)}")
        logger.info(f"URL: {url}")
        logger.info(f"{'='*50}")
        
        api = AffilkaAPI(token, url)
        
        # Тест 1: Получение доступных колонок
        logger.info("\n1. Тест получения доступных колонок...")
        try:
            columns = api.get_available_columns()
            if columns:
                logger.info(f"✓ Получено {len(columns)} доступных колонок")
                logger.info(f"  Примеры: {', '.join(columns[:10])}")
            else:
                logger.warning("✗ Не удалось получить список колонок")
        except Exception as e:
            logger.error(f"✗ Ошибка при получении колонок: {e}")
        
        # Тест 2: Получение отчета за последние 7 дней
        logger.info("\n2. Тест получения отчета...")
        try:
            to_date = datetime.now().date()
            from_date = to_date - timedelta(days=7)
            
            logger.info(f"  Запрос данных за период: {from_date} - {to_date}")
            
            report = api.fetch_report(
                from_date=str(from_date),
                to_date=str(to_date),
                columns=['first_deposits_count', 'deposits_count', 'deposits_sum'],
                group_by=['day', 'dynamic_tag_visit_id']  # Используем dynamic_tag_visit_id для получения visit_id как clickid
            )
            
            if report:
                logger.info(f"✓ Отчет получен успешно")
                logger.info(f"  Тип отчета: {report.get('report_type')}")
                
                # Парсим данные
                parsed = api.parse_report_data(report)
                logger.info(f"  Распарсено записей: {len(parsed)}")
                
                if parsed:
                    logger.info(f"  Пример первой записи:")
                    first = parsed[0]
                    for key, value in first.items():
                        if not key.startswith('_'):
                            logger.info(f"    {key}: {value}")
                else:
                    logger.warning("  Нет данных в отчете (возможно, нет данных за этот период)")
            else:
                logger.error("✗ Не удалось получить отчет")
                
        except Exception as e:
            logger.error(f"✗ Ошибка при получении отчета: {e}", exc_info=True)
    
    return True


if __name__ == '__main__':
    success = test_api_connection()
    sys.exit(0 if success else 1)
