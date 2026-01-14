"""
Основной модуль ETL процесса для загрузки данных из Affilka API
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from affilka_api import AffilkaAPI
from database import Database
from config import get_affilka_accounts

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AffilkaETL:
    """Класс для выполнения ETL процесса"""
    
    def __init__(self, token: str, base_url: str, account_id: Optional[str] = None):
        """
        Args:
            token: Токен для подключения к API
            base_url: Базовый URL API
            account_id: Идентификатор аккаунта (для масштабирования)
        """
        self.api = AffilkaAPI(token, base_url)
        self.base_url = base_url
        self.account_id = account_id or token[:8]  # Используем первые 8 символов токена как ID
        self.db = Database()
    
    def normalize_clickid(self, clickid: str) -> str:
        """
        Нормализует clickid (удаляет дубли, очищает)
        
        Args:
            clickid: Исходный clickid
        
        Returns:
            Нормализованный clickid
        """
        if not clickid:
            return None
        
        # Удаляем пробелы и приводим к нижнему регистру для нормализации
        normalized = str(clickid).strip().lower()
        
        # Удаляем пустые значения
        if not normalized or normalized == 'none' or normalized == 'null':
            return None
        
        return normalized
    
    def transform_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Трансформирует данные: нормализует clickid и группирует по (period_date, clickid)
        
        Args:
            raw_data: Сырые данные из API
        
        Returns:
            Трансформированные данные
        """
        if not raw_data:
            return []
        
        # Группируем по (period_date, clickid) и суммируем метрики
        grouped = defaultdict(lambda: {
            'period_date': None,
            'clickid': None,
            'ftd': 0.0,
            'dep_cnt': 0.0,
            'dep_sum': 0.0,
            'ngr': 0.0,
            'cpa': 0.0,
        })
        
        for row in raw_data:
            # Нормализуем clickid
            clickid = self.normalize_clickid(row.get('clickid'))
            if not clickid:
                logger.warning(f"Пропущена строка без валидного clickid: {row}")
                continue
            
            period_date = row.get('period_date')
            if not period_date:
                logger.warning(f"Пропущена строка без period_date: {row}")
                continue
            
            # Создаем ключ для группировки
            key = (period_date, clickid)
            
            # Инициализируем группу если нужно
            if grouped[key]['period_date'] is None:
                grouped[key]['period_date'] = period_date
                grouped[key]['clickid'] = clickid
            
            # Суммируем метрики
            # FTD - это флаг (0 или 1), берем максимальное значение (если хотя бы одна запись имеет FTD=1, то FTD=1)
            grouped[key]['ftd'] = max(grouped[key]['ftd'], row.get('ftd', 0.0) or 0.0)
            grouped[key]['dep_cnt'] += row.get('dep_cnt', 0.0) or 0.0
            grouped[key]['dep_sum'] += row.get('dep_sum', 0.0) or 0.0
            grouped[key]['ngr'] += row.get('ngr', 0.0) or 0.0
            grouped[key]['cpa'] += row.get('cpa', 0.0) or 0.0
        
        # Преобразуем в список
        transformed = list(grouped.values())
        
        logger.info(f"Трансформировано {len(raw_data)} записей в {len(transformed)} уникальных групп")
        return transformed
    
    def load_data(self, data: List[Dict[str, Any]]):
        """
        Загружает данные в БД
        
        Args:
            data: Данные для загрузки
        """
        if not data:
            logger.warning("Нет данных для загрузки")
            return
        
        try:
            with self.db:
                self.db.upsert_fact_click_month(data, account_id=self.account_id)
                logger.info(f"Успешно загружено {len(data)} записей для аккаунта {self.account_id}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            raise
    
    def process_date_range(
        self,
        from_date: str,
        to_date: str,
        columns: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None
    ):
        """
        Выполняет полный ETL процесс для указанного диапазона дат
        
        Args:
            from_date: Начальная дата (YYYY-MM-DD)
            to_date: Конечная дата (YYYY-MM-DD)
            columns: Список колонок для запроса
            group_by: Список полей для группировки
        """
        logger.info(f"Начало ETL процесса для аккаунта {self.account_id}, период: {from_date} - {to_date}")
        
        # 1. Extract: Получаем данные из API
        logger.info("Шаг 1: Извлечение данных из API")
        # Используем конвертацию в EUR для всех валют
        report_data = self.api.fetch_report(
            from_date, to_date, columns, group_by,
            conversion_currency='EUR'
        )
        
        if not report_data:
            logger.error("Не удалось получить данные из API")
            return
        
        # 2. Parse: Парсим данные из формата API
        logger.info("Шаг 2: Парсинг данных API")
        raw_data = self.api.parse_report_data(report_data)
        
        if not raw_data:
            logger.warning("Нет данных для обработки после парсинга")
            return
        
        # Валидация: проверяем наличие clickid
        missing_clickid = [row for row in raw_data if not row.get('clickid')]
        if missing_clickid:
            logger.error(f"Найдено {len(missing_clickid)} записей без clickid. Прерываем обработку.")
            return
        
        # 3. Transform: Трансформируем данные
        logger.info("Шаг 3: Трансформация данных")
        transformed_data = self.transform_data(raw_data)
        
        if not transformed_data:
            logger.warning("Нет данных после трансформации")
            return
        
        # 4. Load: Загружаем в БД
        logger.info("Шаг 4: Загрузка данных в БД")
        self.load_data(transformed_data)
        
        # 5. Обогащаем данными из Keitaro через v_click_dims
        logger.info("Шаг 5: Обогащение данными из Keitaro (buyer_id, offer_id, creative_id)")
        try:
            with self.db:
                updated_count = self.db.enrich_dims_from_keitaro(
                    period_date_start=from_date,
                    period_date_end=to_date
                )
                if updated_count > 0:
                    logger.info(f"Обновлено {updated_count} записей с данными из Keitaro")
        except Exception as e:
            logger.warning(f"Не удалось обогатить данные из Keitaro (это не критично): {e}")
        
        logger.info(f"ETL процесс завершен успешно для аккаунта {self.account_id}")


def process_all_accounts(
    from_date: str,
    to_date: str,
    columns: Optional[List[str]] = None,
    group_by: Optional[List[str]] = None
):
    """
    Обрабатывает все аккаунты из конфигурации
    
    Args:
        from_date: Начальная дата (YYYY-MM-DD)
        to_date: Конечная дата (YYYY-MM-DD)
        columns: Список колонок для запроса
        group_by: Список полей для группировки
    """
    accounts = get_affilka_accounts()
    
    if not accounts:
        logger.error("Не найдено ни одного аккаунта в конфигурации. Проверьте переменные окружения.")
        return
    
    logger.info(f"Найдено {len(accounts)} аккаунтов для обработки")
    
    # Группируем аккаунты по URL для лучшего логирования
    accounts_by_url = {}
    for account in accounts:
        url = account['url']
        if url not in accounts_by_url:
            accounts_by_url[url] = []
        accounts_by_url[url].append(account)
    
    logger.info(f"Аккаунты сгруппированы по {len(accounts_by_url)} URL:")
    for url, url_accounts in accounts_by_url.items():
        logger.info(f"  - {url}: {len(url_accounts)} токен(ов)")
    
    for i, account in enumerate(accounts, 1):
        url = account['url']
        token = account['token']
        token_preview = token[:8] + "..." if len(token) > 8 else token
        logger.info(f"\n{'='*60}")
        logger.info(f"Обработка аккаунта {i}/{len(accounts)}")
        logger.info(f"URL: {url}")
        logger.info(f"Token: {token_preview}")
        logger.info(f"{'='*60}")
        try:
            etl = AffilkaETL(token, url, account_id=f"account_{i}")
            # Используем стандартные параметры, если не указаны
            if columns is None:
                columns = ['first_deposits_count', 'deposits_count', 'deposits_sum', 'partner_income', 'ngr']
            if group_by is None:
                group_by = ['day', 'dynamic_tag_visit_id']
            etl.process_date_range(from_date, to_date, columns, group_by)
        except Exception as e:
            logger.error(f"Ошибка при обработке аккаунта {i} ({url}): {e}", exc_info=True)
            continue
    
    # После загрузки всех аккаунтов, обогащаем данными из Keitaro для всего периода
    logger.info("\n" + "="*60)
    logger.info("Финальное обогащение данными из Keitaro для всех загруженных данных")
    logger.info("="*60)
    try:
        db = Database()
        with db:
            updated_count = db.enrich_dims_from_keitaro(
                period_date_start=from_date,
                period_date_end=to_date
            )
            if updated_count > 0:
                logger.info(f"Итого обновлено {updated_count} записей с данными из Keitaro (buyer_id, offer_id, creative_id)")
    except Exception as e:
        logger.warning(f"Не удалось выполнить финальное обогащение из Keitaro (это не критично): {e}")
    
    logger.info("Обработка всех аккаунтов завершена")
