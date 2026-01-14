"""
Модуль для работы с Affilka API
"""
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
from config import AFFILKA_API_ENDPOINT, AFFILKA_MAP

logger = logging.getLogger(__name__)


class AffilkaAPI:
    """Класс для работы с Affilka API"""
    
    def __init__(self, token: str, base_url: str):
        """
        Args:
            token: Токен для авторизации
            base_url: Базовый URL API
        """
        self.token = token
        self.base_url = base_url
        self.endpoint = AFFILKA_API_ENDPOINT
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': token,
            'Version': 'HTTP/1.0'
        })
    
    def get_available_columns(self) -> Optional[List[str]]:
        """Получает список доступных колонок из API"""
        try:
            url = f"{self.base_url}/api/customer/v1/partner/report/attributes"
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get('available_columns', [])
        except Exception as e:
            logger.error(f"Ошибка при получении доступных колонок: {e}")
            return None
    
    def fetch_report(
        self,
        from_date: str,
        to_date: str,
        columns: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None,
        async_mode: bool = False,
        conversion_currency: Optional[str] = None,
        exchange_rates_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Получает отчет из API
        
        Args:
            from_date: Начальная дата в формате ISO 8601 (YYYY-MM-DD)
            to_date: Конечная дата в формате ISO 8601 (YYYY-MM-DD)
            columns: Список колонок для включения в отчет
            group_by: Список полей для группировки
            async_mode: Асинхронный режим расчета отчета
        
        Returns:
            Словарь с данными отчета или None в случае ошибки
        """
        if columns is None:
            # Используем колонки из маппинга
            columns = [
                'first_deposits_count',  # ftd
                'deposits_count',        # dep_cnt
                'deposits_sum',          # dep_sum
                'partner_income',        # cpa
                'ngr',                   # ngr (Net Gaming Revenue)
                'visits_count',          # для информации
            ]
        
        if group_by is None:
            # Группируем по дню и dynamic_tag для получения visit_id/sub_id
            # Приоритет: visit_id > sub_id > click_id > campaign
            group_by = ['day', 'dynamic_tag_visit_id']
        
        try:
            url = f"{self.base_url}{self.endpoint}"
            
            # Формируем параметры запроса
            # Для массивов используем список значений, requests автоматически преобразует в columns[]=val1&columns[]=val2
            params = {
                'async': 'true' if async_mode else 'false',
                'from': from_date,
                'to': to_date,
            }
            
            # Добавляем параметры конвертации валют
            if conversion_currency:
                params['conversion_currency'] = conversion_currency
            if exchange_rates_date:
                params['exchange_rates_date'] = exchange_rates_date
            
            # Добавляем columns как массив (requests поддерживает списки в params)
            if columns:
                params['columns[]'] = columns
            
            # Добавляем group_by как массив
            if group_by:
                params['group_by[]'] = group_by
            
            logger.info(f"Запрос к API: {url} с параметрами {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Получен ответ от API, тип отчета: {data.get('report_type')}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Ответ сервера: {e.response.text}")
            return None
    
    def parse_report_data(self, report_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Парсит данные отчета из формата API в формат для БД
        
        Args:
            report_data: Данные отчета от API
        
        Returns:
            Список словарей с нормализованными данными
        """
        if not report_data or 'rows' not in report_data:
            logger.warning("Отчет не содержит данных rows")
            return []
        
        rows = report_data.get('rows', {}).get('data', [])
        if not rows:
            logger.warning("Отчет не содержит данных в rows.data")
            return []
        
        parsed_data = []
        
        for row in rows:
            # Каждая строка - это массив объектов с name, value, type
            row_dict = {}
            clickid = None
            period_date = None
            
            for field in row:
                field_name = field.get('name')
                field_value = field.get('value')
                field_type = field.get('type')
                
                if not field_name:
                    continue
                
                # Обработка даты
                if field_name == 'date':
                    if isinstance(field_value, str):
                        # Парсим ISO 8601 дату
                        try:
                            dt = datetime.fromisoformat(field_value.replace('Z', '+00:00'))
                            period_date = dt.date()
                            row_dict['period_date'] = period_date
                        except:
                            logger.warning(f"Не удалось распарсить дату: {field_value}")
                    elif isinstance(field_value, (datetime,)):
                        period_date = field_value.date()
                        row_dict['period_date'] = period_date
                
                # Обработка clickid
                # Приоритет: dynamic_tag_visit_id > dynamic_tag_sub_id > dynamic_tag_click_id > 
                #            dynamic_tag_subid > visit_id > sub_id > campaign_id > player_id
                elif field_name in ['dynamic_tag_visit_id', 'dynamic_tag_sub_id', 'dynamic_tag_click_id', 
                                   'dynamic_tag_subid', 'dynamic_tag_web_id', 'dynamic_tag_webid']:
                    # Dynamic tags - это то, что нам нужно (visit_id, sub_id и т.д.)
                    if field_value is not None:
                        clickid_val = str(field_value).strip()
                        if clickid_val and clickid_val.lower() not in ['null', 'none', '']:
                            # Используем первое найденное значение (приоритет по порядку в списке)
                            if not clickid:
                                clickid = clickid_val
                                row_dict['clickid'] = clickid
                elif field_name in ['visit_id', 'sub_id', 'clickid']:
                    # Прямые поля для clickid (если API их возвращает напрямую)
                    if field_value is not None:
                        clickid_val = str(field_value).strip()
                        if clickid_val and clickid_val.lower() not in ['null', 'none', '']:
                            if not clickid:
                                clickid = clickid_val
                                row_dict['clickid'] = clickid
                elif field_name in ['campaign_id', 'campaign']:
                    # Используем campaign_id как fallback, если нет dynamic_tag
                    if not clickid and field_value is not None:
                        clickid = str(field_value)
                        row_dict['clickid'] = clickid
                    row_dict['campaign_id'] = field_value
                elif field_name in ['player_id', 'player']:
                    # Используем player_id как последний fallback
                    if not clickid and field_value is not None:
                        clickid = str(field_value)
                        row_dict['clickid'] = clickid
                
                # Маппинг метрик
                elif field_name == 'first_deposits_count':
                    row_dict['ftd'] = self._parse_number(field_value)
                elif field_name == 'deposits_count':
                    row_dict['dep_cnt'] = self._parse_number(field_value)
                elif field_name == 'deposits_sum':
                    # deposits_sum может быть объектом с currency и amount
                    if isinstance(field_value, dict):
                        amount = (field_value.get('amount') or 
                                 field_value.get('amount_cents') or 
                                 field_value.get('value') or 0)
                        row_dict['dep_sum'] = self._parse_number(amount)
                    else:
                        row_dict['dep_sum'] = self._parse_number(field_value)
                elif field_name == 'ngr':
                    # NGR может быть объектом с currency и amount
                    if isinstance(field_value, dict):
                        amount = (field_value.get('amount') or 
                                 field_value.get('amount_cents') or 
                                 field_value.get('value') or 0)
                        row_dict['ngr'] = self._parse_number(amount)
                    else:
                        row_dict['ngr'] = self._parse_number(field_value)
                elif field_name in ['partner_income', 'clean_net_revenue']:
                    # partner_income может быть объектом с currency и amount
                    if isinstance(field_value, dict):
                        # Пробуем разные варианты ключей
                        amount = (field_value.get('amount') or 
                                 field_value.get('amount_cents') or 
                                 field_value.get('value') or 0)
                        row_dict['cpa'] = self._parse_number(amount)
                    else:
                        row_dict['cpa'] = self._parse_number(field_value)
                
                # Сохраняем все остальные поля для отладки
                else:
                    row_dict[f'_{field_name}'] = field_value
            
            # Валидация: должны быть period_date и clickid
            if not period_date or not clickid:
                logger.warning(f"Пропущена строка без period_date или clickid: {row_dict}")
                continue
            
            parsed_data.append(row_dict)
        
        logger.info(f"Распарсено {len(parsed_data)} записей из отчета")
        return parsed_data
    
    def _parse_number(self, value: Any) -> float:
        """Парсит число из различных форматов"""
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.replace(',', ''))
            except:
                return 0.0
        if isinstance(value, dict):
            # Для вложенных объектов с amount
            amount = value.get('amount') or value.get('amount_cents', 0)
            return self._parse_number(amount)
        return 0.0
