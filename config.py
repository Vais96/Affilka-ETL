"""
Конфигурация для ETL процесса Affilka
"""
import os
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}

# Affilka API configuration
AFFILKA_API_ENDPOINT = '/api/customer/v1/partner/report'

# Поддержка множественных аккаунтов (URL + токен)
def get_affilka_accounts() -> List[Dict[str, str]]:
    """
    Получает список аккаунтов (URL + токен) для подключения к API
    
    Формат конфигурации:
    - AFFILKA_BASE_URL_1 - URL для первой группы аккаунтов
    - AFFILKA_TOKEN_1 - основной токен для первой группы
    - AFFILKA_TOKEN_1_1 - дополнительный токен для первой группы
    - AFFILKA_TOKEN_1_2 - еще один токен для первой группы
    - AFFILKA_BASE_URL_2 - URL для второй группы аккаунтов
    - AFFILKA_TOKEN_2 - основной токен для второй группы
    - и т.д.
    
    Также поддерживается старый формат для обратной совместимости:
    - AFFILKA_BASE_URL + AFFILKA_TOKEN (один аккаунт)
    - AFFILKA_ACCOUNTS=url1|token1,url2|token2 (пары через запятую)
    
    Returns:
        Список словарей с ключами 'url' и 'token'
    """
    accounts = []
    
    # Новый формат: AFFILKA_BASE_URL_N с токенами AFFILKA_TOKEN_N, AFFILKA_TOKEN_N_M
    i = 1
    while True:
        base_url = os.getenv(f'AFFILKA_BASE_URL_{i}')
        if not base_url:
            break
        
        # Ищем все токены для этого URL
        tokens_for_url = []
        
        # Основной токен: AFFILKA_TOKEN_N
        main_token = os.getenv(f'AFFILKA_TOKEN_{i}')
        if main_token:
            tokens_for_url.append(main_token)
        
        # Дополнительные токены: AFFILKA_TOKEN_N_M (где M = 1, 2, 3...)
        j = 1
        while True:
            additional_token = os.getenv(f'AFFILKA_TOKEN_{i}_{j}')
            if not additional_token:
                break
            tokens_for_url.append(additional_token)
            j += 1
        
        # Создаем аккаунты для каждого токена с этим URL
        for token in tokens_for_url:
            accounts.append({'url': base_url, 'token': token})
        
        i += 1
    
    # Старый формат 1: пары URL|TOKEN через запятую (для обратной совместимости)
    if not accounts:
        accounts_env = os.getenv('AFFILKA_ACCOUNTS')
        if accounts_env:
            for account_str in accounts_env.split(','):
                account_str = account_str.strip()
                if '|' in account_str:
                    parts = account_str.split('|', 1)
                    if len(parts) == 2:
                        url = parts[0].strip()
                        token = parts[1].strip()
                        if url and token:
                            accounts.append({'url': url, 'token': token})
    
    # Старый формат 2: один аккаунт AFFILKA_BASE_URL + AFFILKA_TOKEN (для обратной совместимости)
    if not accounts:
        base_url = os.getenv('AFFILKA_BASE_URL', 'https://admin.kawaii.partners')
        token = os.getenv('AFFILKA_TOKEN')
        if token:
            accounts.append({'url': base_url, 'token': token})
    
    # Старый формат 3: только токены с дефолтным URL (для обратной совместимости)
    if not accounts:
        tokens_env = os.getenv('AFFILKA_TOKENS')
        if tokens_env:
            base_url = os.getenv('AFFILKA_BASE_URL', 'https://admin.kawaii.partners')
            for token in tokens_env.split(','):
                token = token.strip()
                if token:
                    accounts.append({'url': base_url, 'token': token})
    
    return accounts

# Для обратной совместимости (deprecated, используйте get_affilka_accounts)
def get_affilka_tokens() -> List[str]:
    """Получает список токенов (deprecated, используйте get_affilka_accounts)"""
    accounts = get_affilka_accounts()
    # Возвращаем только токены, используя первый URL для всех
    return [acc['token'] for acc in accounts]

# Маппинг колонок API к полям БД
AFFILKA_MAP = {
    'clickid': ['visit_id', 'sub_id', 'player_id', 'campaign_id'],  # Будем использовать player_id или campaign_id из group_by
    'ftd': 'first_deposits_count',
    'dep_cnt': 'deposits_count',
    'dep_sum': 'deposits_sum',
    'cpa': ['partner_income', 'clean_net_revenue', 'ngr'],  # Попробуем найти подходящее поле
    'period': 'date',
}
