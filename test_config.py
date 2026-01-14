"""
Скрипт для тестирования конфигурации аккаунтов
Помогает проверить, правильно ли настроены переменные окружения
"""
import os
from config import get_affilka_accounts
from dotenv import load_dotenv

load_dotenv()

def test_config():
    """Тестирует конфигурацию аккаунтов"""
    print("=" * 60)
    print("Тестирование конфигурации Affilka аккаунтов")
    print("=" * 60)
    
    accounts = get_affilka_accounts()
    
    if not accounts:
        print("\n❌ Не найдено ни одного аккаунта!")
        print("\nПроверьте переменные окружения в .env файле.")
        print("\nПример конфигурации:")
        print("  AFFILKA_BASE_URL_1=https://admin.kawaii.partners")
        print("  AFFILKA_TOKEN_1=your_token_1")
        print("  AFFILKA_TOKEN_1_1=your_token_1_1")
        return False
    
    print(f"\n✓ Найдено {len(accounts)} аккаунт(ов)\n")
    
    # Группируем по URL
    accounts_by_url = {}
    for account in accounts:
        url = account['url']
        if url not in accounts_by_url:
            accounts_by_url[url] = []
        accounts_by_url[url].append(account)
    
    # Выводим информацию
    for url_idx, (url, url_accounts) in enumerate(accounts_by_url.items(), 1):
        print(f"Группа {url_idx}: {url}")
        print(f"  Токенов: {len(url_accounts)}")
        for token_idx, account in enumerate(url_accounts, 1):
            token = account['token']
            token_preview = token[:12] + "..." if len(token) > 12 else token
            print(f"    {token_idx}. {token_preview}")
        print()
    
    print("=" * 60)
    print("Конфигурация корректна! ✓")
    print("=" * 60)
    
    return True

if __name__ == '__main__':
    test_config()
