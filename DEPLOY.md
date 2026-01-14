# Деплой на Railway

## Команда для запуска

**Основная команда:**
```bash
python main.py
```

По умолчанию загружается весь месяц с 1 числа по текущее число.

## Настройка на Railway

### 1. Переменные окружения

Добавьте в Railway все переменные из `.env.example`:

**База данных:**
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

**Affilka API (один аккаунт):**
- `AFFILKA_BASE_URL_1`
- `AFFILKA_TOKEN_1`

**Affilka API (несколько аккаунтов):**
- `AFFILKA_BASE_URL_1`
- `AFFILKA_TOKEN_1`
- `AFFILKA_TOKEN_1_1` (опционально, дополнительные токены для первого URL)
- `AFFILKA_TOKEN_1_2` (опционально)
- `AFFILKA_BASE_URL_2`
- `AFFILKA_TOKEN_2`
- и т.д.

### 2. Настройка Cron

В Railway:
1. Перейдите в настройки проекта → **Cron Jobs**
2. Добавьте новый Cron Job:
   - **Schedule**: `0 2 * * *` (каждый день в 02:00 UTC)
   - **Command**: `python main.py`

### 3. Procfile

Railway автоматически использует `Procfile`:
```
worker: python main.py --days-back 1
```

## Автоматическая обработка множественных аккаунтов

**Да, скрипт автоматически обрабатывает все аккаунты!**

Функция `get_affilka_accounts()` в `config.py` автоматически находит все аккаунты по паттерну:
- `AFFILKA_BASE_URL_1` + `AFFILKA_TOKEN_1`, `AFFILKA_TOKEN_1_1`, `AFFILKA_TOKEN_1_2`...
- `AFFILKA_BASE_URL_2` + `AFFILKA_TOKEN_2`, `AFFILKA_TOKEN_2_1`...
- `AFFILKA_BASE_URL_3` + `AFFILKA_TOKEN_3`...
- и т.д.

**Процесс обработки:**
1. Скрипт находит все аккаунты из переменных окружения
2. Обрабатывает их последовательно (один за другим)
3. Для каждого аккаунта:
   - Загружает данные из API
   - Трансформирует данные
   - Загружает в БД
   - Обогащает данными из Keitaro
4. После всех аккаунтов выполняет финальное обогащение

**Пример логов:**
```
Найдено 3 аккаунтов для обработки
Аккаунты сгруппированы по 2 URL:
  - https://admin.kawaii.partners: 2 токен(ов)
  - https://admin2.kawaii.partners: 1 токен(ов)

Обработка аккаунта 1/3
URL: https://admin.kawaii.partners
Token: 5c68ed6c...
...

Обработка аккаунта 2/3
URL: https://admin.kawaii.partners
Token: abc12345...
...

Обработка аккаунта 3/3
URL: https://admin2.kawaii.partners
Token: def67890...
...
```

## Добавление новых аккаунтов

Чтобы добавить новый аккаунт:
1. В Railway добавьте переменные окружения:
   - `AFFILKA_BASE_URL_N` (где N - следующий номер)
   - `AFFILKA_TOKEN_N`
2. Перезапустите сервис (или дождитесь следующего cron запуска)
3. Скрипт автоматически найдет и обработает новый аккаунт

**Важно:** Не нужно менять код, просто добавляйте переменные окружения!
