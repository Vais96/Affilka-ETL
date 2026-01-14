# Быстрый старт

## 1. Настройка переменных окружения

Скопируйте `.env.example` в `.env` и настройте:

**Для одного аккаунта:**
```env
AFFILKA_BASE_URL=https://admin.kawaii.partners
AFFILKA_TOKEN=your_statistic_token_here
```

**Для множественных аккаунтов (рекомендуется):**
```env
# Группа 1: один URL, несколько токенов
AFFILKA_BASE_URL_1=https://admin.kawaii.partners
AFFILKA_TOKEN_1=token1
AFFILKA_TOKEN_1_1=token1_1
AFFILKA_TOKEN_1_2=token1_2

# Группа 2: другой URL, один токен
AFFILKA_BASE_URL_2=https://admin2.kawaii.partners
AFFILKA_TOKEN_2=token2

# Группа 3: третий URL, несколько токенов
AFFILKA_BASE_URL_3=https://admin3.kawaii.partners
AFFILKA_TOKEN_3=token3
AFFILKA_TOKEN_3_1=token3_1
```

Логика:
- `AFFILKA_BASE_URL_N` - URL для группы N
- `AFFILKA_TOKEN_N` - основной токен для группы N
- `AFFILKA_TOKEN_N_M` - дополнительные токены (M = 1, 2, 3...)

## 2. Установка зависимостей

```bash
pip install -r requirements.txt
```

## 3. Проверка конфигурации

```bash
python test_config.py
```

Этот скрипт проверит:
- Правильность настройки переменных окружения
- Количество найденных аккаунтов
- Группировку по URL

## 4. Проверка подключения к API

```bash
python test_api.py
```

Этот скрипт проверит:
- Подключение к API
- Получение списка доступных колонок
- Получение тестового отчета

## 5. Проверка структуры БД

```bash
python check_db_schema.py
```

Убедитесь, что таблица `fact_click_month` существует и имеет правильную структуру.

## 6. Тестовый запуск ETL

```bash
# Загрузка данных за вчерашний день
python main.py

# Загрузка данных за конкретный период
python main.py --from-date 2025-01-01 --to-date 2025-01-31
```

## 7. Развертывание на Railway

1. Подключите репозиторий к Railway
2. Добавьте все переменные окружения из `.env` в Railway dashboard
3. Настройте cron для автоматического запуска (см. README.md)

## Возможные проблемы

### Ошибка "Не найдено ни одного токена"
- Проверьте, что переменная `AFFILKA_TOKEN` или `AFFILKA_TOKENS` установлена в `.env`
- Убедитесь, что файл `.env` находится в корне проекта

### Ошибка подключения к БД
- Проверьте правильность данных в `.env` (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
- Убедитесь, что БД доступна с вашего IP адреса

### Ошибка "Не найдены ключевые поля для upsert"
- Запустите `python check_db_schema.py` для проверки структуры таблицы
- Убедитесь, что таблица содержит поля `period_date` (или `period`, `date`) и `clickid` (или `click_id`)

### API возвращает пустой отчет
- Проверьте, что за указанный период есть данные в партнерской программе
- Убедитесь, что токен имеет права на доступ к отчетам
- Попробуйте запросить данные за другой период
