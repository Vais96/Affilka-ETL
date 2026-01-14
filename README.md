# Affilka ETL - Загрузка данных о качестве трафика через API

ETL процесс для загрузки данных из партнерской программы Affilka через API в базу данных.

## Описание

Проект заменяет загрузку CSV файлов на прямую интеграцию с API Affilka. Данные о качестве трафика загружаются в таблицу `fact_click_month` с поддержкой множественных аккаунтов.

## Структура проекта

```
.
├── main.py                 # Главный скрипт для запуска ETL
├── config.py               # Конфигурация (БД, API, токены)
├── database.py             # Модуль для работы с БД
├── affilka_api.py          # Модуль для работы с Affilka API
├── etl_process.py          # Основной ETL процесс
├── check_db_schema.py      # Скрипт для проверки структуры БД
├── requirements.txt        # Зависимости Python
├── Procfile               # Конфигурация для Railway
├── railway.json           # Дополнительная конфигурация Railway
├── .env.example           # Пример файла с переменными окружения
└── .env                   # Переменные окружения (не в git, создайте из .env.example)
```

## Деплой на Railway

### Команда для запуска

**Основная команда:**
```bash
python main.py
```

**По умолчанию:** загружается весь месяц с 1 числа по текущее число.

**Группировка:** используется `group_by=['month', 'dynamic_tag_visit_id']`, поэтому:
- Данные агрегируются по месяцу
- В БД записываются с `period_date` = первое число месяца (например, `2026-01-01` для января)
- Это позволяет правильно распределить данные по месяцам в БД

### Настройка Cron на Railway

1. В Railway перейдите в настройки проекта → **Cron Jobs**
2. Добавьте новый Cron Job:
   - **Schedule**: `0 2 * * *` (каждый день в 02:00 UTC)
   - **Command**: `python main.py`

### Множественные аккаунты

**✅ Да, скрипт автоматически обрабатывает все аккаунты!**

При добавлении новых переменных окружения в Railway:
- `AFFILKA_BASE_URL_2` + `AFFILKA_TOKEN_2`
- `AFFILKA_BASE_URL_3` + `AFFILKA_TOKEN_3`
- и т.д.

**Скрипт автоматически найдет все аккаунты и обработает их последовательно:**

1. **Первый аккаунт** (AFFILKA_BASE_URL_1):
   - Загружает данные из API
   - Трансформирует и загружает в БД
   - Обогащает данными из Keitaro

2. **Второй аккаунт** (AFFILKA_BASE_URL_2):
   - Загружает данные из API
   - Трансформирует и загружает в БД
   - Обогащает данными из Keitaro

3. **И так далее** по всем найденным аккаунтам

4. **Финальное обогащение** данными из Keitaro для всех загруженных данных

**Важно:** Не нужно менять код! Просто добавляйте переменные окружения в Railway, и скрипт автоматически их найдет и обработает.

Подробнее см. [DEPLOY.md](DEPLOY.md)

## Установка (локально)

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Настройте переменные окружения:
   
   Скопируйте `.env.example` в `.env` и заполните значения:
   ```bash
   cp .env.example .env
   ```
   
   Затем отредактируйте `.env` и укажите ваши данные.
   
   **Для одного аккаунта:**
   ```env
   # Database
   DB_HOST=your_db_host
   DB_PORT=3306
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_NAME=your_db_name

   # Affilka API (один аккаунт)
   AFFILKA_BASE_URL=https://admin.kawaii.partners
   AFFILKA_TOKEN=your_statistic_token_here
   ```
   
   **Для множественных аккаунтов (рекомендуется):**
   
   Новый формат - группы URL с токенами:
   ```env
   # Первая группа: один URL, несколько токенов
   AFFILKA_BASE_URL_1=https://admin.kawaii.partners
   AFFILKA_TOKEN_1=token1
   AFFILKA_TOKEN_1_1=token1_1
   AFFILKA_TOKEN_1_2=token1_2
   
   # Вторая группа: другой URL, один токен
   AFFILKA_BASE_URL_2=https://admin2.kawaii.partners
   AFFILKA_TOKEN_2=token2
   
   # Третья группа: третий URL, несколько токенов
   AFFILKA_BASE_URL_3=https://admin3.kawaii.partners
   AFFILKA_TOKEN_3=token3
   AFFILKA_TOKEN_3_1=token3_1
   ```
   
   Логика:
   - `AFFILKA_BASE_URL_N` определяет URL для группы N
   - `AFFILKA_TOKEN_N` - основной токен для группы N
   - `AFFILKA_TOKEN_N_M` - дополнительные токены для той же группы (M = 1, 2, 3...)

## Использование

### Ручной запуск

```bash
# Загрузка данных за вчерашний день (по умолчанию)
python main.py

# Загрузка данных за конкретный период
python main.py --from-date 2025-01-01 --to-date 2025-01-31

# Загрузка данных за последние 7 дней
python main.py --days-back 7
```

### Проверка структуры БД

```bash
python check_db_schema.py
```

## Процесс ETL

1. **Extract (Извлечение)**: Получение данных из Affilka API
   - Endpoint: `GET /api/customer/v1/partner/report`
   - Параметры: `from`, `to`, `columns[]`, `group_by[]`

2. **Transform (Трансформация)**:
   - Нормализация `clickid` (удаление дублей)
   - Группировка по `(period_date, clickid)`
   - Суммирование метрик: `ftd`, `dep_cnt`, `dep_sum`, `cpa`

3. **Load (Загрузка)**:
   - Upsert в таблицу `fact_click_month`
   - Поддержка множественных аккаунтов через `account_id`

## Маппинг данных

| Поле БД | API поле | Описание |
|---------|----------|----------|
| `period_date` | `date` | Дата периода |
| `clickid` | `dynamic_tag_visit_id` / `dynamic_tag_sub_id` | Идентификатор визита (visit_id, sub_id) |
| `ftd` | `first_deposits_count` | Количество первых депозитов |
| `dep_cnt` | `deposits_count` | Количество депозитов |
| `dep_sum` | `deposits_sum` | Сумма депозитов |
| `cpa` | `partner_income` / `clean_net_revenue` | Доход партнера |

## Масштабирование

Проект поддерживает работу с несколькими аккаунтами (разные URL и токены):

**Новый формат (рекомендуется):**

Группировка по URL с возможностью нескольких токенов для каждого URL:

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

**Логика работы:**
- `AFFILKA_BASE_URL_N` определяет базовый URL для группы аккаунтов N
- `AFFILKA_TOKEN_N` - основной токен для группы N
- `AFFILKA_TOKEN_N_M` - дополнительные токены для той же группы (M = 1, 2, 3...)
- ETL процесс прогоняется по всем найденным комбинациям (URL, token) последовательно

**Старые форматы (для обратной совместимости):**
- `AFFILKA_BASE_URL` + `AFFILKA_TOKEN` (один аккаунт)
- `AFFILKA_ACCOUNTS=url1|token1,url2|token2` (пары через запятую)

Все аккаунты обрабатываются автоматически при запуске `main.py`. Каждый аккаунт получает уникальный `account_id` для различения данных в БД.

## Развертывание на Railway

1. Подключите репозиторий к Railway
2. Настройте переменные окружения в Railway dashboard:
   - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
   - `AFFILKA_BASE_URL` (опционально, по умолчанию `https://admin.kawaii.partners`)
   - `AFFILKA_TOKEN` или `AFFILKA_TOKENS` (для множественных аккаунтов)
3. Railway автоматически определит Python проект и установит зависимости

### Настройка автоматического запуска (Cron)

Railway поддерживает cron через несколько способов:

#### Вариант 1: Использование Railway Cron (рекомендуется)

1. В Railway dashboard создайте новый сервис типа "Cron"
2. Укажите команду: `python main.py --days-back 1`
3. Настройте расписание (например, `0 2 * * *` для запуска каждый день в 02:00 UTC)

#### Вариант 2: Использование внешнего сервиса

Используйте внешний сервис для cron (например, cron-job.org, EasyCron) который будет делать HTTP запрос к вашему Railway сервису или использовать Railway CLI.

#### Вариант 3: Использование GitHub Actions

Создайте `.github/workflows/etl.yml`:
```yaml
name: Affilka ETL
on:
  schedule:
    - cron: '0 2 * * *'  # Каждый день в 02:00 UTC
  workflow_dispatch:  # Ручной запуск

jobs:
  etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python main.py --days-back 1
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_PORT: ${{ secrets.DB_PORT }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
          DB_NAME: ${{ secrets.DB_NAME }}
          AFFILKA_TOKEN: ${{ secrets.AFFILKA_TOKEN }}
```

## Логирование

Все операции логируются с уровнем INFO. Логи включают:
- Подключение к БД
- Запросы к API
- Количество обработанных записей
- Ошибки и предупреждения

## Требования

- Python 3.8+
- MySQL/MariaDB база данных
- Доступ к Affilka API с валидным токеном

## Примечания

- API требует обязательные параметры `from` и `to` (даты в формате ISO 8601)
- `clickid` получается через группировку по `player` или `campaign` в API
- Данные автоматически дедуплицируются по `(period_date, clickid)`
- При отсутствии `clickid` в данных, запись пропускается (валидация)
