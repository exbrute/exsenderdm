# EXSender v2

Многоаккаунтный Telegram userbot + бот управления для рассылок и парсинга аудитории.  
Проект использует **Telethon** (userbot-сессии) и **python-telegram-bot** (панель управления в Telegram).

> Важно: используйте инструмент только в рамках правил Telegram и законодательства вашей страны.

## Возможности

- Мультиаккаунт: несколько userbot-слотов в одном рантайме.
- Управление через Telegram-бота: меню, кнопки, настройка без редактирования кода.
- Парсинг аудитории:
  - из участников групп;
  - из личных диалогов;
  - из отправителей последних сообщений (до 15k).
- Рассылка в ЛС по очереди с учетом дедупликации и лимитов.
- Настройка текста, интервалов, рандома интервала и источников сообщений.
- Поддержка прокси на уровне аккаунта.
- Поддержка входа по ссылкам `t.me`, invite и `addlist`.
- Фоновые отчеты администраторам о статусах и ошибках.

## Стек

- Python 3.10+
- [Telethon](https://github.com/LonamiWebs/Telethon)
- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot)
- [python-dotenv](https://github.com/theskumar/python-dotenv)

## Быстрый старт

### 1) Клонирование и установка зависимостей

```bash
git clone <your-repo-url>
cd "exsender v2"
python -m venv .venv
```

**Windows (PowerShell):**

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Linux / macOS:**

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Настройка `.env`

Создайте файл `.env` в корне проекта и заполните минимум:

```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_bot_token
ADMIN_USER_IDS=123456789,987654321
```

### 3) Запуск

```bash
python main.py
```

После запуска откройте вашего control-бота в Telegram и отправьте `/start`.

## Переменные окружения

### Обязательные

- `API_ID` — Telegram API ID.
- `API_HASH` — Telegram API hash.

### Рекомендуемые для панели управления

- `BOT_TOKEN` — токен Telegram-бота управления.
- `ADMIN_USER_IDS` — список Telegram user id админов через запятую.

### Дополнительные

- `ACCOUNTS` — список слотов аккаунтов (по умолчанию: `default`).
- `STOP_SPAM_ON_START` — сбрасывает активную рассылку при старте (`1/true/yes/on`).
- `TELETHON_CONNECTION_RETRIES` — количество попыток подключения Telethon.
- `TELETHON_RETRY_DELAY` — задержка между попытками подключения.
- `BOT_API_CONNECT_TIMEOUT`, `BOT_API_READ_TIMEOUT`, `BOT_API_WRITE_TIMEOUT`, `BOT_API_POOL_TIMEOUT` — таймауты Bot API.
- `YOD_BOT` — бот для авто-команды `/start` (например anti-spam сервис).
- `YOD_START_INTERVAL_SEC` — интервал между двумя `/start`.
- `YOD_ON_PEER_FLOOD` — включить авто-YOD после PeerFlood (`1/0`).
- `YOD_PEER_FLOOD_COOLDOWN_SEC` — cooldown авто-YOD после ограничений.
- `YOD_MULTI_SLOT_DELAY_SEC` — задержка запуска YOD между слотами.

## Как устроен проект

- `main.py` — точка входа: загрузка состояния, запуск userbot-ов и control-бота.
- `control_bot.py` — интерфейс управления (кнопки, сценарии, команды, онбординг аккаунтов).
- `spam_scheduler.py` — цикл рассылки, интервалы, очереди, обработка ограничений.
- `state.py` — модель и хранение состояния (`runtime_state.json`).
- `telethon_accounts.py` — создание клиентов и подключение с fallback.
- `telethon_join.py` — вступление в чаты/каналы по ссылкам, обработка `addlist`.
- `group_dialogs.py` — парсинг пользователей в очередь рассылки.
- `proxy_util.py` — разбор прокси.
- `report_queue.py` — очередь системных отчетов администраторам.
- `yod_util.py` — утилиты YOD (`/start` x2).

## Данные и сессии

- `runtime_state.json` создается автоматически и хранит текущее состояние слотов/настроек.
- Файлы сессий Telethon хранятся в папке `sessions/`.
- Не коммитьте `.env`, `sessions/` и `runtime_state.json` в публичный репозиторий.

## Типовой сценарий использования

1. Запускаете `main.py`.
2. В control-боте добавляете/авторизуете слот.
3. Добавляете источники (группы) и парсите пользователей в очередь.
4. Настраиваете текст, интервалы и источник контента.
5. Включаете рассылку и отслеживаете отчеты.

## Рекомендации по безопасности

- Используйте отдельные Telegram-аккаунты под автоматизацию.
- Ограничьте доступ к control-боту через `ADMIN_USER_IDS`.
- Никогда не публикуйте токены и API-ключи.
- Добавьте в `.gitignore` как минимум:
  - `.env`
  - `sessions/`
  - `runtime_state.json`

## Лицензия

Добавьте подходящую лицензию перед публичной публикацией (например MIT).
