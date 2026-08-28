# EPD System – Анализ Единых Платежных Документов (ЖКХ) с AI

Система автоматического сбора, парсинга, анализа и визуализации данных из Единых Платежных Документов (ЕПД) ЖКХ с использованием AI-ассистента (через OpenAI-совместимый API). Результаты анализа отправляются в Telegram и отображаются в веб-интерфейсе.

## 🚀 Возможности

- **Фильтрация писем** – через `getmail` по правилам из БД (отправитель, получатель, тема).
- **Парсинг ЕПД** – извлечение данных из HTML/текста писем (поддерживается формат mos.ru).
- **Хранение** – SQLite с полной историей начислений по месяцам, услугам и лицевым счетам.
- **AI-анализ** – автоматический запрос к LLM (OpenRouter, Groq, Ollama) для формирования аналитического отчёта.
- **Telegram-уведомления** – отправка красиво оформленных отчётов в мессенджер.
- **Веб-интерфейс** – просмотр динамики платежей, детализация по услугам, управление AI-запросами.
- **Оркестратор** – фоновый процесс для обработки AI-запросов и отправки сообщений с повторными попытками.
- **CI/CD** – готовые тесты и GitHub Actions.

## 🧱 Архитектура

| Компонент | Описание |
|-----------|----------|
| `getmail_filter.py` | Фильтр для `getmail`, проверяет письма по правилам из БД и запускает парсер. |
| `parsers/mos_parser.py` | Парсер для писем от mos.ru (HTML/текст). |
| `parsers/base_parser.py` | Общие функции для всех парсеров (работа с БД, нормализация услуг). |
| `orchestrator.py` | Оркестратор: обрабатывает AI-запросы (статус `pending`) и отправляет Telegram-сообщения. |
| `frontend.py` | Веб-интерфейс на Flask. |
| `utils.py` | Общие утилиты (загрузка конфигов, логирование, подключение к БД). |
| `init_db.py` | Инициализация и миграция БД. |
| `tests/` | Модульные тесты для всех компонентов. |

## 📋 Требования

- Python 3.9+
- SQLite3
- getmail (для приёма почты)
- Telegram бот (токен)
- API-ключ для AI-провайдера (OpenRouter, Groq, Ollama)

## ⚙️ Установка

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/yourusername/epd-system.git
   cd epd-system
   ```

2. Создайте и активируйте виртуальное окружение:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

4. Создайте конфигурационные файлы:

   - **`config.ini`** – скопируйте из примера:
     ```ini
     [paths]
     db_path = /path/to/epd.db
     email_temp_dir = /tmp/
     lock_file = /tmp/epd.lock

     [frontend]
     port = 5000
     static_dir = static
     debug = false

     [openrouter]
     api_key = gsk_...          # ваш API-ключ

     [telegram]
     bot_token = <токен бота>
     chat_id = <id чата>

     [logging]
     log_dir = /var/log/epd/
     ```

   - **`ai_config.yaml`** – настройки AI и промты:
     ```yaml
     ai:
       provider: groq
       model: openai/gpt-oss-20b
       url: https://api.groq.com/openai/v1/chat/completions
       timeout: 120
       temperature: 0.7
       max_tokens: 2000

     prompts:
       analysis: |
         (ваш шаблон промта)
     ```

5. Инициализируйте базу данных:
   ```bash
   python3 init_db.py
   ```

6. Настройте правила фильтрации (один раз):
   Добавьте правила в таблицу `filter_rules` вручную.

## 🔧 Настройка getmail

Пример конфигурации `~/.getmail/getmailrc`:

```ini
[retriever]
type = SimpleIMAPSSLRetriever
server = imap.gmail.com
username = your_email@gmail.com
password = your_app_password

[destination]
type = MDA_external
path = /path/to/epd-system/getmail_filter.py
arguments = ("--stdout",)

[options]
verbose = 0
read_all = false
delete = false
```

## 🏃 Запуск

### Оркестратор (AI + Telegram)
Запускается периодически (например, через cron каждые 5 минут):
```bash
cd /path/to/epd-system
python3 orchestrator.py
```

### Веб-интерфейс
```bash
python3 frontend.py
```
По умолчанию доступен по адресу `http://localhost:5000`.

## 📊 Использование

1. **Получение писем** – getmail автоматически передаёт письма в `getmail_filter.py`.
2. **Парсинг** – письма, соответствующие правилам, сохраняются во временную папку и обрабатываются `mos_parser.py`.
3. **AI-анализ** – оркестратор находит необработанные периоды, формирует промт, отправляет запрос к LLM и сохраняет результат.
4. **Telegram** – после успешного AI-анализа создаётся сообщение и отправляется (с повторными попытками при ошибках).
5. **Веб-интерфейс** – позволяет просматривать историю, графики, детали AI-запросов, повторно запускать AI или отправку в Telegram.

## 🧪 Тестирование

Установите тестовые зависимости:
```bash
pip install -r requirements-test.txt
```

Запуск всех тестов:
```bash
pytest
```

С покрытием:
```bash
pytest --cov=. --cov-report=html
```

## 🚦 CI/CD

Проект содержит GitHub Actions workflow (`.github/workflows/tests.yml`), который автоматически запускает тесты при push/pull request в ветки `main`/`master`.

## 📁 Структура проекта

```
epd-system/
├── .github/workflows/tests.yml
├── config.ini
├── ai_config.yaml
├── requirements.txt
├── requirements-test.txt
├── README.md
├── init_db.py
├── frontend.py
├── orchestrator.py
├── getmail_filter.py
├── utils.py
├── parsers/
│   ├── base_parser.py
│   └── mos_parser.py
├── templates/
│   └── index.html
├── tests/
│   ├── conftest.py
│   ├── test_utils.py
│   ├── test_parsers.py
│   ├── test_orchestrator.py
│   ├── test_frontend.py
│   ├── test_getmail_filter.py
│   ├── test_db_init.py
│   └── pytest.ini
└── static/ (опционально)
```


## 📄 Лицензия

MIT
