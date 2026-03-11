# EPD Analyzer

Система автоматического сбора, анализа и визуализации данных Единого платежного документа (ЕПД) за жилищно-коммунальные услуги.

## Возможности

- Автоматический парсинг входящих писем с ЕПД от портала mos.ru (через `getmail` + фильтр)
- Извлечение структурированных данных: лицевой счёт, адрес, период, перечень услуг, объёмы потребления, тарифы, суммы
- Сохранение истории начислений в базе данных SQLite
- Анализ с помощью AI (OpenRouter, Ollama, Groq и другие OpenAI-совместимые API) — сравнение с предыдущим месяцем, динамика за 12 месяцев, структура расходов
- Отправка отчёта в Telegram после обработки каждого письма
- Веб-интерфейс для просмотра и анализа данных: графики, детализация, выбор периода и услуг, переключение месяца
- Адаптивная вёрстка для мобильных устройств

## Архитектура

Проект состоит из следующих компонентов:

1. **Парсер email (`email_parser.py`)** — извлекает данные из HTML/plain-text писем, нормализует названия услуг, сохраняет в БД, инициирует AI-анализ и отправляет уведомление в Telegram.
2. **Анализатор AI (`ai_analyzer.py`)** — собирает агрегированные данные по адресу за месяц и за предыдущие периоды, формирует промпт и вызывает внешний AI API (OpenRouter / Ollama / Groq). Результат сохраняется в БД.
3. **Веб-интерфейс (`frontend.py` + шаблоны)** — Flask-приложение с графиками Chart.js и таблицей детализации. Позволяет выбирать адрес, лицевые счета, период, услуги, просматривать анализ AI.
4. **Фильтр для getmail (`getmail_filter.py`)** — скрипт, вызываемый getmail для каждого письма. Проверяет отправителя, получателя и тему, копирует подходящие письма во временный каталог и запускает `email_parser.py`.
5. **Утилита для Telegram (`telegram_notifier.py`)** — функция отправки сообщений с поддержкой Markdown-разметки.
6. **База данных SQLite** — хранит счета, периоды, услуги, начисления, дополнительную информацию по периодам, результаты анализов.

## Требования

- Python 3.8+
- Установленные пакеты (см. `requirements.txt`)
- Доступ к API OpenRouter / Ollama / Groq (опционально)
- Telegram-бот для уведомлений
- `getmail` для получения почты

## Установка

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/TSergeymsk/epd-service.git
   cd epd-analyzer
   ```

2. Создайте виртуальное окружение и активируйте его:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

4. Скопируйте пример конфигурации и отредактируйте:
   ```bash
   cp config.ini.example config.ini
   nano config.ini
   ```
   Заполните все необходимые параметры (токены, пути, шаблоны фильтрации).

5. Инициализируйте базу данных:
   ```bash
   python3 init_db.py
   ```

## Конфигурация

Файл `config.ini` содержит следующие секции:

```ini
[paths]
import_dir = /путь/к/папке/для/входящих/pdf      # не используется, можно оставить
archive_dir = /путь/к/архиву/pdf                 # не используется
db_path = /путь/к/epd.db
lock_file = /tmp/epd.lock
email_temp_dir = /путь/к/временной/папке/для/писем   # сюда фильтр сохраняет письма перед обработкой

[frontend]
port = 5000
static_dir = static
debug = false

[openrouter]
api_key = ваш_ключ_или_local_ollama
model = название_модели (например, glm-4.7-flash:latest)
url = http://localhost:11434/v1/chat/completions   # или https://openrouter.ai/api/v1/chat/completions
timeout = 180

[telegram]
bot_token = токен_вашего_бота
chat_id = ваш_chat_id

[logging]
log_dir = /путь/к/папке/с/логами

[getmail_filter]
from_pattern = uslugi@mos.ru
to_pattern = ваш_email@example.com
subject_pattern = Единый платежный документ
```

## Запуск компонентов

### Веб-интерфейс (вручную)
```bash
python3 frontend.py
```
Сервер будет доступен по адресу `http://localhost:5000`.

Для автозапуска через systemd (пользовательский сервис) см. раздел **Systemd**.

### Обработка писем через getmail

Настройте `getmail` так, чтобы каждое письмо передавалось в фильтр. Пример конфигурации `~/.getmail/getmailrc`:

```ini
[retriever]
type = SimplePOP3Retriever
server = pop.gmail.com
username = ваш_email@example.com
password = пароль

[destination]
type = Maildir
path = ~/Maildir/

[filter]
type = Filter_command
command = /путь/к/epd-analyzer/getmail_filter.py
```

Убедитесь, что скрипт `getmail_filter.py` исполняемый:
```bash
chmod +x /путь/к/epd-analyzer/getmail_filter.py
```

После этого каждое новое письмо будет проверяться на соответствие шаблонам из `[getmail_filter]`. Подходящие письма сохраняются во временную папку (`email_temp_dir`), обрабатываются парсером, а затем временный файл удаляется (в случае успеха).

### Анализ AI (ручной запуск)
```bash
python3 ai_analyzer.py --limit 5   # обработать 5 самых новых месяцев без анализа
python3 ai_analyzer.py              # обработать все недостающие
```
Обычно запуск происходит автоматически из `email_parser.py` после каждого нового письма, поэтому ручной запуск требуется только для первичного заполнения.

## Структура проекта

```
epd-analyzer/
├── ai_analyzer.py          # модуль AI-анализа
├── config.ini.example       # пример конфигурации
├── email_parser.py          # основной парсер писем
├── frontend.py              # веб-сервер Flask
├── getmail_filter.py        # фильтр для getmail
├── init_db.py               # инициализация БД
├── telegram_notifier.py     # утилита отправки в Telegram
├── requirements.txt         # зависимости
├── templates/               # HTML-шаблоны Flask
│   └── index.html           # главная страница
├── static/                  # статические файлы (пусто)
└── README.md                # этот файл
```

## Systemd (автозапуск фронтенда)

Создайте пользовательский юнит `~/.config/systemd/user/epd-frontend.service`:

```ini
[Unit]
Description=EPD Frontend
After=network.target

[Service]
Type=simple
WorkingDirectory=/путь/к/epd-analyzer
ExecStart=/путь/к/venv/bin/python3 /путь/к/epd-analyzer/frontend.py
Restart=on-failure
RestartSec=10
StandardOutput=append:/путь/к/логам/frontend.log
StandardError=append:/путь/к/логам/frontend.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
```

Затем выполните:
```bash
systemctl --user daemon-reload
systemctl --user enable epd-frontend.service
systemctl --user start epd-frontend.service
```

Для автозапуска при загрузке системы (даже без входа пользователя) включите linger:
```bash
sudo loginctl enable-linger ваш_пользователь
```

## Logrotate

Создайте файл `/etc/logrotate.d/epd` (если логи в `/var/log/epd`) или `/etc/logrotate.d/epd-user` (если в домашней папке) с содержимым:

```
/путь/к/папке/с/логами/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    dateext
    dateformat -%Y%m%d
}
```

## Примечания

- Все конфиденциальные данные (email, токены) должны быть заменены на реальные в `config.ini`.
- Для работы с OpenRouter требуется API-ключ и модель; для локальной Ollama — запущенный сервер.
- В шаблонах getmail_filter используются простые подстроки (вхождение). При необходимости можно заменить на регулярные выражения.
- При ошибках AI-анализа сообщение в Telegram всё равно отправляется, но блок анализа заменяется на "Анализ не проведен".
- Для корректной работы с getmail убедитесь, что пути в конфиге абсолютные и права доступа правильные.

## Лицензия

MIT# epd-service
Service to parse EPD from Moscow government service, store them in the DB, make AI analysis and send via Telegram
