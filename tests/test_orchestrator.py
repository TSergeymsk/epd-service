"""Тесты для orchestrator: постановка задач, отправка VK, вызов GigaChat."""
import sys

import pytest

import orchestrator


# ============================================================
# create_messages_for_successful_llm
# ============================================================

def _insert_llm_success(db, address, period_id, enqueued=0, response='ok'):
    db.execute("""
        INSERT INTO llm_requests
            (address, period_id, model, status, response_text, notifications_enqueued)
        VALUES (?, ?, 'GigaChat-2', 'success', ?, ?)
    """, (address, period_id, response, enqueued))
    db.commit()


def test_create_messages_only_for_new_success(db, ini_config, mock_logger,
                                              mocker, no_sleep):
    """Обрабатываются только success с notifications_enqueued=0."""
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mocker.patch.object(
        orchestrator, 'generate_telegram_message_for_period',
        return_value='📊 Анализ ЕПД\n...',
    )

    _insert_llm_success(db, 'new_addr', 1, enqueued=0)
    _insert_llm_success(db, 'old_addr', 2, enqueued=1)  # исторический, не должен попасть

    orchestrator.create_messages_for_successful_llm(db)

    rows = db.execute(
        "SELECT address, channel FROM telegram_messages ORDER BY address, channel"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]['address'] == 'new_addr'
    assert rows[1]['address'] == 'new_addr'

    # Old_addr не должна появиться
    assert all(r['address'] != 'old_addr' for r in rows)


def test_create_messages_marks_enqueued(db, ini_config, mock_logger,
                                         mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mocker.patch.object(
        orchestrator, 'generate_telegram_message_for_period',
        return_value='📊 Анализ ЕПД\n...',
    )

    _insert_llm_success(db, 'a', 1, enqueued=0)
    orchestrator.create_messages_for_successful_llm(db)

    flag = db.execute(
        "SELECT notifications_enqueued FROM llm_requests WHERE address='a'"
    ).fetchone()[0]
    assert flag == 1


def test_create_messages_skips_already_enqueued(db, ini_config, mock_logger,
                                                mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mocker.patch.object(
        orchestrator, 'generate_telegram_message_for_period',
        return_value='📊 Анализ ЕПД\n...',
    )

    _insert_llm_success(db, 'a', 1, enqueued=1)
    orchestrator.create_messages_for_successful_llm(db)

    count = db.execute("SELECT COUNT(*) FROM telegram_messages").fetchone()[0]
    assert count == 0


def test_create_messages_ignores_non_success(db, ini_config, mock_logger,
                                             mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mocker.patch.object(
        orchestrator, 'generate_telegram_message_for_period',
        return_value='📊 Анализ ЕПД\n...',
    )

    db.execute("""
        INSERT INTO llm_requests (address, period_id, model, status, response_text)
        VALUES ('a', 1, 'm', 'pending', NULL)
    """)
    db.commit()

    orchestrator.create_messages_for_successful_llm(db)
    count = db.execute("SELECT COUNT(*) FROM telegram_messages").fetchone()[0]
    assert count == 0


# ============================================================
# process_vk_messages
# ============================================================

def _seed_pending_vk(db, address='a', period_id=1, text='📊 Анализ ЕПД\nhello'):
    db.execute("""
        INSERT INTO telegram_messages
            (address, period_id, message_text, status, channel)
        VALUES (?, ?, ?, 'pending', 'vk')
    """, (address, period_id, text))
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def test_process_vk_uses_data_not_params(db, ini_config, mock_logger,
                                         mocker, no_sleep):
    """Главное исправление: параметры уходят в теле (data=), а не в URL (params=)."""
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)

    mock_resp = mocker.MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {'response': 42}
    mock_post = mocker.patch.object(orchestrator.requests, 'post',
                                    return_value=mock_resp)

    _seed_pending_vk(db)
    orchestrator.process_vk_messages(db)

    assert mock_post.call_count == 1
    _, kwargs = mock_post.call_args
    # data= должен присутствовать, params= — нет
    assert 'data' in kwargs
    assert kwargs.get('params') is None
    # Ключевые поля — в data
    assert kwargs['data']['peer_id'] == '123456'
    assert kwargs['data']['message'].startswith('📊 Анализ ЕПД')
    assert kwargs['data']['access_token'] == 'test_vk_token'
    assert kwargs['data']['v'] == '5.131'
    # random_id не должен быть 0
    assert kwargs['data']['random_id'] != 0

    row = db.execute("SELECT status, attempts FROM telegram_messages").fetchone()
    assert row['status'] == 'sent'
    assert row['attempts'] == 1


def test_process_vk_random_id_unique(db, ini_config, mock_logger,
                                     mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)

    mock_resp = mocker.MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {'response': 1}
    mock_post = mocker.patch.object(orchestrator.requests, 'post',
                                    return_value=mock_resp)

    _seed_pending_vk(db, address='a1')
    _seed_pending_vk(db, address='a2')
    _seed_pending_vk(db, address='a3')
    orchestrator.process_vk_messages(db)

    random_ids = [call.kwargs['data']['random_id'] for call in mock_post.call_args_list]
    assert len(random_ids) == 3
    assert len(set(random_ids)) == 3  # все разные


def test_process_vk_handles_api_error(db, ini_config, mock_logger,
                                      mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)

    mock_resp = mocker.MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {
        'error': {'error_code': 901, 'error_msg': "Can't send messages"}
    }
    mocker.patch.object(orchestrator.requests, 'post', return_value=mock_resp)

    _seed_pending_vk(db)
    orchestrator.process_vk_messages(db)

    row = db.execute("SELECT status, attempts, last_error FROM telegram_messages").fetchone()
    assert row['status'] == 'failed'
    assert row['attempts'] == 1
    assert '901' in row['last_error']


def test_process_vk_skipped_when_disabled(db, ini_config, mock_logger,
                                          mocker, no_sleep):
    ini_config.set('notifications', 'vk_enabled', 'false')
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mock_post = mocker.patch.object(orchestrator.requests, 'post')

    _seed_pending_vk(db)
    orchestrator.process_vk_messages(db)

    assert mock_post.call_count == 0
    row = db.execute("SELECT status FROM telegram_messages").fetchone()
    assert row['status'] == 'pending'  # не тронута


def test_process_vk_truncates_long_message(db, ini_config, mock_logger,
                                           mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)

    mock_resp = mocker.MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {'response': 1}
    mock_post = mocker.patch.object(orchestrator.requests, 'post',
                                    return_value=mock_resp)

    long_text = '📊 Анализ ЕПД\n' + ('x' * 5000)
    _seed_pending_vk(db, text=long_text)
    orchestrator.process_vk_messages(db)

    sent_text = mock_post.call_args.kwargs['data']['message']
    assert len(sent_text) <= orchestrator.VK_MAX_LEN


# ============================================================
# process_telegram_messages
# ============================================================

def test_process_telegram_uses_json_payload(db, ini_config, mock_logger,
                                            mocker, no_sleep):
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)

    mock_resp = mocker.MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_post = mocker.patch.object(orchestrator.requests, 'post',
                                    return_value=mock_resp)

    db.execute("""
        INSERT INTO telegram_messages (address, period_id, message_text, status, channel)
        VALUES ('a', 1, '📊 Анализ ЕПД\nhello', 'pending', 'telegram')
    """)
    db.commit()

    orchestrator.process_telegram_messages(db)

    assert mock_post.call_count == 1
    _, kwargs = mock_post.call_args
    assert 'json' in kwargs
    assert kwargs['json']['chat_id'] == 'test_chat_id'


# ============================================================
# call_gigachat_api
# ============================================================

def test_call_gigachat_api_success(ini_config, mock_logger, mocker,
                                   fake_gigachat_module):
    """Проверяем корректное извлечение текста из ответа SDK."""
    _, _, FakeGigaChat = fake_gigachat_module

    # Готовим фейковый ответ SDK
    answer_content = mocker.MagicMock()
    answer_content.text = 'Hello from GigaChat'

    answer_message = mocker.MagicMock()
    answer_message.content = [answer_content]

    choice = mocker.MagicMock()
    choice.message = answer_message

    response = mocker.MagicMock()
    response.choices = [choice]
    response.messages = None
    response.usage = mocker.MagicMock()
    response.usage.total_tokens = 42

    # Патчим конструктор клиента, чтобы вернуть готовый ответ из chat()
    original_init = FakeGigaChat.__init__

    def patched_init(self, **kwargs):
        original_init(self, **kwargs)
        self.chat.return_value = response

    mocker.patch.object(FakeGigaChat, '__init__', patched_init)

    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mocker.patch.object(orchestrator, 'AI_CONFIG', {
        'ai': {'provider': 'gigachat', 'model': 'GigaChat-2',
               'timeout': 30, 'temperature': 0.5, 'max_tokens': 500},
    })

    text, tokens = orchestrator.call_gigachat_api('prompt text', retries=1)
    assert text == 'Hello from GigaChat'
    assert tokens == 42


def test_call_gigachat_api_returns_none_on_import_error(mocker):
    """Если gigachat не установлен, функция возвращает (None, None), не падает."""
    # Удаляем возможный настоящий/фейковый gigachat
    mocker.patch.dict(sys.modules, {'gigachat': None})
    mocker.patch.object(orchestrator, 'logger', mocker.MagicMock())

    text, tokens = orchestrator.call_gigachat_api('x', retries=1)
    assert text is None
    assert tokens is None


def test_call_gigachat_api_uses_ca_bundle_file(ini_config, mock_logger,
                                               mocker, fake_gigachat_module,
                                               tmp_path):
    """ca_bundle_path передаётся в конструктор как ca_bundle_file."""
    _, _, FakeGigaChat = fake_gigachat_module

    ca_file = tmp_path / 'ca.pem'
    ca_file.write_text('fake cert')

    ini_config.set('ssl', 'ca_bundle_path', str(ca_file))

    captured = {}

    original_init = FakeGigaChat.__init__

    def patched_init(self, **kwargs):
        captured.update(kwargs)
        original_init(self, **kwargs)
        self.chat.return_value = mocker.MagicMock(
            choices=None, messages=mocker.MagicMock(), usage=None
        )

    mocker.patch.object(FakeGigaChat, '__init__', patched_init)
    mocker.patch.object(orchestrator, 'INI_CONFIG', ini_config)
    mocker.patch.object(orchestrator, 'logger', mock_logger)
    mocker.patch.object(orchestrator, 'AI_CONFIG', {
        'ai': {'provider': 'gigachat', 'model': 'GigaChat-2',
               'timeout': 30, 'temperature': 0.5, 'max_tokens': 500},
    })

    orchestrator.call_gigachat_api('x', retries=1)

    assert captured.get('ca_bundle_file') == str(ca_file)