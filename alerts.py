# -*- coding: utf-8 -*-
"""
Алерты владельцу в Телеграм из Python-части (vacancy_collector.py / llm_scorer.py /
run_all.py). Аналог worker/alerts.js для облачной части - но без троттлинга
(прогон раз в сутки, повтор одного и того же кода ошибки не страшен) и без
ретраев на отправку: если сломан сам Телеграм, повторная попытка сообщить об
этом через Телеграм зациклится на той же сетевой ошибке. Одна попытка, дальше
в лог. Fail-open: сбой самого алерта никогда не пробрасывается наружу.

Секретов в коде нет: TG_BOT_TOKEN/TG_CHAT_ID - из .env / Secrets, как везде.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN', '')
TG_CHAT_ID = os.getenv('TG_CHAT_ID', '')


def send_owner_alert(text):
    """Один POST в Телеграм владельцу, без ретраев. Любой сбой - строка в
    консоль/run.log, исключение наружу не выходит (fail-open - см. докстринг
    модуля). Возвращает True/False - вызывающий код МОЖЕТ проверить успех,
    но ни один текущий вызов (report_crash/check_collection_anomaly) этого
    не требует - раньше это была тишина: requests не бросает исключение на
    4xx/5xx сам по себе, поэтому HTTP 401/403/429/500 проходили как успех."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print(f'[алерт] TG_BOT_TOKEN/TG_CHAT_ID не заданы, алерт не отправлен: {text}')
        return False
    try:
        import requests
        r = requests.post(
            f'https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage',
            json={'chat_id': TG_CHAT_ID, 'text': text, 'disable_web_page_preview': True},
            timeout=15,
        )
    except Exception as e:
        print(f'[алерт] отправка в телеграм не удалась (сеть): {e}')
        return False
    try:
        body = r.json()
    except ValueError:
        body = {}
    if r.status_code != 200 or not body.get('ok'):
        desc = body.get('description') or f'HTTP {r.status_code}'
        print(f'[алерт] телеграм отказал: {desc}')
        return False
    return True


def report_crash(script_name, exc):
    """Алерт о падении прогона целиком: что упало и техническая причина
    (тип исключения + текст). Без содержимого данных и без user_id."""
    reason = f'{type(exc).__name__}: {exc}'
    send_owner_alert(f'🔥 {script_name}: прогон упал.\n{reason}')
