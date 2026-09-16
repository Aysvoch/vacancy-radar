# -*- coding: utf-8 -*-
"""
ЗВЕНО 2: LLM-оценка вакансий -> чистый лист "Вакансии" в "Таблице неудач".

Конвейер:
  читает сырьё из листов "Хабр"/"Телеграм" (файл-сборщик) ->
  по каждой НОВОЙ вакансии зовёт LLM (через OpenRouter) ->
  LLM извлекает поля + ставит оценку 0-10 + сухой вердикт ->
  пишет строку в лист "Вакансии" (в "Таблице неудач") с подсветкой ->
  шлёт в телеграм счётчик новых (если задан бот).

ЗАЩИТА ТАБЛИЦ (важно):
  - пишем ТОЛЬКО в лист "Вакансии". Трекер/Дашборд/Справочник не открываются на запись.
  - только дозапись новых строк (дедуп по source_id). Ничего не перезаписываем.
  - LLM не пишет в таблицу - он лишь возвращает JSON; строку формирует и пишет код.
  - кривой ответ LLM ловится в try/except: вакансия помечается, прогон не падает.

Значения Грейд/Формат/Опыт/Источник LLM выдаёт СТРОГО из твоего Справочника,
чтобы поля совпадали с выпадающими списками Трекера (для будущего переноса).

Секретов в коде нет: всё из .env.
"""

import os
import re
import html
import json
import time
import datetime as dt

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==========================================================================
# СЕКРЕТЫ / ПРИВЯЗКИ
# ==========================================================================
CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
RAW_SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '')            # файл-сборщик (сырьё)
TRACKER_SPREADSHEET_ID = os.getenv('TRACKER_SPREADSHEET_ID', '')  # "Таблица неудач"
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY', '')
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN', '')
TG_CHAT_ID = os.getenv('TG_CHAT_ID', '')
SUBSCRIBERS_API_SECRET = os.getenv('SUBSCRIBERS_API_SECRET', '')  # часть Б - рассылка аудитории

# ==========================================================================
# ПЕРЕКЛЮЧАТЕЛЬ МОДЕЛИ  (раскомментируй нужную строку)
# ==========================================================================
# openrouter/free - авто-роутер бесплатных моделей (лимит ~200/день, 429 при превышении).
# deepseek/deepseek-v4-flash - платная, дёшево (~центы), без жёстких лимитов - для полного прогона.
# LLM_MODEL = 'openrouter/free'                    # бесплатно (медленно, лимиты)
LLM_MODEL = 'deepseek/deepseek-v4-flash'           # ДЁШЕВО, без лимитов (нужен баланс) - по умолчанию
# LLM_MODEL = 'anthropic/claude-3.5-haiku'         # Claude, точнее (дороже)

# ==========================================================================
# НАСТРОЙКИ
# ==========================================================================
RAW_SHEETS = ['Хабр', 'Телеграм']       # откуда читаем сырьё
OUT_SHEET = 'Вакансии'                   # куда пишем чистое (в Таблице неудач)
SLEEP_LLM = 0.6                          # пауза между вызовами LLM
MAX_PER_RUN = 0                          # 0 = без лимита; >0 = не больше N за прогон (для теста)

# Отсечка по свежести: не оценивать вакансии старше N дней.
# 0 = не отсекать. Дата не распозналась -> НЕ отсекаем (лучше лишняя, чем потерянная).
MAX_AGE_DAYS = 10

# Отсечка по оценке: в лист попадают только score >= MIN_SCORE.
# Нераспознанный score (LLM вернул не число) -> НЕ отсекаем (лучше лишняя, чем потерянная).
MIN_SCORE = 3
NOTIFY_DETAIL_SCORE = 5   # >= этого идёт в бот (в таблицу пишется всё от MIN_SCORE=3, в бот только явно подходящее, чтобы не сыпать Senior/Middle)

# Страховочный потолок листа "Вакансии": если после подчистки старья строк
# всё ещё больше - обрезаем лишние снизу (см. cleanup_old_rows).
MAX_ROWS = 150

# ==========================================================================
# ЧАСТЬ Б - рассылка аудитории через телеграм-бот (воркер Cloudflare)
# ==========================================================================
# Не секрет - публичный URL воркера, как и остальные API-адреса в этом файле.
AUDIENCE_WORKER_URL = 'https://audience-bot.vakansiya-podehala.workers.dev'

# Столько же вакансий в рассылку, сколько Андрею (notify_top[:10]) - паритет,
# не простыня. Остальные кандидаты просто остаются непомеченными в журнале
# «Аудитория» и уйдут следующим прогоном - бэклог не теряется, просто не спешит.
AUDIENCE_BATCH_CAP = 10

# Запас от лимита Telegram (4096) под шапку и погрешность - при этой длине режем
# накопленный текст на отдельное сообщение, не разрывая вакансию посередине строки.
AUDIENCE_MSG_BUDGET = 3800

AUDIENCE_LOG_SHEET = 'Аудитория'              # служебный лист-журнал В СБОРЩИКЕ
AUDIENCE_LOG_COLUMNS = ['source_id', 'sent_at']

# Порог подчистки журнала - ЗАВЕДОМО больше MAX_AGE_DAYS, не впритык. Причина:
# запись здесь снята по sent_at (когда разослали), а сама вакансия в листе
# «Вакансии» снимается cleanup_old_rows по published (когда опубликована) -
# это разные даты, published <= sent_at (рассылаем не раньше публикации, часто
# позже - пока соберём, оценим). Если чистить журнал впритык к MAX_AGE_DAYS,
# при любой рассинхронизации (задержка сборщика, ручной перезапуск, кривая дата)
# можно снести запись о вакансии, которая в «Вакансии» ещё жива - already_sent_ids
# её не увидит, build_audience_candidates сочтёт новой, и она уйдёт подписчикам
# повторно. +7 дней - запас с большим отрывом от типичной задержки публикация->
# рассылка (обычно часы-сутки, не неделя), не тесно подобранное число.
AUDIENCE_LOG_MAX_AGE_DAYS = MAX_AGE_DAYS + 7

AUDIENCE_SEND_PAUSE = 0.05       # пауза между получателями - лимит Телеграма ~30/сек
AUDIENCE_SEND_RETRIES = 3        # сеть/5xx - столько попыток, потом пропуск получателя
AUDIENCE_SEND_RETRY_DELAYS = [2, 4]
AUDIENCE_RATE_LIMIT_RETRIES = 5  # 429 - ждём ровно retry_after, но не бесконечно

# ДОЛЖНО совпадать с MAX_REMOVE_PER_CALL в worker/src/config.js - воркер отвергнет
# пачку крупнее этого потолка (400 Bad Request), а не молча обрежет её сам.
WORKER_REMOVE_BATCH_CAP = 40

_MONTHS = {'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
           'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12}

def parse_date(s):
    """Понимает ISO (2026-08-12, телеграм) и '13 августа' (Хабр). None если не распознал."""
    s = (s or '').strip().lower()
    if not s:
        return None
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try:
            return dt.date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None
    m = re.match(r'(\d{1,2})\s+([а-яё]+)', s)
    if m and m.group(2) in _MONTHS:
        d, mon = int(m.group(1)), _MONTHS[m.group(2)]
        today = dt.date.today()
        try:
            cand = dt.date(today.year, mon, d)
        except ValueError:
            return None
        if cand > today:                 # дата "в будущем" -> это прошлый год
            cand = dt.date(today.year - 1, mon, d)
        return cand
    return None

def too_old(published):
    """True если вакансия старше MAX_AGE_DAYS. Нераспознанная дата -> False (не режем)."""
    if not MAX_AGE_DAYS:
        return False
    d = parse_date(published)
    if d is None:
        return False
    return (dt.date.today() - d).days > MAX_AGE_DAYS

# Бигтехи -> цвет строки (RGB 0..1). Подсветка в листе "Вакансии" - ОБЫЧНОЙ заливкой
# ячеек при записи строки (fill_row), НЕ условным правилом REGEXMATCH: Sheets отвергает
# REGEXMATCH с кириллицей внутри addConditionalFormatRule (APIError 400). Матчинг компании -
# по вхождению подстроки в нижнем регистре (Python, не формула). Цвета мягкие пастельные,
# чтобы не спорить со шкалой оценки (условное правило, красит только ячейку "Оценка").
BIG_TECH_COLORS = {
    'яндекс': (1.00, 0.9569, 0.80), 'yandex': (1.00, 0.9569, 0.80),        # #FFF4CC
    'vk': (0.8902, 0.9137, 0.9686),                                        # #E3E9F7
    'сбер': (0.8784, 0.9412, 0.8784), 'sber': (0.8784, 0.9412, 0.8784),    # #E0F0E0
    'альфа': (0.9686, 0.8784, 0.8784), 'alfa': (0.9686, 0.8784, 0.8784),   # #F7E0E0
    'мтс': (0.9686, 0.8784, 0.8784), 'mts': (0.9686, 0.8784, 0.8784),      # #F7E0E0
    'озон': (0.8784, 0.9294, 0.9686), 'ozon': (0.8784, 0.9294, 0.9686),    # #E0EDF7
    'т-банк': (1.00, 0.9765, 0.8392), 'тинькофф': (1.00, 0.9765, 0.8392),  # #FFF9D6
    't-bank': (1.00, 0.9765, 0.8392),
}

def color_for_company(company):
    """Первый совпавший по подстроке (нижний регистр) бигтех -> его RGB, иначе None."""
    c = (company or '').strip().lower()
    if not c:
        return None
    for key, rgb in BIG_TECH_COLORS.items():
        if key in c:
            return rgb
    return None

# Допустимые значения ИЗ ТВОЕГО СПРАВОЧНИКА - LLM выбирает только из них.
V_GRADE = ['Intern', 'Junior', 'Junior+', 'Middle', 'Senior', 'Lead', 'не указано']
V_EXP = ['Без опыта', '1-3 года', '3-6 лет', '6+ лет', 'не указано']
V_FORMAT = ['Офис', 'Удалёнка', 'Гибрид', 'не указано']

# Колонки листа "Вакансии". source_id - технический ключ дедупа (колонка A,
# скрыта в UI, но остаётся в данных). Просмотрено - чекбокс, последняя колонка,
# чтобы не сбивать чтение данных и дедуп по колонке A.
COLUMNS = ['source_id', 'Компания', 'Должность', 'Опыт (треб.)', 'Грейд',
           'Источник', 'Формат', 'ЗП вилка', 'Локация', 'Оценка',
           'Вердикт', 'Опубликовано', 'Ссылка', 'Комментарий', 'Просмотрено',
           'Версия промпта']

# ==========================================================================
# ВЕРСИЯ ПРОМПТА - увеличивай (v2, v3...) при каждой значимой правке
# SYSTEM_PROMPT, чтобы строки в листе "Вакансии" можно было отличить по
# версии оценки (см. колонку "Версия промпта" в COLUMNS).
# ==========================================================================
PROMPT_VERSION = 'v2'

# ==========================================================================
# ПРОФИЛЬ КАНДИДАТА для промпта (правь под себя)
# ==========================================================================
CANDIDATE_PROFILE = """Кандидат: переход в продакт-менеджмент из операционного/проектного
менеджмента (5+ лет). Учится на продакт-менеджера в Школе 21 (Сбер).
Ищет: junior / стажировку / позиции без опыта или до 1-3 лет в продукте.
Локация: любой город России или удалённо - подходит. Вакансии за пределами РФ (СНГ, зарубеж) без удалёнки - не подходят.
Цель - первая роль в продукте, не Senior/Lead."""

SYSTEM_PROMPT = f"""Ты - строгий ассистент по подбору вакансий. Оцениваешь вакансию
на соответствие профилю кандидата. Работай ТОЛЬКО с фактами из текста вакансии.
ЗАПРЕЩЕНО: выдумывать данные, приукрашивать, добавлять то, чего нет в тексте.
Если поля нет в тексте - ставь "не указано". Вердикт - сухой и конкретный,
без похвал и воды, 1 короткое предложение.

Профиль кандидата:
{CANDIDATE_PROFILE}

Верни СТРОГО JSON без пояснений и markdown, ровно эти ключи:
{{
  "is_vacancy": "true если это ОДНА конкретная вакансия; false если это подборка/
                дайджест из нескольких вакансий, инфопост, реклама или не вакансия",
  "company": "название компании или 'не указано'",
  "title": "должность или 'не указано'",
  "experience": "одно из: {V_EXP}",
  "grade": "одно из: {V_GRADE}",
  "format": "одно из: {V_FORMAT}",
  "salary": "вилка как в тексте (например '80000-120000 RUB') или 'не указано'",
  "location": "город/условия или 'не указано'",
  "score": "целое 0-10, насколько подходит кандидату (10=идеально, 0=совсем нет)",
  "verdict": "1 сухое предложение на русском языке почему такая оценка, максимум 25 слов"
}}
Оценку занижай для Senior/Lead, требований 5+ лет, нерелевантных доменов;
повышай для junior/intern/стажировок в продукте в России или с удалёнкой. Занижай за локацию только вне РФ без удалёнки.
Если роль по сути НЕ продуктовая (Project/Delivery/Account/Проектный/Аккаунт-менеджер,
продажи, страхование, доставка, торговый представитель и т.п.), даже если рядом
встречается слово "продукт" - ставь оценку не выше 3.
Побочные плюсы (крупная компания, удалёнка, зарплата) НЕ компенсируют несоответствие
типа роли профилю.
Если НЕ указаны ОБА - и грейд, и требуемый опыт - это НЕ повод для средней оценки:
неизвестность - недостаток информации, а не находка. Ставь не выше 4 и прямо пиши в
вердикте, что данных для точной оценки не хватает. Если известно хотя бы одно из
двух - правило не применяется, оценивай по существу."""

# JSON-схема ответа. OpenRouter направит запрос провайдеру, который её держит
# (см. provider=require_parameters в теле). Грейд/Формат/Опыт заданы enum'ом строго
# из Справочника - модель обязана вернуть одно из допустимых, pick() остаётся страховкой.
RESPONSE_FORMAT = {
    'type': 'json_schema',
    'json_schema': {
        'name': 'vacancy_eval',
        'strict': True,
        'schema': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'is_vacancy': {'type': 'boolean'},
                'company':    {'type': 'string'},
                'title':      {'type': 'string'},
                'experience': {'type': 'string', 'enum': V_EXP},
                'grade':      {'type': 'string', 'enum': V_GRADE},
                'format':     {'type': 'string', 'enum': V_FORMAT},
                'salary':     {'type': 'string'},
                'location':   {'type': 'string'},
                'score':      {'type': 'integer', 'minimum': 0, 'maximum': 10},
                'verdict':    {'type': 'string'},
            },
            'required': ['is_vacancy', 'company', 'title', 'experience', 'grade',
                         'format', 'salary', 'location', 'score', 'verdict'],
        },
    },
}

# ==========================================================================
# LLM через OpenRouter
# ==========================================================================
LLM_TRIES = 3                  # макс. попыток вызова OpenRouter, потом честно сдаёмся
LLM_RETRY_DELAYS = [2, 4]       # паузы между попытками (сек)

def truncate_for_llm(text, limit=4000, head=2500, tail=1500):
    """Длинный пост режем на начало+конец, а не просто [:limit]. Зарплата, опыт и
    ссылка часто в хвосте поста - [:limit] бы их потерял. Короткий текст - как есть."""
    text = text or ''
    if len(text) <= limit:
        return text
    return text[:head] + '\n\n[…текст сокращён…]\n\n' + text[-tail:]

def llm_evaluate(vacancy_text):
    """Возвращает (dict полей, None) при успехе или (None, причина) при неудаче
    после LLM_TRIES попыток. Ретраит сетевые ошибки/таймауты и 429/5xx."""
    body = {
        'model': LLM_MODEL,
        'messages': [
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': f'Текст вакансии:\n{truncate_for_llm(vacancy_text)}'},
        ],
        'temperature': 0,               # без фантазии - максимально детерминированно
        'max_tokens': 1200,             # было 800 - длинные JSON типа А обрывались
        'response_format': RESPONSE_FORMAT,        # форсим валидный JSON по схеме
        'provider': {'require_parameters': True},  # только провайдер, который держит схему
        # reasoning гасим: задача классификационная, «размышления» только жрут бюджет
        # токенов и текут в content вместо JSON. {'enabled': False} модель игнорирует
        # и рассуждения текут в content - используем {'exclude': True} (думает молча).
        'reasoning': {'exclude': True},
    }
    headers = {
        'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        'Content-Type': 'application/json',
        'X-Title': 'vacancy-llm',
    }
    reason = 'LLM ошибка'
    for attempt in range(1, LLM_TRIES + 1):
        retryable = True                 # ретраим только сеть/429/5xx; парс и 4xx - нет
        try:
            r = requests.post('https://openrouter.ai/api/v1/chat/completions',
                              json=body, headers=headers, timeout=60)
        except requests.exceptions.RequestException:
            reason = 'LLM таймаут/сеть'
        else:
            if r.status_code == 429 or 500 <= r.status_code < 600:
                reason = f'LLM HTTP {r.status_code}'          # временное - повтор уместен
            elif r.status_code != 200:
                # 4xx (напр. схема не поддержана провайдером) - повтор не поможет
                reason = f'LLM HTTP {r.status_code}'
                retryable = False
            else:
                try:
                    j = r.json()
                except ValueError:
                    reason = 'LLM битый ответ'; retryable = False
                else:
                    choices = j.get('choices') or []
                    msg = choices[0].get('message', {}) or {} if choices else {}
                    content = msg.get('content') or msg.get('reasoning') or ''
                    if not content:
                        reason = 'LLM пустой ответ'; retryable = False
                    else:
                        data = parse_llm_json(content)
                        if data is None:
                            # при temperature=0 повтор даст тот же битый ответ - не тратим попытки
                            reason = 'LLM битый JSON'; retryable = False
                        else:
                            time.sleep(SLEEP_LLM)
                            return data, None
        if not retryable:
            break
        if attempt < LLM_TRIES:
            delay = LLM_RETRY_DELAYS[attempt - 1]
            print(f'    [LLM] попытка {attempt}/{LLM_TRIES} не удалась ({reason}), '
                  f'жду {delay}с и повторяю...')
            time.sleep(delay)
    return None, reason

def parse_llm_json(content):
    """Достаёт ПЕРВЫЙ сбалансированный JSON-объект, даже если вокруг текст/markdown.
    None, если объекта нет или он оборван (незакрытая скобка = упор в max_tokens).
    Надёжнее жадного \\{.*\\}: тот брал от первой { до последней } и падал на обрыве."""
    if not content or not isinstance(content, str):
        return None
    s = content.strip()
    if s.startswith('```'):
        s = s.strip('`')
    start = s.find('{')
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(s)):
        c = s[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(s[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None  # скобка не закрылась -> ответ обрезан, подними max_tokens

# ==========================================================================
# Нормализация под Справочник (чтобы совпадало с выпадающими списками)
# ==========================================================================
def _norm(s):
    return str(s).strip().lower().replace('ё', 'е')

# синонимы -> каноничное значение Справочника (для формата/грейда)
_SYNONYMS = {
    'удаленка': 'Удалёнка', 'удаленно': 'Удалёнка', 'remote': 'Удалёнка',
    'можно удаленно': 'Удалёнка', 'офис': 'Офис', 'on-site': 'Офис', 'onsite': 'Офис',
    'гибрид': 'Гибрид', 'hybrid': 'Гибрид',
    'интерн': 'Intern', 'intern': 'Intern', 'стажер': 'Intern', 'стажёр': 'Intern',
    'джуниор': 'Junior', 'джун': 'Junior',
}

def pick(value, allowed, default='не указано'):
    if not value:
        return default
    nv = _norm(value)
    # прямое совпадение с допустимым значением (без учёта ё/регистра)
    for a in allowed:
        if nv == _norm(a):
            return a
    # синоним -> каноничное (если оно вообще в списке допустимых)
    if nv in _SYNONYMS and _SYNONYMS[nv] in allowed:
        return _SYNONYMS[nv]
    return default

# ==========================================================================
# Retry с экспоненциальной задержкой (для нестабильных внешних сервисов)
# ==========================================================================
def with_retry(fn, tries=5, base_delay=2, what="операция"):
    """До 5 попыток с паузами 2/4/8/16с. После последней - падаем с понятной ошибкой."""
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == tries:
                print(f"  [retry] {what}: не удалось после {tries} попыток: {e}")
                raise
            delay = base_delay * (2 ** (attempt - 1))
            print(f"  [retry] {what}: попытка {attempt} не удалась ({e}), "
                  f"жду {delay}с и повторяю...")
            time.sleep(delay)

# ==========================================================================
# Google Sheets
# ==========================================================================
def gc_client():
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        return gspread.authorize(creds)
    except Exception as e:
        raise SystemExit(
            f"Ошибка подключения к Google Sheets: {e}. "
            f"Проверь credentials.json и доступ к таблице."
        )

def open_raw(gc):
    """Открывает таблицу-сборщик. Отдельно от read_raw, чтобы то же соединение
    переиспользовать для журнала рассылки аудитории (notify_audience) - без
    второго open_by_key на ту же таблицу в том же прогоне."""
    return with_retry(lambda: gc.open_by_key(RAW_SPREADSHEET_ID),
                      what="открытие таблицы-сборщика")

def read_raw(ss):
    """Читает обе вкладки сырья из уже открытой таблицы-сборщика (см. open_raw),
    возвращает список (source_id, текст_для_LLM, дата, ссылка)."""
    items = []
    for name in RAW_SHEETS:
        try:
            ws = ss.worksheet(name)
        except gspread.WorksheetNotFound:
            continue
        rows = with_retry(lambda ws=ws, name=name: ws.get_all_records(),
                          what=f"чтение листа «{name}»")   # list of dict по заголовкам
        for row in rows:
            sid = str(row.get('source_id', '')).strip()
            if not sid:
                continue
            if name == 'Хабр':
                text = (f"{row.get('Должность','')} | {row.get('Компания','')} | "
                        f"{row.get('Локация','')} | грейд {row.get('Грейд','')} | "
                        f"вилка {row.get('Вилка','')}")
                src = 'Хабр'
            else:
                text = row.get('Текст', '')
                src = f"Telegram/{row.get('Канал','')}"
            items.append({
                'source_id': sid, 'text': text, 'src': src,
                'published': row.get('Опубликовано', ''),
                'url': row.get('Ссылка', ''),
            })
    return items

def open_out(gc):
    """Открывает/создаёт лист 'Вакансии' в Таблице неудач. Трекер и пр. НЕ трогаем."""
    ss = with_retry(lambda: gc.open_by_key(TRACKER_SPREADSHEET_ID),
                    what="открытие Таблицы неудач")
    try:
        ws = ss.worksheet(OUT_SHEET)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=OUT_SHEET, rows=2000, cols=len(COLUMNS))
    vals = with_retry(lambda: ws.get_all_values(), what="чтение листа «Вакансии»")
    if not vals or vals[0][:len(COLUMNS)] != COLUMNS:
        with_retry(lambda: ws.update([COLUMNS], 'A1'), what="запись заголовка")
        with_retry(lambda: ws.freeze(rows=1), what="закрепление шапки")
    return ws

def existing_ids(ws):
    vals = with_retry(lambda: ws.get_all_values(), what="чтение существующих ID")
    return {r[0] for r in vals[1:] if r and r[0]} if len(vals) > 1 else set()

# ==========================================================================
# Журнал рассылки аудитории (часть Б) - служебная механика, не то, на что
# смотрят глазами, поэтому живёт в таблице-сборщике, а не в Таблице неудач.
# Дедуп по source_id (одна лента на всех - персонализации под подписчика нет),
# НЕ по паре подписчик+вакансия: так проще и не требует правок в KV воркера.
# ==========================================================================
def open_audience_log(ss):
    """Открывает/создаёт лист 'Аудитория' в таблице-сборщике (ss - уже открытая
    open_raw()). Схема отдельная от 'Вакансии' - никак не задевает её колонки/
    форматирование."""
    try:
        ws = ss.worksheet(AUDIENCE_LOG_SHEET)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=AUDIENCE_LOG_SHEET, rows=1000, cols=len(AUDIENCE_LOG_COLUMNS))
        with_retry(lambda: ws.update([AUDIENCE_LOG_COLUMNS], 'A1'),
                  what="запись заголовка «Аудитория»")
    return ws

def already_sent_ids(ws):
    vals = with_retry(lambda: ws.get_all_values(), what="чтение журнала «Аудитория»")
    return {r[0] for r in vals[1:] if r and r[0]} if len(vals) > 1 else set()

def append_sent(ws, source_ids):
    # append_rows (серверный авто-детект диапазона) safe ТОЛЬКО пока в этом
    # листе нет скрытых колонок. В листе «Вакансии» ровно из-за скрытой колонки A
    # (source_id, hiddenByUser) авто-детект пропускал её и писал строки со сдвигом
    # на +1, молча ломая дедуп по колонке A (см. build_row/target_range выше -
    # там из-за этого записываем явным A{n}:O{n}, а не append_row). Если когда-нибудь
    # здесь тоже спрячут колонку - повторится тот же баг. Прячете колонку - переходите
    # на явный target_range, как в build_row, а не append_rows.
    if not source_ids:
        return
    now_iso = dt.datetime.now().isoformat(timespec='seconds')
    rows = [[sid, now_iso] for sid in source_ids]
    with_retry(lambda: ws.append_rows(rows, value_input_option='RAW'),
              what="запись в журнал «Аудитория»")

def cleanup_audience_log(ws):
    """Чистит лист 'Аудитория' от записей старше AUDIENCE_LOG_MAX_AGE_DAYS (по
    sent_at). По образцу cleanup_old_rows: собрать индексы старых строк, снести
    пачкой deleteDimension. Нераспознанная дата -> НЕ удаляем (лучше лишняя
    строка, чем потерянный дедуп - симметрично too_old())."""
    vals = with_retry(lambda: ws.get_all_values(), what="чтение журнала «Аудитория» перед подчисткой")
    if len(vals) <= 1:
        return
    cutoff = dt.date.today() - dt.timedelta(days=AUDIENCE_LOG_MAX_AGE_DAYS)
    to_delete = []
    for i, row in enumerate(vals[1:], start=2):
        raw = row[1] if len(row) > 1 else ''
        try:
            sent_date = dt.datetime.fromisoformat(raw).date()
        except ValueError:
            continue
        if sent_date < cutoff:
            to_delete.append(i)
    if not to_delete:
        return
    sid = ws.id
    reqs = [{'deleteDimension': {
        'range': {'sheetId': sid, 'dimension': 'ROWS',
                  'startIndex': rownum - 1, 'endIndex': rownum}}}
            for rownum in sorted(to_delete, reverse=True)]
    try:
        ws.spreadsheet.batch_update({'requests': reqs})
        print(f'  [аудитория] журнал: удалено старых записей (>{AUDIENCE_LOG_MAX_AGE_DAYS}д): {len(to_delete)}')
    except Exception as e:
        print(f'  [аудитория] журнал: не удалось подчистить: {e}')

def build_row(item, data):
    def g(key, default='не указано'):
        v = data.get(key)
        return v if v not in (None, '') else default
    company = g('company')
    grade = pick(data.get('grade'), V_GRADE)
    exp = pick(data.get('experience'), V_EXP)
    fmt = pick(data.get('format'), V_FORMAT)
    verdict = str(g('verdict', ''))
    if len(verdict) > 200:                       # ровные строки: длинный вердикт обрезаем
        verdict = verdict[:200].rstrip() + '…'
    return [
        item['source_id'], company, g('title'),
        exp, grade, item['src'], fmt, g('salary'), g('location'),
        g('score', ''), verdict,
        item['published'], item['url'], '', '',
        PROMPT_VERSION,
    ]

def col_to_letter(idx):
    """0-based индекс колонки -> буква(ы) Google Sheets (0->A, 25->Z, 26->AA, 27->AB...)."""
    letters = ''
    idx += 1
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(ord('A') + rem) + letters
    return letters

def fill_row(ws, rownum, rgb):
    """Обычная заливка всей строки (не условное правило) - подсветка бигтеха
    при записи строки. Условное правило "Просмотрено" (CUSTOM_FORMULA) рисуется
    ПОВЕРХ обычной заливки ячейки в Google Sheets, так что просмотренное
    само перекрывает эту заливку без дополнительной логики приоритета."""
    last = col_to_letter(len(COLUMNS) - 1)
    with_retry(lambda: ws.format(
        f'A{rownum}:{last}{rownum}',
        {'backgroundColor': {'red': rgb[0], 'green': rgb[1], 'blue': rgb[2]}}
    ), what="подсветка строки")

def style_sheet(ws, n_rows):
    """Оформление под Дашборд: тёмная шапка, ширины, wrap, закрепление, автофильтр,
    чекбокс "Просмотрено" и условные правила подсветки (шкала оценки, просмотрено).
    Зовётся каждый прогон, поэтому все настройки держатся даже после пересоздания
    листа с нуля. Подсветка компаний-бигтехов - НЕ здесь: она обычная заливка
    ячеек, ставится один раз при записи строки (см. fill_row/color_for_company)."""
    last = col_to_letter(len(COLUMNS) - 1)
    # тёмная шапка с белым жирным текстом (как заголовки Дашборда)
    ws.format(f'A1:{last}1', {
        'backgroundColor': {'red': 0.17, 'green': 0.24, 'blue': 0.31},
        'textFormat': {'bold': True,
                       'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        'horizontalAlignment': 'CENTER',
        'verticalAlignment': 'MIDDLE',
    })
    ws.freeze(rows=1)
    try:
        ws.set_basic_filter(f'A1:{last}{max(n_rows,1)+1}')
    except Exception:
        pass
    # ширины колонок под содержимое (через batch-запрос к Sheets API)
    # source_id, Компания, Должность, Опыт, Грейд, Источник, Формат, ЗП вилка,
    # Локация, Оценка, Вердикт, Опубликовано, Ссылка, Комментарий, Просмотрено,
    # Версия промпта
    widths = [80, 170, 170, 80, 80, 110, 90, 130, 150, 80, 320, 100, 100, 300, 90, 70]
    reqs = []
    sid = ws.id
    for i, w in enumerate(widths):
        if w:
            reqs.append({'updateDimensionProperties': {
                'range': {'sheetId': sid, 'dimension': 'COLUMNS',
                          'startIndex': i, 'endIndex': i + 1},
                'properties': {'pixelSize': w}, 'fields': 'pixelSize'}})
    # source_id остаётся в данных (дедуп), но скрыт от глаз в UI
    reqs.append({'updateDimensionProperties': {
        'range': {'sheetId': sid, 'dimension': 'COLUMNS',
                  'startIndex': COLUMNS.index('source_id'),
                  'endIndex': COLUMNS.index('source_id') + 1},
        'properties': {'hiddenByUser': True}, 'fields': 'hiddenByUser'}})
    # перенос текста + выравнивание вверх во всех колонках данных
    reqs.append({'repeatCell': {
        'range': {'sheetId': sid, 'startRowIndex': 1,
                  'startColumnIndex': 0, 'endColumnIndex': len(COLUMNS)},
        'cell': {'userEnteredFormat': {'wrapStrategy': 'WRAP', 'verticalAlignment': 'TOP'}},
        'fields': 'userEnteredFormat(wrapStrategy,verticalAlignment)'}})

    data_end_row = max(n_rows, 1) + 1     # 0-based exclusive, чуть шире текущих данных
    seen_idx = COLUMNS.index('Просмотрено')

    # Читаем метаданные листа: реальные размеры грида (чтобы стереть ЛЮБУЮ старую
    # data validation по всему листу, а не только в границах текущих данных) и
    # список текущих правил подсветки (чтобы не копить дубликаты с каждым прогоном).
    #
    # ИСТОЧНИК БАГА "лишний чекбокс P без заголовка": код всегда чистил старые
    # ПРАВИЛА ПОДСВЕТКИ (conditionalFormats) перед тем как поставить новые, но
    # ни разу не чистил ПРАВИЛА ВАЛИДАЦИИ (setDataValidation) - только добавлял
    # свежую BOOLEAN-валидацию на нужную колонку поверх. Любая validation-рамка,
    # когда-то повисшая на другой колонке (лист чистился очисткой содержимого,
    # а не пересозданием вкладки; либо более старая версия COLUMNS сдвигала
    # "Просмотрено" на другую букву), никогда не удалялась и рисовала свой
    # чекбокс вечно. Чинится ниже: сначала стираем validation по всему гриду,
    # потом ставим ровно одну - на COLUMNS.index('Просмотрено').
    clear_cols, clear_rows = len(COLUMNS), data_end_row
    try:
        meta = ws.spreadsheet.fetch_sheet_metadata(
            params={'fields': 'sheets(properties(sheetId,gridProperties),conditionalFormats)'})
        for sheet in meta.get('sheets', []):
            props = sheet.get('properties', {})
            if props.get('sheetId') != sid:
                continue
            gp = props.get('gridProperties', {})
            clear_cols = max(clear_cols, gp.get('columnCount', clear_cols))
            clear_rows = max(clear_rows, gp.get('rowCount', clear_rows))
            n_cf = len(sheet.get('conditionalFormats', []))
            for i in range(n_cf - 1, -1, -1):
                reqs.append({'deleteConditionalFormatRule': {'sheetId': sid, 'index': i}})
            break
    except Exception as e:
        print(f'  [оформление] не удалось прочитать метаданные листа: {e}')

    reqs.append({'setDataValidation': {
        'range': {'sheetId': sid, 'startRowIndex': 0, 'endRowIndex': clear_rows,
                  'startColumnIndex': 0, 'endColumnIndex': clear_cols},
        'rule': None}})
    # чекбокс РОВНО в колонке "Просмотрено" - ни шире, ни на соседнюю колонку
    reqs.append({'setDataValidation': {
        'range': {'sheetId': sid, 'startRowIndex': 1, 'endRowIndex': data_end_row,
                  'startColumnIndex': seen_idx, 'endColumnIndex': seen_idx + 1},
        'rule': {'condition': {'type': 'BOOLEAN'}, 'strict': True}}})

    # Две подсветки условными правилами (addConditionalFormatRule):
    #   1 (высший приоритет) - шкала "Оценка", красит только саму ячейку оценки;
    #   2 - просмотренное (чекбокс=TRUE) - вся строка бледно-зелёная.
    # Подсветка бигтеха - НЕ условное правило, а обычная заливка строки при записи
    # (fill_row, ставится в main() один раз при append_row). Условное форматирование
    # в Google Sheets рисуется ПОВЕРХ обычной заливки ячейки, поэтому зелёное
    # "просмотрено" само перекрывает заливку бигтеха - конфликта приоритетов нет.
    cf_index = [0]
    def add_cf(rng, condition, rgb):
        reqs.append({'addConditionalFormatRule': {
            'rule': {'ranges': [rng], 'booleanRule': {
                'condition': condition,
                'format': {'backgroundColor': {'red': rgb[0], 'green': rgb[1], 'blue': rgb[2]}}}},
            'index': cf_index[0]}})
        cf_index[0] += 1

    score_idx = COLUMNS.index('Оценка')
    score_range = {'sheetId': sid, 'startRowIndex': 1, 'endRowIndex': data_end_row,
                   'startColumnIndex': score_idx, 'endColumnIndex': score_idx + 1}
    for lo, hi, rgb in ((8, 10, (0.65, 0.85, 0.62)),
                        (6, 7, (0.85, 0.94, 0.80)),
                        (3, 5, (1.00, 0.95, 0.75))):
        add_cf(score_range,
              {'type': 'NUMBER_BETWEEN',
               'values': [{'userEnteredValue': str(lo)}, {'userEnteredValue': str(hi)}]},
              rgb)

    row_range = {'sheetId': sid, 'startRowIndex': 1, 'endRowIndex': data_end_row,
                'startColumnIndex': 0, 'endColumnIndex': len(COLUMNS)}
    seen_col = col_to_letter(seen_idx)
    add_cf(row_range,
          {'type': 'CUSTOM_FORMULA', 'values': [{'userEnteredValue': f'=${seen_col}2=TRUE'}]},
          (0.85, 0.94, 0.85))

    # сортировка: непросмотренные (чекбокс снят) сверху, внутри - по Оценке убыв.
    # Сортируем весь диапазон строк целиком -> Комментарий и подсветка едут со строкой.
    if n_rows > 1:
        reqs.append({'sortRange': {
            'range': {'sheetId': sid, 'startRowIndex': 1, 'endRowIndex': n_rows,
                      'startColumnIndex': 0, 'endColumnIndex': len(COLUMNS)},
            'sortSpecs': [
                {'dimensionIndex': seen_idx, 'sortOrder': 'ASCENDING'},
                {'dimensionIndex': COLUMNS.index('Оценка'), 'sortOrder': 'DESCENDING'},
            ]}})
    if reqs:
        try:
            ws.spreadsheet.batch_update({'requests': reqs})
        except Exception as e:
            print(f'  [оформление] часть стилей не применилась: {e}')

def cleanup_old_rows(ws, delivered_ids=frozenset()):
    """Разгрузка листа: удаляет строки старше MAX_AGE_DAYS (по 'Опубликовано'),
    затем страховочный cap - если данных всё ещё > MAX_ROWS, обрезает лишние.
    Нераспознанная дата -> НЕ удаляем. Источники сырья (Хабр/Телеграм) не
    трогаем - дедуп остаётся по source_id.

    delivered_ids - source_id, УЖЕ подтверждённые журналом «Аудитория» (см.
    возврат notify_audience() и её вызов в main()). Строка защищена от cap'а,
    если её оценка >= NOTIFY_DETAIL_SCORE и её source_id ещё НЕ в delivered_ids -
    то есть она ещё может дойти до подписчиков. Раньше cap резал строго по
    позиции после сортировки (непросмотренные-сверху/оценка-убыв), не зная про
    доставку - свежую, ещё не отправленную вакансию с оценкой ровно на пороге
    можно было срезать раньше, чем до неё дойдёт очередь в notify_audience
    (у неё свой лимит AUDIENCE_BATCH_CAP на прогон). Если защищённых строк
    больше, чем нужно удалить, чтобы дойти до MAX_ROWS, - cap в этот прогон
    не дотянется до предела, и это осознанно: лучше временно больше строк,
    чем потерянная доставка."""
    vals = with_retry(lambda: ws.get_all_values(), what="чтение листа перед подчисткой")
    if len(vals) <= 1:
        return
    pub_idx = COLUMNS.index('Опубликовано')
    to_delete = [i for i, row in enumerate(vals[1:], start=2)
                 if too_old(row[pub_idx] if len(row) > pub_idx else '')]
    sid = ws.id
    if to_delete:
        reqs = [{'deleteDimension': {
            'range': {'sheetId': sid, 'dimension': 'ROWS',
                      'startIndex': rownum - 1, 'endIndex': rownum}}}
                for rownum in sorted(to_delete, reverse=True)]
        try:
            ws.spreadsheet.batch_update({'requests': reqs})
            print(f'  [подчистка] удалено старых строк (>{MAX_AGE_DAYS}д): {len(to_delete)}')
        except Exception as e:
            print(f'  [подчистка] не удалось удалить старые строки: {e}')

    vals = with_retry(lambda: ws.get_all_values(), what="чтение листа после подчистки старья")
    sid_idx = COLUMNS.index('source_id')
    # ТОЛЬКО строки с непустым source_id - реальные данные. Лист растится
    # заранее пачками по 200 (см. add_rows в main()), поэтому get_all_values()
    # обычно возвращает и пустой хвост незаписанных строк грида - без этого
    # фильтра n_data мерил бы высоту листа, а не число вакансий, и cap мог бы
    # сработать вхолостую на пустоте, а не на реальном переполнении.
    data_rows = [(i, row) for i, row in enumerate(vals[1:], start=2)
                 if len(row) > sid_idx and row[sid_idx].strip()]
    n_data = len(data_rows)
    if n_data > MAX_ROWS:
        seen_idx = COLUMNS.index('Просмотрено')
        score_idx = COLUMNS.index('Оценка')

        def parse_score(row):
            try:
                return float(row[score_idx]) if len(row) > score_idx and row[score_idx] != '' else None
            except ValueError:
                return None

        deletable = []   # (rownum, seen_bool, score_or_neg_inf) - только незащищённые
        for i, row in data_rows:
            sid_val = row[sid_idx]
            score = parse_score(row)
            protected = score is not None and score >= NOTIFY_DETAIL_SCORE and sid_val not in delivered_ids
            if not protected:
                seen = row[seen_idx] if len(row) > seen_idx else ''
                deletable.append((i, seen == 'TRUE', score if score is not None else -1))

        protected_count = n_data - len(deletable)
        excess = n_data - MAX_ROWS
        # худшие сначала: просмотренные впереди непросмотренных, внутри - оценка по возрастанию
        deletable.sort(key=lambda t: (not t[1], t[2]))
        to_cap = [rownum for rownum, _, _ in deletable[:excess]]

        if to_cap:
            reqs = [{'deleteDimension': {
                'range': {'sheetId': sid, 'dimension': 'ROWS',
                          'startIndex': rownum - 1, 'endIndex': rownum}}}
                    for rownum in sorted(to_cap, reverse=True)]
            try:
                ws.spreadsheet.batch_update({'requests': reqs})
                note = f' (защищено от cap: {protected_count})' if protected_count else ''
                print(f'  [подчистка] cap {MAX_ROWS}: удалено лишних строк: {len(to_cap)}{note}')
            except Exception as e:
                print(f'  [подчистка] cap не применился: {e}')
        else:
            print(f'  [подчистка] cap {MAX_ROWS}: строк {n_data}, но все {protected_count} '
                  f'защищены (ещё не доставлены) - не режу')

# ==========================================================================
# ДОБОР ОПИСАНИЯ ДЛЯ ХАБРА - когда LLM вернула И грейд, И опыт как "не указано"
# ==========================================================================
# Причина: в LLM для Хабра уходит только "Должность | Компания | Локация |
# грейд | вилка" (см. read_raw) - текст требований не собирается, и модель
# честно упирается в потолок из-за правила "грейд и/или опыт не указаны".
# Здесь - вторая попытка: идём на страницу вакансии, забираем описание,
# зовём тот же LLM ещё раз, перезаписываем строку. Отдельная фаза ПОСЛЕ
# основной записи листа, под своим try - падение добора не должно ронять
# cleanup_old_rows/style_sheet (см. вызов в main()).
ENRICH_SOURCE = 'Хабр'
ENRICH_VERSION_LABEL = 'v2+desc'      # защита от повторного добора той же строки
ENRICH_MAX_PER_RUN = 25               # не добрали - доберём в следующий прогон
ENRICH_FETCH_TIMEOUT = 30
ENRICH_FETCH_PAUSE = 0.4              # пауза между запросами к career.habr.com
HABR_DESC_SELECTOR = '.vacancy-description__text'
ENRICH_HEADERS = {'User-Agent': 'Mozilla/5.0 (job-search-personal; vacancy-radar-enrich)'}

def fetch_habr_description(url):
    """Возвращает (текст, None) при успехе или (None, причина) при неудаче.
    Причины делятся на 2 категории для лога добора: страница не открылась
    (сеть/таймаут/статус не 200) и селектор пуст/не найден (страница 200,
    но .vacancy-description__text не дал текста - нетиповая карточка)."""
    try:
        r = requests.get(url, headers=ENRICH_HEADERS, timeout=ENRICH_FETCH_TIMEOUT)
    except requests.exceptions.RequestException as e:
        return None, f'страница не открылась ({e})'
    if r.status_code != 200:
        return None, f'страница не открылась (HTTP {r.status_code})'
    soup = BeautifulSoup(r.text, 'lxml')
    el = soup.select_one(HABR_DESC_SELECTOR)
    text = el.get_text('\n', strip=True) if el else ''
    if not text:
        return None, 'селектор пуст'
    return text, None

def enrich_habr_descriptions(ws, today_rows):
    """Добор описания для строк Хабра с неизвестными грейдом И опытом.
    today_rows - {rownum: {...}} строк, записанных В ЭТОМ прогоне (из main()) -
    если добор попадает в одну из них, обновляет её данные на месте, чтобы
    notify_top ниже пересобрался с учётом добора (см. вызов в main())."""
    vals = with_retry(lambda: ws.get_all_values(), what="чтение листа «Вакансии» для добора")
    if len(vals) <= 1:
        print('  [добор] лист пуст - нечего добирать')
        return

    src_idx = COLUMNS.index('Источник')
    grade_idx = COLUMNS.index('Грейд')
    exp_idx = COLUMNS.index('Опыт (треб.)')
    ver_idx = COLUMNS.index('Версия промпта')
    company_idx = COLUMNS.index('Компания')
    title_idx = COLUMNS.index('Должность')
    location_idx = COLUMNS.index('Локация')
    url_idx = COLUMNS.index('Ссылка')
    score_idx = COLUMNS.index('Оценка')
    comment_idx = COLUMNS.index('Комментарий')
    seen_idx = COLUMNS.index('Просмотрено')
    pub_idx = COLUMNS.index('Опубликовано')

    # Собираем ВСЕХ подходящих кандидатов (без раннего обрыва по лимиту) и
    # сортируем по дате публикации, свежие первыми, ПЕРЕД тем как резать по
    # ENRICH_MAX_PER_RUN. Без этого строка, у которой добор стабильно не
    # удаётся (метка v2+desc не ставится при неудаче - это правильно), могла
    # бы вечно занимать место в начале очереди и блокировать остальных при
    # переполнении лимита. Сортировка по дате решает это без счётчика попыток:
    # неудачник со временем сползает вниз относительно более свежих строк и
    # через MAX_AGE_DAYS уходит обычной чисткой (cleanup_old_rows). Строки с
    # нераспознанной датой - в конец очереди, не в начало (parse_date -> None
    # не должен получать приоритет).
    dated, undated = [], []
    for i, row in enumerate(vals[1:], start=2):
        if len(row) <= ver_idx:
            continue
        if row[src_idx] != ENRICH_SOURCE:
            continue
        if row[ver_idx] == ENRICH_VERSION_LABEL:
            continue
        if row[grade_idx].strip() != 'не указано' or row[exp_idx].strip() != 'не указано':
            continue
        d = parse_date(row[pub_idx] if len(row) > pub_idx else '')
        if d is None:
            undated.append((i, row))
        else:
            dated.append((d, i, row))
    dated.sort(key=lambda t: t[0], reverse=True)
    candidates = [(i, row) for _, i, row in dated] + undated
    candidates = candidates[:ENRICH_MAX_PER_RUN]

    if not candidates:
        print('  [добор] нечего добирать')
        return
    print(f'  [добор] кандидатов: {len(candidates)}')

    last_col = col_to_letter(len(COLUMNS) - 1)
    ok = 0
    fail = 0
    fail_reasons = {}          # 4 категории для итоговой строки
    is_vacancy_false_count = 0

    for rownum, row in candidates:
        url = row[url_idx] if len(row) > url_idx else ''
        title = row[title_idx] if len(row) > title_idx else ''
        company = row[company_idx] if len(row) > company_idx else ''
        location = row[location_idx] if len(row) > location_idx else ''
        old_score = row[score_idx] if len(row) > score_idx else ''

        if not url:
            fail += 1
            fail_reasons['страница не открылась (нет ссылки)'] = \
                fail_reasons.get('страница не открылась (нет ссылки)', 0) + 1
            continue

        desc, reason = fetch_habr_description(url)
        time.sleep(ENRICH_FETCH_PAUSE)
        if desc is None:
            fail += 1
            fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
            continue

        text = f"{title} | {company} | {location}\n\n{desc}"
        data, llm_reason = llm_evaluate(text)
        if not data:
            fail += 1
            bucket = 'ответ LLM не распарсился' if llm_reason == 'LLM битый JSON' \
                else 'LLM не ответила после ретраев'
            fail_reasons[bucket] = fail_reasons.get(bucket, 0) + 1
            continue

        is_vac = str(data.get('is_vacancy', 'true')).strip().lower()
        if is_vac in ('false', 'нет', '0', 'no'):
            fail += 1
            is_vacancy_false_count += 1
            print(f'    [добор][внимание] {url}: LLM сочла страницу НЕ единичной '
                  f'вакансией (is_vacancy=false) - проверь, что селектор тянет '
                  f'описание, а не что-то ещё')
            continue

        item = {'source_id': row[0], 'src': ENRICH_SOURCE,
                'published': row[COLUMNS.index('Опубликовано')], 'url': url}
        new_row = build_row(item, data)
        new_row[ver_idx] = ENRICH_VERSION_LABEL
        # сохраняем ручные поля существующей строки - build_row пишет для них ''
        # (корректно для НОВОЙ строки, но здесь мы перезаписываем существующую)
        new_row[comment_idx] = row[comment_idx] if len(row) > comment_idx else ''
        new_row[seen_idx] = row[seen_idx] if len(row) > seen_idx else ''

        target_range = f'A{rownum}:{last_col}{rownum}'
        with_retry(lambda row=new_row, rng=target_range: ws.update(
            [row], rng, value_input_option='USER_ENTERED'),
                  what="перезапись строки после добора")

        new_score = new_row[score_idx]
        print(f'    [добор] было {old_score} -> стало {new_score}, {title} / {company}')
        ok += 1

        if rownum in today_rows:
            try:
                score_num = float(new_score)
            except (TypeError, ValueError):
                score_num = None
            today_rows[rownum].update({
                'company': str(data.get('company') or ''),
                'title': str(data.get('title') or 'вакансия'),
                'format': str(data.get('format') or ''),
                'location': str(data.get('location') or ''),
                'score': score_num,
            })

    reasons_txt = ', '.join(f'{k}: {v}' for k, v in fail_reasons.items()) or '-'
    print(f'  [добор] успешно {ok}, не удалось {fail} из {len(candidates)} '
          f'(причины: {reasons_txt})')
    if is_vacancy_false_count:
        print(f'  [добор][внимание] is_vacancy=false у {is_vacancy_false_count} - '
              f'возможно, селектор {HABR_DESC_SELECTOR} съезжает не туда')

# ==========================================================================
# Телеграм-счётчик
# ==========================================================================
# ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ (не баг, не чинить вслепую): notify_count зовётся ОДИН
# раз, после того как весь цикл по todo в main() уже отработал, а запись строки
# в лист происходит раньше, чем пополнение today_rows (из которого потом
# собирается notify_top). Если процесс упадёт посередине цикла - часть
# вакансий уже физически в листе (и больше никогда не попадёт в todo/
# today_rows на будущих прогонах), но notify_count для них в
# этом прогоне не вызывался -> личное уведомление по ним теряется навсегда,
# сама вакансия в листе остаётся видна. Сценарий редкий, осознанно не лечим:
# таблица - основной канал, уведомление - удобство, а лечение требует переделки
# уже работающего кода. У рассылки аудитории (notify_audience) от этого же
# сценария есть защита - персистентный журнал «Аудитория», это разные механизмы.
def notify_count(n, top=None):
    """Возвращает True/False - успех отправки. Не бросает исключение (main()
    зовёт её без try/except, в отличие от notify_audience - падение здесь не
    должно ронять прогон, лист уже записан, см. ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ выше).
    До правки requests.post не проверялся вовсе: HTTP 401/403/429/500
    проходили как успех - requests не бросает исключение на 4xx/5xx сам."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return False
    top = top or []
    if top:
        lines = [f'🎯 Вакансии product manager: {len(top)}']
        for v in top:
            title = v['title'] if len(v['title']) <= 40 else v['title'][:40].rstrip() + '…'
            parts = [p for p in (v['company'], v['format'], v['location'])
                     if p and p != 'не указано']
            tail = ' · '.join(parts)
            link = f'<a href=\"{html.escape(v["url"], quote=True)}\">{html.escape(title)}</a>'
            line = f'• {link}' + (f' · {html.escape(tail)}' if tail else '')
            lines.append(line)
        text = '\n'.join(lines)
    else:
        text = 'Пу-пу-пуу, пока тишина 🤷'
    try:
        r = requests.post(f'https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage',
                      json={'chat_id': TG_CHAT_ID, 'text': text,
                            'parse_mode': 'HTML', 'disable_web_page_preview': True},
                      timeout=30)
    except Exception as e:
        print(f'  [телеграм] не отправлено (сеть): {e}')
        return False
    try:
        body = r.json()
    except ValueError:
        body = {}
    if r.status_code != 200 or not body.get('ok'):
        desc = body.get('description') or f'HTTP {r.status_code}'
        print(f'  [телеграм] не отправлено: {desc}')
        return False
    return True

# ==========================================================================
# Часть Б - рассылка аудитории (подписчики бота, список берём у воркера)
# ==========================================================================
def build_audience_candidates(ws, already_sent):
    """Читает лист 'Вакансии' целиком, возвращает кандидатов на рассылку:
    score >= NOTIFY_DETAIL_SCORE, ещё не в журнале, не старше MAX_AGE_DAYS.
    Читает ВЕСЬ лист (не только сегодняшнее добавленное) - так подхватывается
    и бэклог с прошлого упавшего/пропущенного прогона, не только новое."""
    rows = with_retry(lambda: ws.get_all_records(), what="чтение листа «Вакансии» для аудитории")
    candidates = []
    for row in rows:
        sid = str(row.get('source_id', '')).strip()
        if not sid or sid in already_sent:
            continue
        if too_old(row.get('Опубликовано', '')):
            continue
        try:
            score = float(row.get('Оценка'))
        except (TypeError, ValueError):
            continue
        if score < NOTIFY_DETAIL_SCORE:
            continue
        candidates.append({
            'source_id': sid,
            'company': str(row.get('Компания') or ''),
            'title': str(row.get('Должность') or 'вакансия'),
            'format': str(row.get('Формат') or ''),
            'location': str(row.get('Локация') or ''),
            'url': str(row.get('Ссылка') or ''),
            'score': score,
        })
    candidates.sort(key=lambda v: v['score'], reverse=True)
    return candidates

def audience_vacancy_line(v):
    title = v['title'] if len(v['title']) <= 40 else v['title'][:40].rstrip() + '…'
    parts = [p for p in (v['company'], v['format'], v['location'])
             if p and p != 'не указано']
    tail = ' · '.join(parts)
    link = f'<a href=\"{html.escape(v["url"], quote=True)}\">{html.escape(title)}</a>'
    return f'• {link}' + (f' · {html.escape(tail)}' if tail else '')

def build_audience_messages(candidates):
    """Список вакансий -> список текстов сообщений, каждый <= AUDIENCE_MSG_BUDGET
    символов. Режем по границам вакансий, не разрывая строку посередине."""
    header = f'🎯 Вакансии product manager: {len(candidates)}'
    lines = [header] + [audience_vacancy_line(v) for v in candidates]
    messages, current, current_len = [], [], 0
    for line in lines:
        piece_len = len(line) + (1 if current else 0)   # +1 за '\n', если не первая в чанке
        if current and current_len + piece_len > AUDIENCE_MSG_BUDGET:
            messages.append('\n'.join(current))
            current, current_len, piece_len = [], 0, len(line)
        current.append(line)
        current_len += piece_len
    if current:
        messages.append('\n'.join(current))
    return messages

def fetch_active_subscribers():
    """GET /subscribers у воркера. None - не удалось получить список (сеть,
    неверный секрет и т.п.), рассылку в этот раз лучше не делать вовсе, чем
    разослать пустоте или упасть посередине."""
    url = f'{AUDIENCE_WORKER_URL}/subscribers'
    headers = {'X-Subscribers-Secret': SUBSCRIBERS_API_SECRET}
    def call():
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()
    try:
        data = with_retry(call, what="список подписчиков (воркер)")
    except Exception as e:
        print(f'  [аудитория] не удалось получить список подписчиков: {e}')
        return None
    ids = data.get('user_ids') if isinstance(data, dict) else None
    if not isinstance(ids, list):
        print('  [аудитория] неожиданный формат ответа воркера')
        return None
    return [str(i) for i in ids]

def report_blocked_subscribers(chat_ids):
    """Лучшее усилие: сообщает воркеру, кого можно вычистить из KV (403/chat not
    found при реальной отправке). Не критично для уже состоявшейся рассылки -
    без with_retry: если не получилось, воркер продолжит натыкаться на тот же
    403 у тех же адресатов и в следующий раз, ничего не теряется, просто отложится.
    Пачками по WORKER_REMOVE_BATCH_CAP - ровно потолок, который держит воркер."""
    if not chat_ids:
        return
    url = f'{AUDIENCE_WORKER_URL}/subscribers/remove'
    headers = {'X-Subscribers-Secret': SUBSCRIBERS_API_SECRET, 'Content-Type': 'application/json'}
    for i in range(0, len(chat_ids), WORKER_REMOVE_BATCH_CAP):
        batch = chat_ids[i:i + WORKER_REMOVE_BATCH_CAP]
        try:
            r = requests.post(url, headers=headers, json={'user_ids': batch}, timeout=30)
            if r.status_code == 200:
                removed = (r.json() or {}).get('removed', '?')
                print(f'  [аудитория] воркер вычистил заблокировавших: {removed}')
            else:
                print(f'  [аудитория] воркер отказал в чистке заблокировавших ({r.status_code})')
        except Exception as e:
            print(f'  [аудитория] не удалось сообщить воркеру о блокировках: {e}')

def send_audience_message(chat_id, text):
    """Отправляет один текст одному получателю.
    'ok' - доставлено; 'blocked' - 403 или 400 chat not found (заблокировал бота/
    удалил аккаунт - подписчика надо чистить); 'failed' - сеть/5xx/прочее после
    всех попыток, этого получателя просто пропускаем в этот раз."""
    network_attempts = 0
    rate_limit_attempts = 0
    while True:
        try:
            r = requests.post(
                f'https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage',
                json={'chat_id': chat_id, 'text': text,
                      'parse_mode': 'HTML', 'disable_web_page_preview': True},
                timeout=30)
        except requests.exceptions.RequestException:
            network_attempts += 1
            if network_attempts > AUDIENCE_SEND_RETRIES:
                return 'failed'
            time.sleep(AUDIENCE_SEND_RETRY_DELAYS[min(network_attempts, len(AUDIENCE_SEND_RETRY_DELAYS)) - 1])
            continue

        if r.status_code == 200:
            return 'ok'
        if r.status_code == 403:
            return 'blocked'
        if r.status_code == 400:
            desc = ''
            try:
                desc = (r.json() or {}).get('description', '')
            except ValueError:
                pass
            return 'blocked' if 'chat not found' in desc.lower() else 'failed'
        if r.status_code == 429:
            rate_limit_attempts += 1
            if rate_limit_attempts > AUDIENCE_RATE_LIMIT_RETRIES:
                return 'failed'
            retry_after = 1
            try:
                retry_after = (r.json() or {}).get('parameters', {}).get('retry_after', 1)
            except ValueError:
                pass
            time.sleep(retry_after)
            continue
        # 5xx и прочее неучтённое - тот же путь, что сеть: ретрай с паузой
        network_attempts += 1
        if network_attempts > AUDIENCE_SEND_RETRIES:
            return 'failed'
        time.sleep(AUDIENCE_SEND_RETRY_DELAYS[min(network_attempts, len(AUDIENCE_SEND_RETRY_DELAYS)) - 1])

def notify_audience(gc, ss_raw, ws_vacancies):
    """Рассылка той же ленты подписчикам бота. Падение здесь НЕ должно ронять
    остальной пайплайн (лист уже записан, Андрею уже отправлено) - поэтому вся
    функция под одним try в вызывающем коде, а не только отдельные отправки.

    Возвращает актуальный набор source_id, УЖЕ подтверждённых журналом
    «Аудитория» (включая только что отправленные в этом прогоне) - его
    переиспользует cleanup_old_rows(), чтобы не открывать таблицу-сборщик
    второй раз ради того же журнала (см. её вызов в main())."""
    if not SUBSCRIBERS_API_SECRET:
        return set()   # часть Б не настроена (секрет ещё не залит) - тихо пропускаем

    ws_log = open_audience_log(ss_raw)
    cleanup_audience_log(ws_log)
    sent_ids = already_sent_ids(ws_log)
    candidates = build_audience_candidates(ws_vacancies, sent_ids)[:AUDIENCE_BATCH_CAP]
    if not candidates:
        return sent_ids

    subscribers = fetch_active_subscribers()
    if subscribers is None:
        print('  [аудитория] откладываю рассылку - список подписчиков недоступен')
        return sent_ids

    messages = build_audience_messages(candidates)
    blocked, sent_count = [], 0
    for chat_id in subscribers:
        ok = True
        for text in messages:
            status = send_audience_message(chat_id, text)
            if status == 'blocked':
                blocked.append(chat_id)
                ok = False
                break
            if status == 'failed':
                ok = False
                break
            time.sleep(AUDIENCE_SEND_PAUSE)
        if ok:
            sent_count += 1

    print(f'  [аудитория] разослано {sent_count}/{len(subscribers)} подписчикам, '
          f'вакансий в пачке: {len(candidates)}')

    # Пометить как отправленное можно, только если пачка реально до кого-то дошла,
    # либо подписчиков просто не было (рассылать некому - пачка тоже "выполнена").
    # Если подписчики БЫЛИ, а дошло до 0 - похоже на системный сбой (не тот
    # BOT_TOKEN и т.п.): лучше повторить всю пачку следующим прогоном, чем
    # тихо списать вакансию в архив, до которой реально никто не дошёл.
    if subscribers and sent_count == 0:
        print('  [аудитория] ни один получатель не подтверждён - не помечаю как отправленное, повтор в следующий прогон')
    else:
        append_sent(ws_log, [c['source_id'] for c in candidates])
        sent_ids = sent_ids | {c['source_id'] for c in candidates}

    if blocked:
        report_blocked_subscribers(blocked)

    return sent_ids

# ==========================================================================
# MAIN
# ==========================================================================
def main():
    for name, val in [('SPREADSHEET_ID', RAW_SPREADSHEET_ID),
                      ('TRACKER_SPREADSHEET_ID', TRACKER_SPREADSHEET_ID),
                      ('OPENROUTER_API_KEY', OPENROUTER_API_KEY)]:
        if not val:
            raise SystemExit(f'{name} не задан в .env')

    gc = gc_client()
    print('Читаю сырьё...')
    ss_raw = open_raw(gc)
    items = read_raw(ss_raw)
    ws = open_out(gc)
    have = existing_ids(ws)
    todo = [it for it in items
            if it['source_id'] not in have and not too_old(it['published'])]
    skipped_old = sum(1 for it in items
                      if it['source_id'] not in have and too_old(it['published']))
    if MAX_PER_RUN:
        todo = todo[:MAX_PER_RUN]
    print(f'  всего в сырье: {len(items)}, уже оценено: {len(have)}, '
          f'старше {MAX_AGE_DAYS}д пропущено: {skipped_old}, к оценке: {len(todo)}')

    num = len(have)
    added = 0
    sheet_rows = ws.row_count   # кэш текущего размера листа для авто-расширения
    today_rows = {}    # rownum -> данные для notify_top; заполняется на КАЖДУЮ
                        # записанную сегодня строку, не только прошедшие порог -
                        # добор ниже может поднять score выше NOTIFY_DETAIL_SCORE
    skipped_non_vacancy = 0     # LLM ответил, но это подборка/инфопост/реклама
    skipped_low_score = 0       # реальная вакансия, но score < MIN_SCORE - не мой профиль
    llm_errors = 0              # не ответил/битый JSON после всех попыток - это НЕ мусор
    for it in todo:
        data, fail_reason = llm_evaluate(it['text'])
        if not data:
            # сбой LLM (не «мусор») - НЕ пишем, вернётся на повтор в след. прогон
            llm_errors += 1
            print(f'    пропуск ({fail_reason}): {it["source_id"]}')
            continue
        # не одиночная вакансия (дайджест/инфопост/реклама) - не засоряем лист
        is_vac = str(data.get('is_vacancy', 'true')).strip().lower()
        if is_vac in ('false', 'нет', '0', 'no'):
            skipped_non_vacancy += 1
            continue
        # низкая оценка (< MIN_SCORE) - не засоряем лист. Нераспознанный score -> не отсекаем.
        try:
            score_num = float(data.get('score'))
        except (TypeError, ValueError):
            score_num = None
        if score_num is not None and score_num < MIN_SCORE:
            skipped_low_score += 1
            continue
        num += 1
        row = build_row(it, data)
        # explicit A{n}:O{n} вместо append_row: серверный авто-детект таблицы
        # у append_row пропускает скрытую колонку A (hiddenByUser) и съезжает
        # на B, сдвигая весь row на +1 (source_id мимо A, дедуп по r[0] ломается).
        last_col = col_to_letter(len(COLUMNS) - 1)
        # авто-расширение: гарантируем, что в листе хватает строк под запись в num+1
        # (лист могли ужать вручную -> ws.update за границей грида падал с 'exceeds grid limits')
        if num + 1 > sheet_rows:
            need = num + 1 + 200   # растим пачкой с запасом, чтобы не дёргать API каждую строку
            try:
                with_retry(lambda add=need - sheet_rows: ws.add_rows(add),
                           what="расширение листа под запись")
                sheet_rows = need
            except Exception as e:
                print(f'  [лист] не удалось расширить ({e}), пробую записать как есть')
        target_range = f'A{num + 1}:{last_col}{num + 1}'
        with_retry(lambda row=row, rng=target_range: ws.update(
            [row], rng, value_input_option='USER_ENTERED'),
                  what="запись строки в лист")
        rgb = color_for_company(row[COLUMNS.index('Компания')])
        if rgb:
            fill_row(ws, num + 1, rgb)     # num+1: заголовок row1 + num дозаписанных строк
        added += 1
        print(f'  [{num}] {str(data.get("score","?")):>2}/10 | {str(data.get("company") or "")[:18]:18} | {str(data.get("title") or "")[:40]}')
        today_rows[num + 1] = {
            'company': str(data.get('company') or ''),
            'title': str(data.get('title') or 'вакансия'),
            'format': str(data.get('format') or ''),
            'location': str(data.get('location') or ''),
            'url': it['url'],
            'score': score_num,
        }

    print(f'  итог по прогону: записано {added}, не вакансия {skipped_non_vacancy}, '
          f'низкий score {skipped_low_score}, ошибки LLM {llm_errors}')

    try:
        enrich_habr_descriptions(ws, today_rows)
    except Exception as e:
        print(f'  [добор] фаза не выполнена: {e}')

    notify_top = sorted(
        (v for v in today_rows.values()
         if v['score'] is not None and v['score'] >= NOTIFY_DETAIL_SCORE),
        key=lambda v: v['score'], reverse=True)
    notify_count(added, notify_top[:10])
    delivered_ids = set()
    try:
        delivered_ids = notify_audience(gc, ss_raw, ws) or set()
    except Exception as e:
        print(f'  [аудитория] рассылка не выполнена: {e}')
    cleanup_old_rows(ws, delivered_ids)
    total_rows = len(with_retry(lambda: ws.get_all_values(), what="чтение листа перед оформлением"))
    style_sheet(ws, total_rows)
    print(f'\nЗвено 2 готово. Оценено и записано: {added}. Прогон: {dt.date.today().isoformat()}')
    print(f'Модель: {LLM_MODEL}')

if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        if e.code not in (0, None):
            from alerts import report_crash
            report_crash('llm_scorer.py', e)
        raise
    except Exception as e:
        from alerts import report_crash
        report_crash('llm_scorer.py', e)
        raise
