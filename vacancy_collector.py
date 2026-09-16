# -*- coding: utf-8 -*-
"""
Сборщик вакансий -> Google Sheets. ЗВЕНО 1 (без LLM).
Автор конфигурации: задаётся в .env (AUTHOR).

Архитектура - модульная (плагинная):
  ИСТОЧНИКИ (фидеры) -> общий формат RawVacancy -> запись в Google Sheets.
Каждый источник - отдельная функция-фидер, возвращающая список RawVacancy.
Чтобы добавить источник - см. раздел "РЕЕСТР ИСТОЧНИКОВ" ниже.

Два листа сырья:
  "Хабр"     - структурные поля (грейд/рейтинг/вилка/локация).
  "Телеграм" - сырой текст поста + ссылка (поля извлечёт LLM в звене 2).
Колонки llm_оценка / llm_вердикт заведены пустыми - их заполнит звено 2.

Дедуп по source_id. Ручная колонка "Комментарии" не перезаписывается.
Телеграм: листание вглубь канала останавливается по возрасту поста
(SOURCE_MAX_AGE_DAYS), не по признаку "первого прогона" - см.
feed_telegram_channel.

Секретов в коде нет: .env локально / Secrets в облаке.
"""

import os
import re
import time
import datetime as dt
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

from collection_monitor import check_collection_anomaly

SOURCE_MAX_AGE_DAYS = 12   # сырьё старше стольки дней удаляем из листов-сборщиков

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ==========================================================================
# СЕКРЕТЫ / ПРИВЯЗКИ - из окружения
# ==========================================================================
AUTHOR = os.getenv('AUTHOR', 'job-search-personal')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '')
CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE', 'credentials.json')
TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN', '')   # для уведомлений (опц., звено 2)
TG_CHAT_ID = os.getenv('TG_CHAT_ID', '')

# ==========================================================================
# НАСТРОЙКИ (не секреты) - меняй тут
# ==========================================================================

# --- Хабр: поисковые запросы (на двух языках) ---
# Без фильтра по городам/remote: раньше искали только Казань+Москва+удалёнку,
# но профиль давно "вся Россия + удалёнка" (см. промпт LLM звена 2) - фильтр
# по локациям сужал выдачу в разы, а рассинхрон с профилем занижал сбор.
# Географию теперь отсеивает LLM, как и раньше делала для Телеграма.
HABR_QUERIES = ['product manager', 'product owner', 'менеджер продукта',
                'продакт', 'delivery manager', 'проджект']
HABR_MAX_PAGES = 5                     # сколько страниц на запрос листать

# --- Телеграм: каналы. Добавить источник = дописать строку. ---
TG_CHANNELS = [
    'yuniorapp', 'workayte', 'bigtechjobs', 'futru_it', 'itshka_v_sbere',
    'young_june', 'habr_career', 'juno_jobs', 'juniors_rabota_jobs',
    'proglib_jobs', 'young_intern', 'enter_career', 'IUCareerFinder',
    'forproducts', 'product_jobs', 'hireproproduct', 'jobs_for_products',
    'hh_vacancy_product_project', 'productconsult', 'jobforjunior',
    'remotejun', 'grinternru', 'workvc', 'hsecareer', 'huggabletalents',
    'ya_jobs_pm', 'digital_hr', 'geekjobs', 'mnogovakansiy', 'jobtalker',
    'cozy_hr', 'xCareers', 'evacuatejobs', 'alfadigital_jobs',
    'changellenge', 'offervacancy', 'avito_career', 'futru',
]
# Убраны как нечитаемые через t.me/s/ (это чаты/группы, не каналы-вещатели):
#   rabotaICL, products_jobs — смотреть вручную.
#   jobstobefoundJobs, mtsfintechjobs — unavailable (0 постов, не читаются через t.me/s/).
# Глубина чтения телеграма: страницы листаются назад, пока не встретится
# страница без единого поста моложе SOURCE_MAX_AGE_DAYS (см.
# feed_telegram_channel) - обычно 1-3 страницы на активный канал, 1 на
# неактивный. TG_DEEP_PAGES - страховочный потолок страниц на канал (той
# же природы, что HABR_MAX_PAGES для Хабра), а не основной критерий
# глубины - предохранитель на случай, если проверка "страница вся
# старая" почему-то не сработает.
TG_DEEP_PAGES = 8

SHEET_HABR = 'Хабр'
SHEET_TG = 'Телеграм'
SLEEP = 0.4               # пауза между сетевыми запросами (вежливость к серверам)

# --- ПРЕДФИЛЬТР: широкий менеджерский продуктовый список ---
# Запись проходит в таблицу, только если содержит хотя бы один маркер.
# Грубый отсев мусора (Java/SRE/реклама) ДО LLM: экономит лимиты и деньги.
# Тонкий отбор под профиль делает LLM (звено 2). Список можно расширять.
# 'проджект'/'project manager' включены как менеджерские - LLM понизит их
# под твой продуктовый профиль, но на входе не теряем пограничное.
PRODUCT_KEYWORDS = [
    'product manager', 'product owner', 'product lead', 'head of product',
    'chief product', 'cpo', 'продакт', 'продукт-менеджер', 'продуктовый менеджер',
    'менеджер продукта', 'менеджер по продукту', 'владелец продукта',
    'delivery manager', 'growth product', 'project manager', 'проджект',
]
# Допускаем дефис/тире между словами фразы ('Project - Manager' тоже матчится).
_PRODUCT_RE = re.compile('|'.join(
    r'[\s\-]+'.join(re.escape(p) for p in k.split(' ')) for k in PRODUCT_KEYWORDS), re.I)

def is_product(*texts):
    return bool(_PRODUCT_RE.search(' '.join(t for t in texts if t)))

# --- Чистка телеграм-текста: срезаем навигацию канала и рекламу ---
TG_TEXT_MAXLEN = 1500
_TG_PROMO_MARKS = ['ai ассистент', 'первый ai', 'софи -', 'erid', 'промокод']

def clean_tg_text(text):
    lines = []
    for line in text.split('\n'):
        l = line.strip()
        if '│' in l:                                  # навигация канала
            continue
        if any(m in l.lower() for m in _TG_PROMO_MARKS):  # рекламные строки
            continue
        lines.append(l)
    res = []
    for l in lines:                                   # схлопываем пустые строки
        if l == '' and (not res or res[-1] == ''):
            continue
        res.append(l)
    t = '\n'.join(res).strip()
    return t[:TG_TEXT_MAXLEN] + ' …' if len(t) > TG_TEXT_MAXLEN else t

_ua = AUTHOR.encode('ascii', 'ignore').decode('ascii').strip() or 'personal'
HEADERS = {'User-Agent': f'Mozilla/5.0 (job-search-personal; {_ua})'}

# ==========================================================================
# ОБЩИЙ ФОРМАТ СЫРЬЯ
# ==========================================================================
@dataclass
class RawVacancy:
    source: str            # 'habr' | 'tg:<channel>'
    source_id: str         # уникальный ID для дедупа
    title: str = ''        # у ТГ пусто - весь текст в raw_text
    company: str = ''
    salary: str = ''
    location: str = ''     # город + формат
    grade: str = ''        # Intern/Junior/... (Хабр)
    rating: str = ''       # рейтинг компании (Хабр)
    published: str = ''
    url: str = ''
    raw_text: str = ''     # полный текст (ТГ)
    channel: str = ''      # для ТГ

# ==========================================================================
# ФИДЕР: ХАБР КАРЬЕРА  (структурный парсинг career.habr.com)
# ==========================================================================
GRADES = {'Intern', 'Junior', 'Middle', 'Senior', 'Lead'}

def _habr_card_to_raw(card):
    tl = card.select_one('.vacancy-card__title-link')
    if not tl:
        return None
    url = tl.get('href', '')
    if url.startswith('/'):
        url = 'https://career.habr.com' + url
    sid = re.search(r'/vacancies/(\d+)', url)
    sid = sid.group(1) if sid else url
    # Имя компании - ТОЛЬКО из ссылки на профиль компании (рейтинг вложен в тот же
    # блок .vacancy-card__company, поэтому get_text по всему блоку его захватывает).
    comp_link = card.select_one('.vacancy-card__company a[href*="/companies/"]')
    company = comp_link.get_text(strip=True) if comp_link else ''
    # Рейтинг - из вложенного блока; берём первое число вида 4.55.
    rating_el = card.select_one('.vacancy-card__company-rating')
    rating = ''
    if rating_el:
        m = re.search(r'\d[.,]\d+', rating_el.get_text(' ', strip=True))
        rating = m.group(0) if m else ''
    date = card.select_one('.vacancy-card__date')
    sal = card.select_one('.vacancy-card__salary')
    meta = card.select_one('.vacancy-meta')
    parts = [p.strip() for p in meta.get_text('|', strip=True).split('|')] if meta else []
    grade = next((p for p in parts if p in GRADES), '')
    loc = ' / '.join(p for p in parts if p not in GRADES)
    sal_txt = ''
    if sal:
        sal_txt = sal.get_text(' ', strip=True).split('Похожие')[0].strip()
    return RawVacancy(
        source='habr', source_id=f'habr:{sid}',
        title=tl.get_text(strip=True),
        company=company,
        salary=sal_txt,
        location=loc,
        grade=grade,
        rating=rating,
        published=date.get_text(strip=True) if date else '',
        url=url,
    )

def feed_habr():
    """Фидер Хабра: все запросы x страницы, дедуп по source_id внутри фидера.
    Возвращает (список прошедших предфильтр, число карточек ДО предфильтра -
    для детектора аномалий сбора, см. collection_monitor.py)."""
    found = {}
    seen_cards = set()   # все распарсенные карточки, независимо от is_product
    base = 'https://career.habr.com/vacancies'
    for q in HABR_QUERIES:
        for page in range(1, HABR_MAX_PAGES + 1):
            params = {'q': q, 'type': 'all', 'page': page}
            url = f'{base}?{requests.compat.urlencode(params)}'
            try:
                r = requests.get(url, headers=HEADERS, timeout=30)
                time.sleep(SLEEP)
                if r.status_code != 200:
                    break
                soup = BeautifulSoup(r.text, 'lxml')
                cards = soup.select('div.vacancy-card')
                if not cards:
                    break
                for c in cards:
                    rv = _habr_card_to_raw(c)
                    if not rv:
                        continue
                    seen_cards.add(rv.source_id)
                    if is_product(rv.title):   # cut: только продуктовое
                        found[rv.source_id] = rv
            except Exception as e:
                print(f'  [Хабр] ошибка на "{q}" стр.{page}: {e}')
                break
    return list(found.values()), len(seen_cards)

# ==========================================================================
# ФИДЕР: ТЕЛЕГРАМ  (t.me/s/<channel>, сырой текст поста)
# ==========================================================================
def _tg_page(channel, before=None):
    url = f'https://t.me/s/{channel}'
    if before:
        url += f'?before={before}'
    r = requests.get(url, headers=HEADERS, timeout=30)
    time.sleep(SLEEP)
    return r

def feed_telegram_channel(channel):
    """
    Возвращает (список RawVacancy, статус_доступности, число постов ДО
    предфильтра, число постов с нераспознанной датой - первое для
    детектора аномалий сбора, см. collection_monitor.py).
    статус: 'ok' | 'empty' | 'unavailable' (чат/приват/опечатка).

    Отсечка по возрасту (SOURCE_MAX_AGE_DAYS) применяется уже здесь, той
    же константой, что и cleanup_source - иначе между "уже не собираем"
    и "ещё не удалили" появляется дыра: карусель постоянной дозаписи и
    подчистки одних и тех же старых постов (см. диагноз - без отсечки
    здесь TG_DEEP_PAGES страниц листались на КАЖДОМ прогоне, а не только
    на условном "первом"). SOURCE_MAX_AGE_DAYS=0 (как и в cleanup_source,
    см. её докстринг) означает "не отсекать" - без этой проверки ноль
    здесь означал бы обратное: "отсечь всё", кроме постов ровно за
    сегодня.

    Посты без распознанной даты НЕ записываются - cleanup_source их не
    трогает (нераспознанная дата -> не удаляем), такой пост иначе
    накапливался бы в листе вечно.

    Листание страниц назад по каналу останавливается, как только на
    очередной странице не нашлось НИ ОДНОГО поста в пределах окна
    SOURCE_MAX_AGE_DAYS - решение по всей странице целиком (не по
    первому старому посту), т.к. порядок виджетов в HTML не гарантирован
    (min_id ниже вычисляется явным сравнением, а не берётся как первый в
    списке). TG_DEEP_PAGES остаётся страховочным потолком страниц на
    канал, а не основным критерием остановки.
    """
    out, seen = [], set()
    undated = 0
    try:
        r = _tg_page(channel)
    except Exception as e:
        return [], f'unavailable ({e})', 0, 0
    if r.status_code != 200 or '/s/' not in r.url:
        return [], 'unavailable (не публичный канал/чат/опечатка)', 0, 0

    today = dt.date.today()
    before = None
    for _ in range(TG_DEEP_PAGES):
        try:
            resp = _tg_page(channel, before) if before else r
            if before:
                r = resp
        except Exception:
            break
        soup = BeautifulSoup(r.text, 'lxml')
        widgets = soup.select('.tgme_widget_message')
        if not widgets:
            break
        min_id = None
        any_fresh = False
        for w in widgets:
            data_post = w.get('data-post', '')     # 'channel/1899'
            mid = data_post.split('/')[-1] if data_post else ''
            if not mid or mid in seen:
                continue
            seen.add(mid)
            if min_id is None or int(mid) < int(min_id):
                min_id = mid
            date_el = w.select_one('time')
            date = date_el.get('datetime', '')[:10] if date_el else ''
            pub_date = _parse_pub_date(date)
            if pub_date is None:
                undated += 1
                continue                            # без даты - не пишем (см. докстринг)
            if SOURCE_MAX_AGE_DAYS and (today - pub_date).days > SOURCE_MAX_AGE_DAYS:
                continue                            # старше окна сбора - не пишем
            any_fresh = True
            text_el = w.select_one('.tgme_widget_message_text')
            text = text_el.get_text('\n', strip=True) if text_el else ''
            text = clean_tg_text(text)              # срезаем навигацию/рекламу
            if not text.strip() or not is_product(text):  # cut: только продуктовое
                continue
            out.append(RawVacancy(
                source=f'tg:{channel}', source_id=f'tg:{channel}:{mid}',
                published=date,
                url=f'https://t.me/{channel}/{mid}',
                raw_text=text, channel=channel,
            ))
        if not any_fresh or not min_id:
            break
        before = min_id     # листаем ленту дальше в прошлое
    return out, 'ok', len(seen), undated

def feed_telegram():
    """Фидер телеграма: все каналы. Битые каналы помечает, не падает.
    Возвращает (список прошедших предфильтр, отчёт по каналам, суммарное
    число постов ДО предфильтра по всем каналам - для детектора аномалий)."""
    all_v, report = [], []
    total_raw = 0
    for ch in TG_CHANNELS:
        vacs, status, raw_count, undated_count = feed_telegram_channel(ch)
        report.append((ch, status, len(vacs)))
        all_v.extend(vacs)
        total_raw += raw_count
        extra = f', без даты пропущено: {undated_count}' if undated_count else ''
        print(f'  [TG] {ch:26} {status:40} постов: {len(vacs)}{extra}')
    return all_v, report, total_raw

# ==========================================================================
# РЕЕСТР ИСТОЧНИКОВ  <-- сюда добавлять новые фидеры
# ==========================================================================
# Формат: (имя_листа, функция_фидер). Функция возвращает список RawVacancy.
# Добавить сайт-источник = написать feed_xxx() по образцу feed_habr() и вписать сюда.
# Добавить телеграм-канал = дописать строку в TG_CHANNELS (новый фидер не нужен).
STRUCTURED_SOURCES = [
    (SHEET_HABR, feed_habr),
    # (SHEET_XXX, feed_xxx),   # <- новый структурный источник
]

# ==========================================================================
# GOOGLE SHEETS
# ==========================================================================
COLS_HABR = ['source_id', 'Должность', 'Компания', 'Вилка', 'Локация', 'Грейд',
             'Рейтинг', 'Опубликовано', 'Ссылка', 'Статус',
             'llm_оценка', 'llm_вердикт', 'Комментарии']
COLS_TG = ['source_id', 'Канал', 'Опубликовано', 'Текст', 'Ссылка', 'Статус',
           'llm_оценка', 'llm_вердикт', 'Комментарии']
MANUAL = {'Комментарии', 'llm_оценка', 'llm_вердикт'}   # звено 1 их не трогает

def with_retry(fn, tries=5, base_delay=2, what="операция"):
    """До 5 попыток с паузами 2/4/8/16с. После последней - падаем с понятной ошибкой.
    Только для идемпотентных сетевых вызовов (чтение/открытие/append с дедупом)."""
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


def open_ws(gc, title, cols):
    ss = with_retry(lambda: gc.open_by_key(SPREADSHEET_ID), what="открытие таблицы-сборщика")
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=2000, cols=len(cols))
    vals = with_retry(lambda: ws.get_all_values(), what="чтение листа-сборщика")
    if not vals or vals[0][:len(cols)] != cols:
        ws.update([cols], 'A1')
        ws.freeze(rows=1)
    return ws

def existing_ids(ws):
    vals = with_retry(lambda: ws.get_all_values(), what="чтение листа-сборщика")
    if not vals:
        return set()
    return {row[0] for row in vals[1:] if row and row[0]}

def row_habr(v):
    return [v.source_id, v.title, v.company, v.salary, v.location, v.grade,
            v.rating, v.published, v.url, 'актуальна', '', '', '']

def row_tg(v):
    return [v.source_id, v.channel, v.published, v.raw_text, v.url,
            'актуальна', '', '', '']

def append_new(ws, rows):
    if rows:
        with_retry(lambda: ws.append_rows(rows, value_input_option='USER_ENTERED'),
                   what="запись новых строк в сборщик")
    return len(rows)

def _parse_pub_date(s):
    """Парсит дату из 'Опубликовано'. '2026-08-12', '2026-08-12 00:00:00', datetime.
    Не распознал -> None (строку не трогаем)."""
    if not s:
        return None
    if isinstance(s, dt.datetime):
        return s.date()
    if isinstance(s, dt.date):
        return s
    txt = str(s).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return dt.datetime.strptime(txt[:len(fmt)+2], fmt).date()
        except ValueError:
            continue
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', txt)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def cleanup_source(ws, cols):
    """Удаляет из листа-сборщика строки старше SOURCE_MAX_AGE_DAYS по 'Опубликовано'.
    Нераспознанная дата -> НЕ трогаем. Никогда не роняет прогон."""
    if not SOURCE_MAX_AGE_DAYS:
        return
    try:
        pub_idx = cols.index('Опубликовано')
    except ValueError:
        return
    try:
        vals = with_retry(lambda: ws.get_all_values(), what="чтение сырья перед подчисткой")
    except Exception as e:
        print(f'  [подчистка сырья] чтение не удалось: {e}')
        return
    if len(vals) <= 1:
        return
    today = dt.date.today()
    to_delete = []
    for i, row in enumerate(vals[1:], start=2):
        if len(row) <= pub_idx:
            continue
        d = _parse_pub_date(row[pub_idx])
        if d is not None and (today - d).days > SOURCE_MAX_AGE_DAYS:
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
        print(f'  [подчистка сырья] удалено старых строк (>{SOURCE_MAX_AGE_DAYS}д): {len(to_delete)}')
    except Exception as e:
        print(f'  [подчистка сырья] не удалось удалить: {e}')


# ==========================================================================
# MAIN
# ==========================================================================
def main():
    if not SPREADSHEET_ID:
        raise SystemExit('SPREADSHEET_ID не задан (.env / Secrets).')
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    gc = gspread.authorize(creds)

    collected_counts = {}   # источник -> карточек ДО предфильтра (детектор аномалий)

    # --- структурные источники (Хабр и будущие) ---
    for sheet_name, feeder in STRUCTURED_SOURCES:
        print(f'Источник [{sheet_name}]...')
        vacs, total_cards = feeder()
        collected_counts[sheet_name] = total_cards
        ws = open_ws(gc, sheet_name, COLS_HABR)
        have = existing_ids(ws)
        new = [row_habr(v) for v in vacs if v.source_id not in have]
        n = append_new(ws, new)
        print(f'  собрано: {len(vacs)} (карточек до предфильтра: {total_cards}), новых записано: {n}')
        cleanup_source(ws, COLS_HABR)

    # --- телеграм ---
    print('Источник [Телеграм]...')
    tg_vacs, report, total_tg_posts = feed_telegram()
    collected_counts[SHEET_TG] = total_tg_posts
    ws_tg = open_ws(gc, SHEET_TG, COLS_TG)
    have = existing_ids(ws_tg)
    new_tg = [row_tg(v) for v in tg_vacs if v.source_id not in have]
    n_tg = append_new(ws_tg, new_tg)
    print(f'  телеграм всего постов: {len(tg_vacs)} (постов до предфильтра: {total_tg_posts}), новых записано: {n_tg}')
    cleanup_source(ws_tg, COLS_TG)

    bad = [f'{ch} ({st})' for ch, st, _ in report if st != 'ok']
    if bad:
        print('\n[!] Недоступные/пустые каналы (проверь хендлы):')
        for b in bad:
            print('   -', b)

    try:
        check_collection_anomaly(gc, collected_counts, open_ws)
    except Exception as e:
        print(f'  [мониторинг] детектор аномалий сбора не выполнился: {e}')

    print(f'\nЗвено 1 готово. Прогон: {dt.date.today().isoformat()}')
    print('LLM-оценка (звено 2) заполнит колонки llm_оценка/llm_вердикт.')

if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        if e.code not in (0, None):
            from alerts import report_crash
            report_crash('vacancy_collector.py', e)
        raise
    except Exception as e:
        from alerts import report_crash
        report_crash('vacancy_collector.py', e)
        raise
