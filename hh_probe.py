"""
hh_probe.py - разовая проверка: пускает ли hh.ru из GitHub Actions.

Ничего никуда не пишет: ни в таблицы, ни в телеграм. Секреты не нужны.
Печатает в лог код ответа, нашёлся ли JSON-блок и сколько в нём вакансий.
Запускать вручную через workflow_dispatch.
"""

import html
import json
import re
import sys

import requests

# Один запрос. Фильтры: Россия, без опыта, искать только в названии вакансии.
URL = (
    "https://kazan.hh.ru/search/vacancy"
    "?text=%D0%9C%D0%B5%D0%BD%D0%B5%D0%B4%D0%B6%D0%B5%D1%80+%D0%BF%D1%80%D0%BE%D0%B4%D1%83%D0%BA%D1%82%D0%B0"
    "&area=113"
    "&experience=noExperience"
    "&search_field=name"
    "&ored_clusters=true"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
}

STATE_RE = re.compile(
    r'id="HH-Lux-InitialState"[^>]*>(.*?)</template>', re.S
)


def extract(page_html):
    """Достаёт вакансии из встроенного JSON.

    Возвращает (vacancies, total, note). vacancies=None - разобрать не удалось.
    """
    m = STATE_RE.search(page_html)
    if not m:
        return None, None, "блок HH-Lux-InitialState не найден"

    raw = m.group(1)
    # Битые html-сущности вида &#3#34; / &#34#34; вместо &#34;
    repaired = re.sub(r"&#3\d?#34;", "&#34;", raw)
    note = "json ok" if repaired == raw else "json ok (после починки сущностей)"

    try:
        data = json.loads(html.unescape(repaired))
    except json.JSONDecodeError as exc:
        # Мягкая деградация: отличаем битый escape от смены структуры.
        found = re.findall(r'"vacancyId":(\d+)', html.unescape(repaired))
        hint = f", но id вакансий видно: {len(set(found))}" if found else ""
        return None, None, f"json не разобрался: {exc}{hint}"

    result = data.get("vacancySearchResult")
    if result is None:
        return None, None, "в состоянии нет vacancySearchResult"

    return result.get("vacancies", []), result.get("totalResults"), note


def main():
    print(f"GET {URL}")
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=30)
    except requests.RequestException as exc:
        print(f"ПРОВАЛ: запрос не состоялся - {exc}")
        return 1

    print(f"код ответа: {resp.status_code}")
    print(f"конечный адрес: {resp.url}")
    print(f"размер страницы: {len(resp.text)} символов")

    body = resp.text.lower()
    for marker in ("captcha", "являетесь роботом", "доступ ограничен"):
        if marker in body:
            print(f"ВНИМАНИЕ: на странице встречается '{marker}'")

    if resp.status_code != 200:
        print("ПРОВАЛ: hh не отдал страницу")
        return 1

    vacancies, total, note = extract(resp.text)
    print(f"разбор: {note}")

    if vacancies is None:
        print("ПРОВАЛ: страница пришла, но вакансии не достать")
        return 1

    print(f"всего по запросу: {total}")
    print(f"на странице: {len(vacancies)}")

    for v in vacancies[:5]:
        company = (v.get("company") or {}).get("name", "?")
        area = (v.get("area") or {}).get("name", "?")
        print(f"  - {v.get('name')} | {company} | {area}")

    if not vacancies:
        print("Запрос прошёл, но вакансий ноль - это не сбой, а пустая выдача")

    print("УСПЕХ: hh доступен из GitHub Actions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
