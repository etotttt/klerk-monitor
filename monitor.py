"""
Мониторинг комментариев klerk.ru на предмет рекламы/спама.

Установка:
    pip install requests beautifulsoup4 groq

Запуск:
    export GROQ_API_KEY="твой_ключ"
    python monitor.py
"""

import json
import logging
import os
import random
import sys
import time
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup
from groq import Groq

# --- Конфиг ---
API_URL = "https://www.klerk.ru/yindex.php/v4/comments"
SEEN_IDS_FILE = "seen_ids.json"
SPAM_LOG_FILE = "spam_log.txt"
INTERVAL = 300  # секунд между проверками
MODEL = "llama-3.1-8b-instant"

# Задержка между запросами к Groq (сек), чтобы не спамить API
GROQ_DELAY = (1.5, 3.0)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.klerk.ru/",
}

SPAM_PROMPT = """Ты модератор бухгалтерского сайта klerk.ru. Определи, является ли комментарий рекламой или спамом.

Спам/реклама — это:
- Упоминание конкретной сторонней компании, сервиса, продукта с целью продвижения
- Предложение услуг (бухгалтерское обслуживание, аутсорсинг, юридические услуги и т.п.)
- Ссылки на сторонние сайты (НЕ klerk.ru) с рекламной целью
- Шаблонные ответы типа "Спасибо за отзыв! Мы всегда рядом..."

НЕ является спамом:
- Ссылки на klerk.ru или его поддомены — это родной сайт
- Офтопик и флуд (разговоры не по теме)
- Грубость, оскорбления, эмоциональные высказывания
- Политические высказывания
- Любые обычные комментарии без рекламного умысла

Ответь только: YES (реклама/спам) или NO
Комментарий: {text}"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# --- Хранение просмотренных ID ---

def load_seen_ids() -> set:
    if not os.path.exists(SEEN_IDS_FILE):
        return set()
    try:
        with open(SEEN_IDS_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Не удалось прочитать %s: %s. Начинаем с пустого набора.", SEEN_IDS_FILE, e)
        return set()


def save_seen_ids(ids: set) -> None:
    with open(SEEN_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(ids), f, ensure_ascii=False)


# --- Получение комментариев через API ---

def fetch_comments() -> list[dict]:
    """Запрашивает все комментарии за сегодня через API с пагинацией."""
    today = date.today().isoformat()
    comments = []
    page = 1

    while True:
        params = {
            "filter[date][gte]": f"{today} 00:00:00",
            "filter[date][lte]": f"{today} 23:59:59",
            "page": page,
        }
        try:
            time.sleep(random.uniform(1.0, 2.0))
            resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=20)
            resp.raise_for_status()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            if status in (429, 403):
                log.warning("Сервер вернул %s — ждём перед следующей попыткой.", status)
                time.sleep(60)
            else:
                log.warning("HTTP ошибка: %s", e)
            break
        except requests.RequestException as e:
            log.warning("Ошибка сети: %s", e)
            break

        batch = resp.json()
        if not batch:
            break

        for item in batch:
            text = BeautifulSoup(item.get("html", ""), "html.parser").get_text(separator=" ", strip=True)
            if not text:
                continue
            article_url = (item.get("entity") or {}).get("url", "")
            comments.append({
                "id": str(item["id"]),
                "text": text,
                "article_url": article_url,
            })

        if len(batch) < 20:
            break
        page += 1

    log.info("Найдено %d комментариев за сегодня.", len(comments))
    return comments


# --- Проверка через Groq ---

def is_spam(text: str, client: Groq) -> bool:
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": SPAM_PROMPT.format(text=text)}],
            max_tokens=5,
            temperature=0,
        )
        answer = response.choices[0].message.content.strip().upper()
        return answer.startswith("YES")
    except Exception as e:
        log.error("Ошибка Groq API: %s. Комментарий пропущен (не спам по умолчанию).", e)
        return False


# --- Логирование спама ---

def log_spam(text: str, url: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = (
        f"[{timestamp}] SPAM\n"
        f"Текст: {text}\n"
        f"Ссылка: {url if url else 'неизвестна'}\n"
        f"---\n"
    )
    with open(SPAM_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(entry)
    log.info("Спам записан в %s", SPAM_LOG_FILE)
    notify_bitrix(text, url)


# --- Уведомление в Битрикс ---

def notify_bitrix(text: str, url: str) -> None:
    webhook = os.environ.get("BITRIX_WEBHOOK")
    chat_id = os.environ.get("BITRIX_CHAT_ID")
    if not webhook or not chat_id:
        return
    message = f"🚨 Рекламный комментарий на klerk.ru\n\n{text[:300]}\n\nСтатья: {url or 'неизвестна'}"
    try:
        requests.post(webhook, json={"DIALOG_ID": chat_id, "MESSAGE": message}, timeout=10)
    except Exception as e:
        log.warning("Не удалось отправить уведомление в Битрикс: %s", e)


# --- Один цикл проверки ---

def run_once(client: Groq, seen_ids: set) -> None:
    comments = fetch_comments()
    new_comments = [c for c in comments if c["id"] not in seen_ids]

    if not new_comments:
        log.info("Новых комментариев нет.")
        return

    log.info("Новых комментариев для проверки: %d", len(new_comments))

    for comment in new_comments:
        seen_ids.add(comment["id"])

        spam = is_spam(comment["text"], client)
        verdict = "SPAM" if spam else "ok  "
        print(f"[{verdict}] id={comment['id']} | {comment['text'][:120]}", flush=True)
        if spam:
            log_spam(comment["text"], comment["article_url"])

        # Задержка между запросами к Groq
        time.sleep(random.uniform(*GROQ_DELAY))

    save_seen_ids(seen_ids)


# --- Точка входа ---

def main() -> None:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        sys.exit(
            "Ошибка: переменная окружения GROQ_API_KEY не задана.\n"
            "Задайте её командой: export GROQ_API_KEY='твой_ключ'"
        )

    client = Groq(api_key=api_key)
    seen_ids = load_seen_ids()

    # RUN_ONCE=1 используется в GitHub Actions — один запуск и выход
    if os.environ.get("RUN_ONCE"):
        log.info("Режим одного запуска (GitHub Actions).")
        run_once(client, seen_ids)
        return

    log.info("Мониторинг запущен. Интервал: %d сек. Модель: %s", INTERVAL, MODEL)

    while True:
        try:
            run_once(client, seen_ids)
        except KeyboardInterrupt:
            log.info("Остановлено пользователем.")
            save_seen_ids(seen_ids)
            break
        except Exception as e:
            log.error("Непредвиденная ошибка: %s", e, exc_info=True)

        log.info("Следующая проверка через %d сек.", INTERVAL)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
