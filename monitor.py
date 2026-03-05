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
MODEL = "llama-3.3-70b-versatile"

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

Спам/реклама — это ТОЛЬКО:
- Ссылки на сторонние сайты (не klerk.ru и не официальные госсайты) с целью продвижения
- Явное предложение платных услуг с контактами или призывом обратиться
- Шаблонные ответы компаний типа "Спасибо за отзыв! Мы всегда рядом..."
- Упоминание конкретной компании/сервиса с явным рекламным умыслом

НЕ является спамом — отвечай NO:
- Ссылки на klerk.ru или его поддомены
- Ссылки на государственные сайты (gov.ru, pravo.gov.ru, nalog.ru и т.п.)
- Грубость, оскорбления, эмоциональные высказывания
- Политические и социальные комментарии
- Обращения к другим участникам (@имя, "В Виктория" и т.п.)
- Обычные вопросы и рассуждения по теме статьи
- Офтопик и флуд

Если сомневаешься — отвечай NO.
Ответь только: YES (реклама/спам) или NO
Комментарий: {text}"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# --- Хранение просмотренных ID ---

def load_last_id() -> int:
    if not os.path.exists(SEEN_IDS_FILE):
        return 0
    try:
        with open(SEEN_IDS_FILE, encoding="utf-8") as f:
            data = json.load(f)
            # поддержка старого формата (список)
            if isinstance(data, list):
                return max((int(x) for x in data), default=0)
            return int(data.get("last_id", 0))
    except (json.JSONDecodeError, OSError, ValueError) as e:
        log.warning("Не удалось прочитать %s: %s. Начинаем с 0.", SEEN_IDS_FILE, e)
        return 0


def save_last_id(last_id: int) -> None:
    with open(SEEN_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_id": last_id}, f, ensure_ascii=False)


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

def run_once(client: Groq, last_id: int, cycle_no: int) -> int:
    sep = "═" * 50
    log.info("%s", sep)
    log.info(" Цикл #%d | %s", cycle_no, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("%s", sep)

    comments = fetch_comments()
    new_comments = [c for c in comments if int(c["id"]) > last_id]

    if not new_comments:
        log.info("Новых комментариев нет.")
        return last_id

    total = len(new_comments)
    log.info("Новых комментариев для проверки: %d", total)

    new_last_id = last_id
    for i, comment in enumerate(new_comments, start=1):
        spam = is_spam(comment["text"], client)
        new_last_id = max(new_last_id, int(comment["id"]))

        if spam:
            log.info("[%d/%d] id=%s | SPAM", i, total, comment["id"])
            log.info("        Текст: %s", comment["text"][:200])
            log.info("        Статья: %s", comment["article_url"] or "неизвестна")
            log_spam(comment["text"], comment["article_url"])
        else:
            log.info("[%d/%d] id=%s | ok | %s", i, total, comment["id"], comment["text"][:100])

        # Задержка между запросами к Groq
        time.sleep(random.uniform(*GROQ_DELAY))

    save_last_id(new_last_id)
    return new_last_id


# --- Точка входа ---

def main() -> None:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        sys.exit(
            "Ошибка: переменная окружения GROQ_API_KEY не задана.\n"
            "Задайте её командой: export GROQ_API_KEY='твой_ключ'"
        )

    client = Groq(api_key=api_key)
    last_id = load_last_id()
    cycle_no = 1

    # RUN_ONCE=1 используется в GitHub Actions — один запуск и выход
    if os.environ.get("RUN_ONCE"):
        log.info("Режим одного запуска (GitHub Actions).")
        run_once(client, last_id, cycle_no)
        return

    log.info("Мониторинг запущен. Интервал: %d сек. Модель: %s", INTERVAL, MODEL)

    while True:
        try:
            last_id = run_once(client, last_id, cycle_no)
            cycle_no += 1
        except KeyboardInterrupt:
            log.info("Остановлено пользователем.")
            break
        except Exception as e:
            log.error("Непредвиденная ошибка: %s", e, exc_info=True)

        log.info("Следующая проверка через %d сек.", INTERVAL)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
