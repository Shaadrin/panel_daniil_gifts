# -*- coding: utf-8 -*-
"""
Парсит Telegram Stars Gifts (раздел «Перепродажа») через MTProto на KuriGram.

Что делает:
  1) payments.GetStarGifts -> список подарков (gift_id, title, и т.п.).
  2) payments.GetResaleStarGifts(gift_id, attributes=[], limit=0, sort_by_price)
     -> достаём модели (sticker_id, name, rarity_per_mille).
  3) Для каждой модели повторяем GetResaleStarGifts с фильтром:
     attributes=[StarGiftAttributeIdModel(sticker_id=...)] и limit=1, sort_by_price=True
     -> берём минимальную цену (floor).
  4) Пишем в JSON батчами по 50 записей (без блокировки файла).
Логи: только авторизация, шаги/прогресс, найденные цены.
"""

import os
import sys
import json
import math
import time
import asyncio
from typing import Any, Dict, List, Optional, Tuple

# ---------- НАСТРОЙКИ ----------
API_ID  = int(os.environ.get("TG_API_ID", "21757287"))      # подставьте свои
API_HASH = os.environ.get("TG_API_HASH", "78389065683ede6c2d7e2b308a634f88")
SESSION  = os.environ.get("TG_SESSION", "kurigram_resale")
OUT_FILE = os.path.abspath("tg_resale_market.json")

BATCH_SIZE = 50          # писать в JSON по 50 найденных моделей
CONCURRENCY = 5          # параллельных запросов на минимальную цену (бережно!)
RETRY_MAX = 5            # попыток при временных ошибках
RETRY_BASE_SLEEP = 1.5   # базовая пауза между попытками
LOG_PROGRESS_EVERY = 1   # как часто печатать прогресс по шагам (каждый шаг)

# ---------- ИМПОРТ KURIGRAM ----------
try:
    from pyrogram import Client, errors
    from pyrogram.raw import functions as raw_funcs
    from pyrogram.raw import types as raw_types
except Exception as e:
    print("[ERR] Установите KuriGram: pip install -U kurigram")
    raise

# --------- УТИЛИТЫ ЛОГГЕРА (минимум шума) ---------
def log(msg: str) -> None:
    print(msg, flush=True)

def save_json_atomic(path: str, data: List[Dict[str, Any]]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # На Windows os.replace атомарен на уровне каталога
    os.replace(tmp, path)

# --------- ВСПОМОГАТЕЛЬНОЕ: извлечение цены из TL-объектов ---------
def extract_floor_price_from_item(item: Any) -> Optional[int]:
    """
    Элементы, которые возвращает GetResaleStarGifts, могут отличаться по названию полей в разных билдах.
    Стараемся найти цену «как есть».
    """
    # Самые частые варианты из свежих слоёв/библиотек:
    for key in ("resale_star_count", "price", "amount", "stars"):
        val = getattr(item, key, None)
        if isinstance(val, int):
            return int(val)
    # Иногда цена лежит в подполе gift/unique, проверим рекурсивно:
    for key in ("gift", "unique", "star_gift", "starGift", "g"):
        sub = getattr(item, key, None)
        if sub is not None:
            p = extract_floor_price_from_item(sub)
            if p is not None:
                return p
    return None

# --------- ВСПОМОГАТЕЛЬНОЕ: извлечение списка моделей из ответа с attributes ---------
def extract_models_from_attributes(attrs: List[Any]) -> List[Dict[str, Any]]:
    """
    В ответе payments.GetResaleStarGifts(..., limit=0) обычно приходит список доступных атрибутов.
    Среди них есть «model(s)» с нужными полями:
      - sticker_id (int)
      - name (str) — обычно есть
      - rarity_per_mille (int) — редкость в промилле *10 (как в TDLib)
    Имена типов/полей могут слегка отличаться в разных SDK. Берём гибко.
    """
    out: List[Dict[str, Any]] = []

    if not attrs:
        return out

    for a in attrs:
        cls = a.__class__.__name__.lower()
        # Ищем атрибут «models»/«model»
        if "model" in cls:  # например StarGiftAttributeModels / StarGiftAttributeModel
            # Коллекция моделей может называться models / values / items
            candidates = (
                getattr(a, "models", None),
                getattr(a, "values", None),
                getattr(a, "items", None),
            )
            models_vec = None
            for cand in candidates:
                if isinstance(cand, list):
                    models_vec = cand
                    break
            if not models_vec:
                continue

            for m in models_vec:
                # sticker_id может быть прямым полем, или вложенным sticker.id
                sid = getattr(m, "sticker_id", None)
                if sid is None:
                    sticker = getattr(m, "sticker", None)
                    sid = getattr(sticker, "id", None) if sticker is not None else None

                name = getattr(m, "name", None)
                rpm  = getattr(m, "rarity_per_mille", None)
                # Попробуем альтернативные названия полей
                if rpm is None:
                    rpm = getattr(m, "rarityPermille", None)
                if isinstance(sid, int):
                    out.append({
                        "sticker_id": int(sid),
                        "name": str(name) if name else "",
                        "rarity_per_mille": int(rpm) if isinstance(rpm, int) else None
                    })
    return out

def rpm_to_str(rpm: Optional[int]) -> str:
    if rpm is None:
        return ""
    try:
        return f"{int(rpm)/10:.1f}".replace(".", ",")
    except Exception:
        return ""

# --------- ВЫЗОВЫ С ПОВТОРАМИ ---------
async def invoke_with_retries(app: Client, req: Any, *, what: str, attempt0: int = 0):
    attempt = attempt0
    while True:
        try:
            return await app.invoke(req)
        except errors.FloodWait as fw:
            sleep_s = int(getattr(fw, "value", 5)) + 1
            log(f"[WARN] FLOOD_WAIT {sleep_s}s на {what}; спим…")
            await asyncio.sleep(sleep_s)
        except (errors.BadRequest, errors.InternalServerError, errors.Timeout) as e:
            if attempt >= RETRY_MAX:
                log(f"[ERROR] {what}: исчерпаны повторы ({RETRY_MAX}) -> {e}")
                raise
            backoff = RETRY_BASE_SLEEP * (2 ** attempt)
            log(f"[WARN] {what}: {e}. Повтор через {backoff:.1f}s (#{attempt+1})")
            await asyncio.sleep(backoff)
            attempt += 1

# --------- ОСНОВНАЯ ЛОГИКА ---------
async def get_all_gifts(app: Client) -> List[Any]:
    """payments.GetStarGifts(hash=0) -> список подарков (StarGift)."""
    # Некоторые реализации требуют hash=0; если будет несовпадение — библиотека подскажет.
    req = raw_funcs.payments.GetStarGifts(hash=0)
    res = await invoke_with_retries(app, req, what="GetStarGifts")
    gifts = getattr(res, "gifts", None) or getattr(res, "star_gifts", None) or []
    return list(gifts)

async def get_gift_models(app: Client, gift_id: int) -> List[Dict[str, Any]]:
    """
    Получить список моделей для gift_id:
      payments.GetResaleStarGifts(gift_id, attributes=[], limit=0, sort_by_price=True)
    и распарсить attributes -> models (sticker_id, name, rarity)
    """
    req = raw_funcs.payments.GetResaleStarGifts(
        gift_id=gift_id,
        attributes=[],       # без фильтров
        offset="",
        limit=0,
        sort_by_price=True    # чтобы сервер вернул консистентные attributes
    )
    res = await invoke_with_retries(app, req, what=f"GetResaleStarGifts(models, gift_id={gift_id})")
    attrs = getattr(res, "attributes", None) or []
    return extract_models_from_attributes(attrs)

def make_attr_model_id(sticker_id: int):
    """
    Создаёт TL-объект фильтра модели: StarGiftAttributeIdModel(sticker_id=?)
    Имена классов иногда могут различаться в форках, попробуем несколько.
    """
    candidates = (
        "StarGiftAttributeIdModel",
        "StarGiftAttributeIDModel",
        "StarGiftAttributeId_Models",  # на всякий случай
    )
    for name in candidates:
        cls = getattr(raw_types, name, None)
        if cls is not None:
            return cls(sticker_id=sticker_id)
    raise RuntimeError("В вашей сборке нет типа StarGiftAttributeIdModel. Обновите kurigram до свежего релиза (layer≈205).")

async def get_floor_for_model(app: Client, gift_id: int, sticker_id: int) -> Optional[int]:
    """
    Берём минимальную цену по модели: limit=1 + sort_by_price=True
    """
    attr = make_attr_model_id(sticker_id)
    req = raw_funcs.payments.GetResaleStarGifts(
        gift_id=gift_id,
        attributes=[attr],
        offset="",
        limit=1,
        sort_by_price=True
    )
    res = await invoke_with_retries(app, req, what=f"GetResaleStarGifts(floor, gift_id={gift_id}, sticker_id={sticker_id})")
    items = getattr(res, "gifts", None) or []
    if not items:
        return None
    # Мы просили sort_by_price=True и limit=1 — первый элемент должен быть самым дешёвым
    return extract_floor_price_from_item(items[0])

async def dump_resale(app: Client):
    # Авторизация
    log("[AUTH] Запуск KuriGram…")
    await app.start()
    me = await app.get_me()
    log("[AUTH] Готово: авторизован как @" + (me.username or str(me.id)))

    # 1) Список подарков
    gifts = await get_all_gifts(app)
    log(f"[INFO] Получено подарков: {len(gifts)} (до фильтрации)")

    # Фильтруем очевидный мусор, если в объекте есть поле resale_count и оно == 0
    filtered = []
    for g in gifts:
        resale_count = getattr(g, "resale_count", None)
        if resale_count is None:
            filtered.append(g)
        else:
            if resale_count > 0:
                filtered.append(g)
    log(f"[INFO] К обработке подарков: {len(filtered)}")

    out: List[Dict[str, Any]] = []
    processed_models = 0

    # 2) Для каждого подарка — собрать модели
    for idx, g in enumerate(filtered, start=1):
        gift_id = int(getattr(g, "id", 0) or getattr(g, "gift_id", 0))
        title = getattr(g, "title", None) or getattr(g, "name", None) or "Unknown Gift"

        # Модели
        models = await get_gift_models(app, gift_id)
        models_cnt = len(models)

        if (idx % LOG_PROGRESS_EVERY) == 0:
            log(f"[STEP] {idx}/{len(filtered)} — '{title}' (gift_id={gift_id}, models={models_cnt})")
            log(f"[PROGRESS] models: {processed_models} записаны (с ценой)")

        if models_cnt == 0:
            continue

        # 3) Поиск floor-цены для каждой модели, бережно (ограничиваем параллелизм)
        sem = asyncio.Semaphore(CONCURRENCY)

        async def fetch_one(m: Dict[str, Any]):
            sticker_id = int(m["sticker_id"])
            async with sem:
                price = await get_floor_for_model(app, gift_id, sticker_id)
                return {
                    "gift": title,
                    "gift_id": str(gift_id),
                    "model": m.get("name") or "",
                    "rarity_per_mille": rpm_to_str(m.get("rarity_per_mille")),
                    "price": int(price) if isinstance(price, int) else None,
                    "sticker_id": sticker_id,
                }

        tasks = [asyncio.create_task(fetch_one(m)) for m in models]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Сохраняем только те, у кого есть цена (по вашему сценарию нужны лоты перепродажи)
        for r in results:
            if isinstance(r, Exception):
                continue
            if r["price"] is not None:
                out.append(r)
                processed_models += 1
                # Пакетная запись
                if (len(out) % BATCH_SIZE) == 0:
                    save_json_atomic(OUT_FILE, out)
        # Дополнительная промежуточная запись на каждом шаге (на всякий)
        save_json_atomic(OUT_FILE, out)

    # финальная запись
    save_json_atomic(OUT_FILE, out)
    log(f"[OK] Сохранено {len(out)} записей в {OUT_FILE}")

async def probe_schema():
    """
    Помогает понять, что есть в установленной версии KuriGram.
    """
    print("== PROBE: payments.* методы с \"Gift\"/\"Resale\" ==")
    got = False
    for name in dir(raw_funcs.payments):
        if ("Gift" in name or "Resale" in name or "Star" in name) and not name.startswith("_"):
            print("  -", name)
            got = True
    if not got:
        print("  (ничего не найдено — обновите kurigram)")

    print("\n== PROBE: raw_types, ищем StarGiftAttributeIdModel ==")
    for name in dir(raw_types):
        if "StarGiftAttribute" in name or "Resale" in name:
            if "IdModel" in name or "IDModel" in name or "Model" in name:
                print("  -", name)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true", help="Показать какие TL-методы/типы доступны (для отладки)")
    args = parser.parse_args()

    if args.probe:
        asyncio.run(probe_schema())
        return

    if not API_ID or not API_HASH:
        print("[ERR] Задайте TG_API_ID и TG_API_HASH через env или константы.")
        sys.exit(2)

    app = Client(SESSION, api_id=API_ID, api_hash=API_HASH, no_updates=True, in_memory=False)
    try:
        asyncio.run(dump_resale(app))
    finally:
        try:
            app.loop.run_until_complete(app.stop())
        except Exception:
            pass

if __name__ == "__main__":
    main()
