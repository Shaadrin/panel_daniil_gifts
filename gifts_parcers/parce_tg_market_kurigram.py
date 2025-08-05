#!/usr/bin/env python3
import asyncio
import json
import random
from typing import Any, Dict, List, Optional, Tuple
from time import perf_counter
from pyrogram import Client, raw
from datetime import datetime
from pathlib import Path
try:
    from pyrogram.errors import FloodWait, RPCError
except Exception:
    from pyrogram.errors import FloodWait, RPCError

API_ID = 21757287
API_HASH = "78389065683ede6c2d7e2b308a634f88"
BASE_DIR = Path(__file__).resolve().parent
SESSION = BASE_DIR / "kurigram_resale"

OUT_FILE = "tg_gifts_resale.json"
PRETTY_JSON = True                  # True → indent=2
JSONL = False                       # True → по одному объекту на строку

FLOOR_STRATEGY = "strict"           # "strict" (гарант) или "hybrid" (быстрее)

# Параллелизм и троттлинг
MAX_CONCURRENT_REQUESTS = 16
GIFTS_CONCURRENCY = 8
VERIFY_CONCURRENCY = 16
JITTER = (0.02, 0.06)

# Пагинация
PAGE_LIMIT = 100

# Для разведки/гибридного режима
DISCOVERY_PAGES_NUM = 2             # по номеру (num)
DISCOVERY_PAGES_PRICE = 2           # по цене
MODEL_DISCOVERY_CAP = 80
FALLBACK_SCAN_PAGES_FOR_MODEL = 2   # быстрый догляд по имени модели

# Логи
LOG_PROGRESS_EVERY_N_GIFTS = 5
VERBOSE = True
# ──────────────────────────────────────────────────────────────────────────────


# ─── ЛОГ ─────────────────────────────────────────────────────────────────────
def now() -> str:
    return datetime.now().strftime("%H:%M:%S")

def log(msg: str) -> None:
    print(f"[{now()}] {msg}", flush=True)


# ─── СЕМАФОРЫ ────────────────────────────────────────────────────────────────
REQ_SEM = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
GIFT_SEM = asyncio.Semaphore(GIFTS_CONCURRENCY)
VERIFY_SEM = asyncio.Semaphore(VERIFY_CONCURRENCY)


# ─── ХЕЛПЕРЫ ─────────────────────────────────────────────────────────────────
def fmt_permille(v: Optional[float | int]) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, int):
        v = v / 10.0
    return f"{float(v):.1f}".replace(".", ",")

def extract_price(g) -> float | None:
    if g is None:
        return None
    v = getattr(g, "stars", None)
    if v is None:
        v = getattr(g, "resell_stars", None)
    return float(v) if v is not None else None

def is_unique_gift(g) -> bool:
    return getattr(g, "attributes", None) is not None or getattr(g, "num", None) is not None

def pick_model_attr(attrs: list) -> Tuple[Optional[str], Optional[float | int], Optional[int]]:
    """Вернёт (model_name, rarity_permille|permille, model_document_id) если есть."""
    for a in attrs or []:
        if "model" in a.__class__.__name__.lower():
            name = getattr(a, "name", None)
            rarity = getattr(a, "rarity_permille", None) or getattr(a, "permille", None)
            doc = getattr(a, "document", None)
            doc_id = getattr(doc, "id", None) if doc is not None else None
            if name:
                return name, rarity, doc_id
    return None, None, None


async def mt_invoke(app: Client, req, *, retries=6):
    delay = 1.2
    for _ in range(retries):
        async with REQ_SEM:
            try:
                resp = await app.invoke(req)
                await asyncio.sleep(random.uniform(*JITTER))
                return resp
            except FloodWait as e:
                secs = int(getattr(e, "value", 1) or 1)
                if VERBOSE:
                    log(f"FloodWait {secs}s → спим")
                await asyncio.sleep(secs + 1)
            except RPCError as e:
                if VERBOSE:
                    log(f"RPCError: {e.__class__.__name__} → retry через {delay:.1f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
    async with REQ_SEM:
        resp = await app.invoke(req)
        await asyncio.sleep(random.uniform(*JITTER))
        return resp


# ─── API ─────────────────────────────────────────────────────────────────────
async def get_all_gift_ids(app: Client) -> List[int]:
    res = await mt_invoke(app, raw.functions.payments.GetStarGifts(hash=0))
    ids = [int(g.id) for g in res.gifts]
    log(f"Каталог: {len(ids)} подарков")
    return ids

async def page_resale(app: Client, gift_id: int, *, by_price: bool, offset: str, limit: int):
    return await mt_invoke(
        app,
        raw.functions.payments.GetResaleStarGifts(
            sort_by_price=by_price,
            sort_by_num=not by_price,
            gift_id=gift_id,
            offset=offset,
            limit=limit
        )
    )

async def floor_by_doc_id(app: Client, gift_id: int, doc_id: Optional[int]) -> Optional[float]:
    if not doc_id:
        return None
    try:
        resp = await mt_invoke(
            app,
            raw.functions.payments.GetResaleStarGifts(
                sort_by_price=True,
                sort_by_num=False,
                gift_id=gift_id,
                attributes=[raw.types.StarGiftAttributeIdModel(document_id=doc_id)],
                offset="",
                limit=1
            )
        )
        g = (getattr(resp, "gifts", []) or [None])[0]
        return extract_price(g)
    except RPCError:
        return None

async def min_price_by_scanning(app: Client, gift_id: int, model_name: str, max_pages: int = 2) -> Optional[float]:
    """Быстрый скан 1–2 страниц по цене с фильтром по имени модели."""
    offset = ""
    best: Optional[float] = None
    for _ in range(max_pages):
        resp = await page_resale(app, gift_id, by_price=True, offset=offset, limit=PAGE_LIMIT)
        gifts = getattr(resp, "gifts", []) or []
        if not gifts:
            break
        for g in gifts:
            attrs = getattr(g, "attributes", None)
            if not attrs:
                continue
            name, _, _ = pick_model_attr(attrs)
            if name != model_name:
                continue
            p = extract_price(g)
            if p is None:
                continue
            if best is None or p < best:
                best = p
        offset = getattr(resp, "next_offset", "")
        if not offset:
            break
    return best


# ─── РАЗВЕДКА МОДЕЛЕЙ (быстро) ──────────────────────────────────────────────
async def discover_models_fast(app: Client, gift_id: int) -> Tuple[Optional[str], Dict[str, Dict[str, Any]]]:
    """
    Возвращает: (title, { model_name: {rarity, doc_id} }).
    """
    title: Optional[str] = None
    models: Dict[str, Dict[str, Any]] = {}

    # Фаза 1: по номеру (даёт широкий срез моделей)
    offset = ""
    for _ in range(DISCOVERY_PAGES_NUM):
        resp = await page_resale(app, gift_id, by_price=False, offset=offset, limit=PAGE_LIMIT)
        gifts = getattr(resp, "gifts", []) or []
        if not gifts:
            break
        if title is None:
            title = getattr(gifts[0], "title", None)
        for g in gifts:
            if not is_unique_gift(g):
                continue
            name, rarity, doc_id = pick_model_attr(getattr(g, "attributes", None))
            if not name:
                continue
            rec = models.get(name)
            if rec is None:
                models[name] = {"rarity": rarity, "doc_id": doc_id}
            else:
                if rec.get("doc_id") is None and doc_id is not None:
                    rec["doc_id"] = doc_id
                if rec.get("rarity") is None and rarity is not None:
                    rec["rarity"] = rarity
        offset = getattr(resp, "next_offset", "")
        if not offset or len(models) >= MODEL_DISCOVERY_CAP:
            break

    # Фаза 2: по цене (если в фазе 1 не нашли)
    if not models:
        offset = ""
        for _ in range(DISCOVERY_PAGES_PRICE):
            resp = await page_resale(app, gift_id, by_price=True, offset=offset, limit=PAGE_LIMIT)
            gifts = getattr(resp, "gifts", []) or []
            if not gifts:
                break
            if title is None:
                title = getattr(gifts[0], "title", None)
            for g in gifts:
                if not is_unique_gift(g):
                    continue
                name, rarity, doc_id = pick_model_attr(getattr(g, "attributes", None))
                if not name:
                    continue
                rec = models.get(name)
                if rec is None:
                    models[name] = {"rarity": rarity, "doc_id": doc_id}
                else:
                    if rec.get("doc_id") is None and doc_id is not None:
                        rec["doc_id"] = doc_id
                    if rec.get("rarity") is None and rarity is not None:
                        rec["rarity"] = rarity
            offset = getattr(resp, "next_offset", "")
            if not offset or len(models) >= MODEL_DISCOVERY_CAP:
                break

    if VERBOSE:
        log(f"gift_id={gift_id}: обнаружено моделей {len(models)} (разведка)")
    return title, models


# ─── STRICT: ПОЛНЫЙ ПРОХОД ПО ЦЕНЕ С “single-model fallback” ────────────────
async def full_scan_floors(app: Client, gift_id: int) -> Tuple[Optional[str], Dict[str, Tuple[Optional[float | int], float]]]:
    """
    Гарантированный флор по МОДЕЛИ.

    Логика:
      1) короткая разведка discover_models_fast() — узнаём, сколько моделей.
      2) если модель ровно одна → любые лоты (в т.ч. без model-атрибута) считаем этой моделью.
      3) идём по ВСЕЙ выдаче sort_by_price=True до конца и считаем минимумы.
    """
    # 1) разведка
    title_probe, models_probe = await discover_models_fast(app, gift_id)
    single_model_name: Optional[str] = None
    single_model_rarity: Optional[float | int] = None
    if len(models_probe) == 1:
        single_model_name = next(iter(models_probe))
        single_model_rarity = models_probe[single_model_name].get("rarity")

    title: Optional[str] = title_probe
    floors: Dict[str, Tuple[Optional[float | int], float]] = {}

    offset = ""
    page = 0
    improvements = 0

    while True:
        resp = await page_resale(app, gift_id, by_price=True, offset=offset, limit=PAGE_LIMIT)
        gifts = getattr(resp, "gifts", []) or []
        if not gifts:
            if VERBOSE:
                log(f"gift_id={gift_id}: пустая страница → стоп")
            break

        if title is None:
            title = getattr(gifts[0], "title", None)

        for g in gifts:
            p = extract_price(g)
            if p is None:
                continue

            attrs = getattr(g, "attributes", None)
            model_name, rarity, _ = pick_model_attr(attrs)

            # ── КЛЮЧ: если у коллекции 1 модель, принимаем ЛЮБОЙ лот как эту модель
            if single_model_name is not None and model_name is None:
                model_name = single_model_name
                # если редкость не пришла в этом лоте — берём из разведки
                if rarity is None:
                    rarity = single_model_rarity

            # для многомодельных коллекций лоты без model пропускаем
            if model_name is None:
                continue

            rec = floors.get(model_name)
            if rec is None or p < rec[1]:
                floors[model_name] = (rarity if rarity is not None else (rec[0] if rec else None), p)
                if rec is not None:
                    improvements += 1

        page += 1
        if VERBOSE and page % 5 == 0:
            log(f"gift_id={gift_id}: стр. {page} | моделей: {len(floors)} | улучшений: {improvements}")

        offset = getattr(resp, "next_offset", "")
        if not offset:
            if VERBOSE:
                log(f"gift_id={gift_id}: конец выдачи (страниц {page})")
            break

    return title, floors


# ─── HYBRID (быстрее, но не 100% гарантия) ──────────────────────────────────
async def min_price_by_hybrid(app: Client, gift_id: int) -> Tuple[Optional[str], Dict[str, Tuple[Optional[float | int], float]]]:
    title, models = await discover_models_fast(app, gift_id)
    if not title or not models:
        return title, {}

    floors: Dict[str, Tuple[Optional[float | int], float]] = {}

    async def one(model_name: str, info: Dict[str, Any]):
        async with VERIFY_SEM:
            doc_id = info.get("doc_id")
            rarity = info.get("rarity")
            p1 = await floor_by_doc_id(app, gift_id, doc_id)
            p2 = await min_price_by_scanning(app, gift_id, model_name)
            candidates = [p for p in (p1, p2) if p is not None]
            if not candidates:
                return
            price = min(candidates)
            floors[model_name] = (rarity, price)

    await asyncio.gather(*(one(n, i) for n, i in models.items()))
    return title, floors


# ─── ОБРАБОТКА ОДНОГО ПОДАРКА ────────────────────────────────────────────────
async def process_gift(app: Client, gift_id: int) -> List[Dict[str, Any]]:
    async with GIFT_SEM:
        t0 = perf_counter()
        if FLOOR_STRATEGY == "strict":
            title, floors = await full_scan_floors(app, gift_id)
        else:
            title, floors = await min_price_by_hybrid(app, gift_id)

        if not title or not floors:
            if VERBOSE:
                log(f"gift_id={gift_id}: пропуск (title={bool(title)}, models={len(floors)})")
            return []

        rows = [
            {
                "gift": title,
                "model": model_name,
                "rarity_per_mille": fmt_permille(rarity),
                "price": float(price)
            }
            for model_name, (rarity, price) in floors.items()
        ]

        if VERBOSE:
            dt = perf_counter() - t0
            log(f"gift_id={gift_id}: записей {len(rows)} | {dt:.1f}s [{FLOOR_STRATEGY}]")
        return rows


# ─── ОБХОД ВСЕГО РЫНКА ───────────────────────────────────────────────────────
async def parse_market(app: Client) -> List[Dict[str, Any]]:
    gift_ids = await get_all_gift_ids(app)
    total = len(gift_ids)
    log(f"Старт обхода: {total} gifts ({FLOOR_STRATEGY})")

    all_results: List[Dict[str, Any]] = []
    done = 0

    async def worker(gid: int):
        nonlocal done, all_results
        res = await process_gift(app, gid)
        all_results.extend(res)
        done += 1
        if done % LOG_PROGRESS_EVERY_N_GIFTS == 0:
            log(f"Прогресс: {done}/{total} gifts | накоплено записей: {len(all_results)}")

    await asyncio.gather(*(worker(gid) for gid in gift_ids))
    return all_results


# ─── ВХОД ────────────────────────────────────────────────────────────────────
async def main():
    t0 = perf_counter()
    async with Client(SESSION, api_id=API_ID, api_hash=API_HASH) as app:
        data = await parse_market(app)
        if JSONL:
            with open(OUT_FILE, "w", encoding="utf-8") as f:
                for row in data:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        else:
            with open(OUT_FILE, "w", encoding="utf-8") as f:
                if PRETTY_JSON:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                else:
                    json.dump(data, f, ensure_ascii=False)
        log(f"✅ Готово: {len(data)} записей → {OUT_FILE} | {perf_counter() - t0:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())