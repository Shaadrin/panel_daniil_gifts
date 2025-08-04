# tg_resale_dump_win.py
"""
Выгружает ВСЕ модели из раздела «Перепродажа» Telegram-маркета (через TDLib, tdjson.dll)
и пишет их в один JSON:
[
  {"gift":"Astral Shard","gift_id":"5933629604416717361",
   "model":"White Opal","rarity_per_mille":"2,3","price":403,"sticker_id":123456789},
  ...
]

Ключевые моменты:
- Список моделей берём одним запросом searchGiftsForResale(limit=0).
- Floor-цену получаем СКАНОМ ленты перепродаж по цене (order=price), без attributes.
- Для сопоставления используем И document_id, И имя модели (имена внутри одного подарка уникальны).
- Останавливаем скан, когда собраны цены для всех моделей (по doc_id или по имени).
- Сохраняем результат каждые SAVE_EVERY записей.
- Можно включить RAW-дампы ответов TDLib для диагностики (папка td_db/raw/).

Windows x64, Python 3.10+.
"""

import os
import sys
import json
import time
import stat
import ctypes
import shutil
import tempfile
import platform
from typing import Any, Dict, List, Optional, Tuple

# ─────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────
API_ID: int = 21757287
API_HASH: str = "78389065683ede6c2d7e2b308a634f88"
TDLIB_BIN_DIR: str = r"C:\Users\myname\Desktop\all_personal_projects\panel_gifts_daniil\td\build\Release"

DB_DIR: str = os.path.abspath("td_db")
OUT_FILE: str = os.path.join(DB_DIR, "tg_resale_market.json")

SAVE_EVERY: int = 50          # сохранять каждые N найденных моделей
PAGE_LIMIT: int = 200         # размер страницы при скане ленты перепродаж
MAX_PAGES_PER_GIFT: int = 200 # предохранитель от бесконечного скролла
SLEEP_BETWEEN: float = 0.0    # мягкая пауза между найденными моделями

INCLUDE_STICKER_ID: bool = True
SHOW_PROGRESS: bool = False   # если True — печатаем шаги/прогресс
LOG_TD_SEND: bool = False     # отладка td_send (шумная)

# ── СЫРЫЕ ДАМПЫ ОТВЕТОВ (включи для диагностики) ──
RAW_DEBUG: bool = True
RAW_DIR: str = os.path.join(DB_DIR, "raw")
# Если указать gift_id (int), дампим только для него. Иначе — для всех.
DEBUG_LOG_ONLY_GIFT_ID: Optional[int] = None

# ─────────────────────────────
# TDLib загрузка
# ─────────────────────────────
if hasattr(os, "add_dll_directory"):
    os.add_dll_directory(TDLIB_BIN_DIR)
TDJSON_PATH = os.path.join(TDLIB_BIN_DIR, "tdjson.dll")
if not os.path.exists(TDJSON_PATH):
    print(f"[ERR] Не найден tdjson.dll в {TDLIB_BIN_DIR}")
    sys.exit(2)

_td = ctypes.CDLL(TDJSON_PATH)
_td.td_json_client_create.restype = ctypes.c_void_p
_td.td_json_client_send.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
_td.td_json_client_receive.argtypes = [ctypes.c_void_p, ctypes.c_double]
_td.td_json_client_receive.restype = ctypes.c_char_p
_td.td_json_client_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
_td.td_json_client_execute.restype = ctypes.c_char_p
_td.td_json_client_destroy.argtypes = [ctypes.c_void_p]

# ─────────────────────────────
# Утилиты
# ─────────────────────────────
def _b(x: str) -> bytes:
    return x.encode("utf-8")

def _now_ms() -> int:
    return int(time.time() * 1000)

def _should_dump_for_gift(gift_id: Optional[int]) -> bool:
    if not RAW_DEBUG:
        return False
    if DEBUG_LOG_ONLY_GIFT_ID is None:
        return True
    return gift_id == DEBUG_LOG_ONLY_GIFT_ID

def raw_dump(name: str, payload: Dict[str, Any], gift_id: Optional[int] = None) -> None:
    if not _should_dump_for_gift(gift_id):
        return
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        ts = _now_ms()
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(name))
        path = os.path.join(RAW_DIR, f"{ts}_{safe}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def td_send(client, d: Dict[str, Any]):
    if LOG_TD_SEND:
        print(f"[DEBUG] td_send: {json.dumps(d, ensure_ascii=False)}")
    _td.td_json_client_send(client, _b(json.dumps(d)))

def td_recv(client, timeout: float = 2.0) -> Optional[Dict[str, Any]]:
    r = _td.td_json_client_receive(client, ctypes.c_double(timeout))
    return None if not r else json.loads(r.decode("utf-8"))

def td_exec(client, d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = _td.td_json_client_execute(client, _b(json.dumps(d)))
    return None if not r else json.loads(r.decode("utf-8"))

def save_snapshot(path: str, data, attempts: int = 8, base_delay: float = 0.15) -> None:
    """Атомарное сохранение JSON с ретраями (Windows-friendly)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path)+".", suffix=".tmp",
                                    dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        try: os.close(fd)
        except Exception: pass
        raise

    for i in range(attempts):
        try:
            if os.path.exists(path):
                try: os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
                except Exception: pass
            os.replace(tmp_path, path)
            return
        except PermissionError:
            time.sleep(base_delay * (2 ** i))
        except Exception:
            break

    fallback = path + ".partial"
    try:
        os.replace(tmp_path, fallback)
        print(f"[WARN] Не удалось заменить '{path}' (занят). Снимок: '{fallback}'")
    except Exception:
        print(f"[WARN] Не удалось сохранить снимок. tmp: {tmp_path}")

def rarity_to_str(rpm: int) -> str:
    try:
        return f"{int(rpm)/10:.1f}".replace(".", ",")
    except Exception:
        return ""

def pause(sec: float):
    if sec and sec > 0:
        time.sleep(sec)

# ─────────────────────────────
# Авторизация
# ─────────────────────────────
def auth(client):
    td_exec(client, {"@type": "setLogVerbosityLevel", "new_verbosity_level": 0})
    td_exec(client, {"@type": "setLogStream", "log_stream": {"@type": "logStreamEmpty"}})

    td_send(client, {"@type": "getAuthorizationState"})
    while True:
        upd = td_recv(client, 10) or {}
        t = upd.get("@type")

        if t == "updateAuthorizationState":
            st = upd["authorization_state"]["@type"]

            if st == "authorizationStateWaitTdlibParameters":
                print("[AUTH] Инициализация TDLib…")
                td_send(client, {
                    "@type": "setTdlibParameters",
                    "use_test_dc": False,
                    "database_directory": DB_DIR,
                    "files_directory": os.path.join(DB_DIR, "files"),
                    "use_file_database": True,
                    "use_chat_info_database": True,
                    "use_message_database": True,
                    "use_secret_chats": False,
                    "api_id": int(API_ID),
                    "api_hash": str(API_HASH),
                    "system_language_code": "en",
                    "device_model": "Windows",
                    "system_version": platform.platform(),
                    "application_version": "1.0",
                    "enable_storage_optimizer": True
                })

            elif st == "authorizationStateWaitEncryptionKey":
                td_send(client, {"@type": "checkDatabaseEncryptionKey", "encryption_key": ""})

            elif st == "authorizationStateWaitPhoneNumber":
                phone = input("Телефон (+7999...): ").strip()
                td_send(client, {"@type": "setAuthenticationPhoneNumber", "phone_number": phone})

            elif st == "authorizationStateWaitCode":
                code = input("Код из Telegram: ").strip()
                td_send(client, {"@type": "checkAuthenticationCode", "code": code})

            elif st == "authorizationStateWaitPassword":
                pwd = input("Пароль 2FA: ").strip()
                td_send(client, {"@type": "checkAuthenticationPassword", "password": pwd})

            elif st == "authorizationStateReady":
                print("[AUTH] Готово: авторизован")
                return

        elif t == "error":
            print("[TD][ERR]", upd)
            raise RuntimeError(upd)

# ─────────────────────────────
# API Gifts Resale
# ─────────────────────────────
def get_available_gifts(client) -> List[Dict[str, Any]]:
    rid = f"avail_{_now_ms()}"
    req = {"@type": "getAvailableGifts", "@extra": {"rid": rid, "kind": "available"}}
    td_send(client, req)
    while True:
        upd = td_recv(client, 15)
        if upd and upd.get("@type") == "availableGifts":
            raw_dump(f"recv_available_{rid}", upd, None)
            return upd.get("gifts", [])

def get_models_for_gift_limit0(client, gift_id: int) -> List[Dict[str, Any]]:
    """
    В этой сборке TDLib список models приходит ТОЛЬКО при limit=0.
    Один запрос — без пагинации — и возвращаем поле "models".
    """
    rid = f"models_{gift_id}_{_now_ms()}"
    req = {
        "@type": "searchGiftsForResale",
        "gift_id": gift_id,
        "order": {"@type": "giftForResaleOrderPrice"},
        "attributes": [],
        "offset": "",
        "limit": 0,
        "@extra": {"rid": rid, "kind": "models", "gift_id": gift_id}
    }
    raw_dump(f"send_models_{gift_id}_{rid}", req, gift_id)
    td_send(client, req)
    while True:
        upd = td_recv(client, 20)
        if upd and upd.get("@type") == "giftsForResale":
            # большинство TDLib эхоит @extra, но на всякий случай дампим всё
            raw_dump(f"recv_models_{gift_id}_{rid}", upd, gift_id)
            return upd.get("models", []) or []

# ─────────────────────────────
# Извлечения
# ─────────────────────────────
def _extract_doc_id(m: Dict[str, Any]) -> Optional[int]:
    """
    Из модели -> документ/стикер id (тот же, что в листингах).
    """
    try_paths = [
        lambda x: ((x.get("model") or {}).get("sticker") or {}).get("id"),
        lambda x: (x.get("sticker") or {}).get("id"),
        lambda x: ((x.get("model") or {}).get("upgraded_sticker") or {}).get("id"),
        lambda x: ((x.get("gift") or {}).get("sticker") or {}).get("id"),
    ]
    for fn in try_paths:
        v = fn(m)
        if isinstance(v, int):
            return v
    return None

def _extract_model_name_from_model(m: Dict[str, Any]) -> Optional[str]:
    mdl = m.get("model") or {}
    name = mdl.get("name") or m.get("name")
    name = name.strip() if isinstance(name, str) else None
    return name if name else None

def _extract_doc_id_from_listing(it: Dict[str, Any]) -> Optional[int]:
    g = it.get("gift") or {}
    try_paths = [
        lambda x: ((x.get("model") or {}).get("sticker") | {}).get("id") if isinstance(x.get("model"), dict) else None,
        lambda x: (x.get("sticker") or {}).get("id"),
    ]
    for fn in try_paths:
        try:
            v = fn(g)
        except Exception:
            v = None
        if isinstance(v, int):
            return v
    # запасной вариант — вдруг структура другая
    try_paths2 = [
        lambda x: ((x.get("model") or {}).get("sticker") or {}).get("id"),
        lambda x: (x.get("sticker") or {}).get("id"),
    ]
    for fn in try_paths2:
        v = fn(it)
        if isinstance(v, int):
            return v
    return None

def _extract_name_from_listing(it: Dict[str, Any]) -> Optional[str]:
    g = it.get("gift") or {}
    mdl = g.get("model") or {}
    name = mdl.get("name")
    if isinstance(name, str):
        name = name.strip()
        if name:
            return name
    return None

def _extract_price_from_listing(it: Dict[str, Any]) -> Optional[int]:
    g = it.get("gift") or {}
    p = g.get("resale_star_count")
    if isinstance(p, int):
        return p
    p2 = it.get("resale_star_count")
    return int(p2) if isinstance(p2, int) else None

# ─────────────────────────────
# Floor через СКАН ленты (и по doc_id, и по имени)
# ─────────────────────────────
def collect_floors_by_scan(
    client,
    gift_id: int,
    target_doc_ids: List[int],
    target_names: List[str],
    page_limit: int = PAGE_LIMIT,
    max_pages: int = MAX_PAGES_PER_GIFT,
) -> Tuple[Dict[int, Optional[int]], Dict[str, Optional[int]]]:
    """
    Идём по ленте перепродаж (order=price asc) страницами и фиксируем первую встреченную цену
    для каждого document_id и для каждого имени модели (на случай несовпадения id).
    Останавливаемся, когда закрыли ВСЕ модели (по id ИЛИ по имени).
    """
    wanted_ids = set(int(x) for x in target_doc_ids if isinstance(x, int))
    wanted_names = set(n for n in target_names if isinstance(n, str) and n.strip())

    floors_by_id: Dict[int, Optional[int]] = {}
    floors_by_name: Dict[str, Optional[int]] = {}

    offset = ""
    pages = 0

    def all_done() -> bool:
        # «достаточно», если у всех моделей есть цена либо по id, либо по имени
        ok = True
        for n in wanted_names:
            if (n not in floors_by_name) and (not any((d in floors_by_id) for d in wanted_ids)):
                ok = False
                break
        # Реально хватит требовать оба покрытия:
        ids_ok = (len(floors_by_id) >= len(wanted_ids)) if wanted_ids else True
        names_ok = (len(floors_by_name) >= len(wanted_names)) if wanted_names else True
        return ids_ok or names_ok

    while pages < max_pages and not all_done():
        rid = f"page_{gift_id}_{pages}_{_now_ms()}"
        req = {
            "@type": "searchGiftsForResale",
            "gift_id": gift_id,
            "order": {"@type": "giftForResaleOrderPrice"},
            "attributes": [],
            "offset": offset,
            "limit": page_limit,
            "@extra": {"rid": rid, "kind": "page", "gift_id": gift_id, "page": pages}
        }
        raw_dump(f"send_page_{gift_id}_p{pages}_{rid}", req, gift_id)
        td_send(client, req)

        upd = None
        while True:
            upd = td_recv(client, 20)
            if upd and upd.get("@type") == "giftsForResale":
                raw_dump(f"recv_page_{gift_id}_p{pages}_{rid}", upd, gift_id)
                break

        gifts = upd.get("gifts", []) or []
        next_offset = upd.get("next_offset") or ""

        for it in gifts:
            price = _extract_price_from_listing(it)
            if not isinstance(price, int):
                continue

            doc_id = _extract_doc_id_from_listing(it)
            if isinstance(doc_id, int) and (doc_id in wanted_ids) and (doc_id not in floors_by_id):
                floors_by_id[doc_id] = int(price)

            name = _extract_name_from_listing(it)
            if isinstance(name, str) and name and (name in wanted_names) and (name not in floors_by_name):
                floors_by_name[name] = int(price)

            if all_done():
                break

        pages += 1
        if all_done():
            break
        if not next_offset:
            break
        offset = next_offset

    # Заполним отсутствующие ключи None
    for d in wanted_ids:
        floors_by_id.setdefault(d, None)
    for n in wanted_names:
        floors_by_name.setdefault(n, None)

    return floors_by_id, floors_by_name

# ─────────────────────────────
# Основной поток
# ─────────────────────────────
def run_once(reset_done: bool = False) -> None:
    client = _td.td_json_client_create()
    out: List[Dict[str, Any]] = []
    processed = 0

    try:
        auth(client)
        save_snapshot(OUT_FILE, out)  # создадим файл заранее

        gifts = get_available_gifts(client) or []
        gifts = [g for g in gifts if (g.get("resale_count") or 0) > 0]

        # Прогноз количества моделей — для прогресса
        total_expected = None
        if SHOW_PROGRESS:
            total_expected = 0
            for ag in gifts:
                gid = (ag.get("gift") or {}).get("id")
                if not gid:
                    continue
                ms = get_models_for_gift_limit0(client, gid)
                total_expected += len(ms)
            print(f"[INFO] Всего моделей к обработке: {total_expected}")

        for gi, ag in enumerate(gifts, start=1):
            title = ag.get("title", "")
            gift_id = (ag.get("gift") or {}).get("id")
            resale_count = ag.get("resale_count", 0)
            if not gift_id:
                continue

            models = get_models_for_gift_limit0(client, gift_id)
            if SHOW_PROGRESS:
                print(f"[STEP] {gi}/{len(gifts)} — '{title}' (gift_id={gift_id}, resale_count={resale_count}, models={len(models)})")

            # Соберём doc_id и имена моделей (обе стратегии сопоставления)
            doc_ids: List[int] = []
            names: List[str] = []
            for m in models:
                d = _extract_doc_id(m)
                if isinstance(d, int):
                    doc_ids.append(d)
                name = _extract_model_name_from_model(m)
                if isinstance(name, str) and name:
                    names.append(name)

            floors_by_id, floors_by_name = collect_floors_by_scan(client, gift_id, doc_ids, names)

            # Формируем записи: сначала ищем цену по doc_id, если нет — по имени
            for m in models:
                mdl = m.get("model") or {}
                name = mdl.get("name", "")
                rpm = mdl.get("rarity_per_mille", 0)
                d = _extract_doc_id(m)

                price = None
                if isinstance(d, int):
                    price = floors_by_id.get(d)
                if not isinstance(price, int) and isinstance(name, str):
                    price = floors_by_name.get(name)

                if not isinstance(price, int):
                    continue  # пропускаем модели без цены

                item = {
                    "gift": title,
                    "gift_id": str(gift_id),
                    "model": name,
                    "rarity_per_mille": rarity_to_str(rpm),
                    "price": int(price),
                }
                if INCLUDE_STICKER_ID:
                    item["sticker_id"] = int(d) if isinstance(d, int) else None

                out.append(item)
                processed += 1
                print(f"[FOUND] gift='{title}' | model='{name}' | price={item['price']}")

                if processed % SAVE_EVERY == 0:
                    save_snapshot(OUT_FILE, out)

                pause(SLEEP_BETWEEN)

            if SHOW_PROGRESS and total_expected:
                print(f"[PROGRESS] models: {processed}/{total_expected} записаны (с ценой)")

        save_snapshot(OUT_FILE, out)
        if SHOW_PROGRESS:
            print(f"[OK] Сохранено {len(out)} записей в {OUT_FILE}")

    except RuntimeError as e:
        msg = str(e)
        if (not reset_done) and ("Valid api_id must be provided" in msg or ("'code': 400" in msg and "api_id" in msg)):
            print(f"[WARN] Ошибка api_id. Стираю БД и пробую заново: {DB_DIR}")
            try: shutil.rmtree(DB_DIR, ignore_errors=True)
            except Exception as ex: print("[WARN] Не смог удалить БД:", ex)
            _td.td_json_client_destroy(client)
            return run_once(True)
        raise
    finally:
        try: save_snapshot(OUT_FILE, out)
        except Exception: pass
        try: _td.td_json_client_destroy(client)
        except Exception: pass

def main():
    print(f"RUN: {__file__}")
    print(f"api_id={API_ID} ({type(API_ID)}), api_hash_len={len(API_HASH)}, tdlib_bin={TDLIB_BIN_DIR}")
    print(f"DB_DIR={DB_DIR}")
    os.makedirs(DB_DIR, exist_ok=True)
    run_once(reset_done=False)

if __name__ == "__main__":
    main()
