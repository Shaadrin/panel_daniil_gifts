#!/usr/bin/env python3
import re
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_URL = "https://proxy.thermos.gifts/api/v1/attributes"

COLLECTIONS = ["Plush Pepe","Heart Locket","Durov's Cap","Precious Peach","Heroic Helmet",
               "Nail Bracelet","Loot Bag","Astral Shard","Mini Oscar","Perfume Bottle",
               "Ion Gem","Gem Signet","Westside Sign","Magic Potion","Bonded Ring","Scared Cat",
               "Genie Lamp","Sharp Tongue","Low Rider","Swiss Watch","Kissed Frog",
               "Electric Skull","Neko Helmet","Signet Ring","Vintage Cigar","Diamond Ring",
               "Toy Bear","Voodoo Doll","Mad Pumpkin","Eternal Rose","Cupid Charm","Top Hat",
               "Love Potion","Flying Broom","Record Player","Love Candle","Sleigh Bell","Crystal Ball",
               "Trapped Heart","Skull Flower","Snoop Cigar","Hanging Star","Sakura Flower",
               "Valentine Box","Evil Eye","Berry Box","Eternal Candle","Bunny Muffin",
               "Snow Mittens","Spy Agaric","Bow Tie","Jelly Bunny","Snow Globe","Light Sword",
               "Witch Hat","Hex Pot","Easter Egg","Star Notepad","Joyful Bundle","Lush Bouquet",
               "Jack-in-the-Box","Restless Jar","Swag Bag","Spiced Wine","Cookie Heart","Tama Gadget",
               "Hypno Lollipop","Winter Wreath","Big Year","Santa Hat","Jingle Bells","Ginger Cookie",
               "Holiday Drink","Jester Hat","Snoop Dogg","Party Sparkler","Candy Cane","Homemade Cake",
               "Lol Pop","Pet Snake","Snake Box","Xmas Stocking","Lunar Snake","Whip Cupcake",
               "B-Day Candle","Desk Calendar"]

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://thermos.gifts",
    "Referer": "https://thermos.gifts/",
    "User-Agent": "Mozilla/5.0 (compatible; gifts-scraper/1.0)"
}

PROXY: Optional[str] = None  # "http://user:pass@host:port"

def _default_out_path() -> str:
    name = f"thermos_gifts.json"
    out_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
    return str(out_dir / name)

def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(str(x).strip()))
        except Exception:
            return None

def _rarity_to_excel(v: Any) -> Optional[str]:
    """
    23 -> '2,3' (делим на 10, ',' как десятичный),
    список/кортеж из 2 значений -> 'a,b',
    иначе пытаемся извлечь целое и делим на 10.
    """
    if v is None:
        return None
    if isinstance(v, (list, tuple)) and len(v) >= 2:
        return f"{v[0]},{v[1]}"
    s = str(v)
    m = re.search(r"-?\d+", s)
    if m:
        n = int(m.group(0))
        return f"{n/10:.1f}".replace(".", ",")
    return s

def fetch_attributes(collections: List[str]) -> Dict[str, Any]:
    proxies = {"http": PROXY, "https": PROXY} if PROXY else None
    r = requests.post(API_URL, json={"collections": collections},
                      headers=HEADERS, proxies=proxies, timeout=60, verify=False)
    r.raise_for_status()
    return r.json()

def parse_and_group(payload: Dict[str, Any]) -> Dict[str, Dict[str, Tuple[Optional[float], Optional[str]]]]:
    """
    -> { gift: { model: (min_price, rarity_str_first) } }
    """
    groups: Dict[str, Dict[str, Tuple[Optional[float], Optional[str]]]] = {}
    for gift, sections in (payload or {}).items():
        if not isinstance(sections, dict):
            continue
        models = sections.get("models") or []
        g = groups.setdefault(gift, {})
        for it in models:
            stats = it.get("stats") or {}
            floor_raw = _to_int(stats.get("floor"))
            price = round(float(floor_raw) / 1e9, 2) if floor_raw is not None else None
            rarity = _rarity_to_excel(it.get("rarity_per_mille", it.get("rarity_per_mile")))
            model = it.get("name")
            if not model:
                continue

            if model not in g:
                g[model] = (price, rarity)
            else:
                old_price, old_rarity = g[model]
                # оставляем минимальную цену; редкость — первая
                if old_price is None:
                    best_price = price
                elif price is None:
                    best_price = old_price
                else:
                    best_price = min(old_price, price)
                g[model] = (best_price, old_rarity)
    return groups

def write_json(groups: Dict[str, Dict[str, Tuple[Optional[float], Optional[str]]]], out_path: str) -> str:
    """
    Пишем плоский список строк (как в Excel):
    [{gift, model, rarity_per_mille, price}, ...]
    """
    rows: List[Dict[str, Any]] = []
    for gift in sorted(groups.keys()):
        items = []
        for model, (price, rarity) in groups[gift].items():
            items.append((model, rarity, price))
        # сортируем по цене (None в конец)
        items.sort(key=lambda t: (t[2] is None, t[2]))
        for model, rarity, price in items:
            rows.append({
                "gift": gift,
                "model": model,
                "rarity_per_mille": rarity,
                "price": price
            })

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return out_path

def main():
    data = fetch_attributes(COLLECTIONS)
    groups = parse_and_group(data)
    if not groups:
        print("Пусто: сервер вернул 0 моделей.")
        return
    out_path = _default_out_path()
    write_json(groups, out_path)
    print(f"Готово: {sum(len(v) for v in groups.values())} строк → {out_path}")

if __name__ == "__main__":
    main()
