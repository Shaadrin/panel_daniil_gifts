# main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import uvicorn
import json
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PARSERS_DIR = BASE_DIR / "gifts_parcers"
THERMOS_FILE = PARSERS_DIR / "thermos_gifts.json"
TG_FILE = BASE_DIR / "tg_gifts_resale.json"


app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Environment(loader=FileSystemLoader("templates"))

# Здесь можно будет позже подтягивать реальные данные
DATA_FILE = "gifts_data.json"

def load_data():
    thermos_data = []
    tg_data = []
    if THERMOS_FILE.exists():
        with open(THERMOS_FILE, "r", encoding="utf-8") as f:
            thermos_data = json.load(f)
    if TG_FILE.exists():
        with open(TG_FILE, "r", encoding="utf-8") as f:
            tg_data = json.load(f)

    thermos_map = {(g["gift"], g["model"]): g.get("price") for g in thermos_data}
    tg_map = {(g["gift"], g["model"]): g.get("price") for g in tg_data}

    keys = sorted(set(thermos_map) | set(tg_map))
    result = []
    for gift, model in keys:
        t_price = thermos_map.get((gift, model))
        tg_price = tg_map.get((gift, model))
        if t_price and tg_price:
            delta = round((tg_price - t_price) / t_price * 100, 2)
        else:
            delta = None
        result.append({
            "tg_name": f"{gift} — {model}",
            "thermos_price": t_price,
            "tgmarket_price": tg_price,
            "delta_percent": delta
        })
    return result

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    data = load_data()
    template = templates.get_template("index.html")
    return template.render(request=request, gifts=data)


@app.post("/update", response_class=HTMLResponse)
async def update(request: Request):
    try:
        subprocess.run(["python", "gifts_parcers/parce_thermos_gifts.py"], cwd=BASE_DIR, check=True)
        subprocess.run(["python", "gifts_parcers/parce_tg_market_kurigram.py"], cwd=BASE_DIR, check=True)
    except subprocess.CalledProcessError as exc:
        return HTMLResponse(
            content=f"Ошибка при обновлении данных: {exc}", status_code=500
        )
    data = load_data()
    template = templates.get_template("index.html")
    return template.render(request=request, gifts=data)


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
