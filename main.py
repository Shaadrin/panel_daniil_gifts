# main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import uvicorn
import json
import subprocess
import sys
import shutil
import asyncio
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
PARSERS_DIR = BASE_DIR / "gifts_parcers"
THERMOS_FILE = BASE_DIR / "thermos_gifts.json"
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

    # Show only gifts present in both sources
    keys = sorted(set(thermos_map) & set(tg_map))
    result = []
    for gift, model in keys:
        t_price = thermos_map[(gift, model)]
        tg_price = tg_map[(gift, model)]

        result.append(
            {
                "tg_name": f"{gift} — {model}",
                "thermos_price": t_price,
                "tgmarket_price": tg_price,
            }
        )
    return result

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    data = load_data()
    gifs_dir = BASE_DIR / "static" / "gifs"
    gif_files = []
    if gifs_dir.exists():
        gif_files = [f"/static/gifs/{p.name}" for p in gifs_dir.iterdir() if p.suffix.lower() == ".gif"]
    template = templates.get_template("index.html")
    return template.render(
        request=request,
        gifts=data,
        gif_files=json.dumps(gif_files),
    )



@app.post("/update")
async def update(request: Request):
    try:
        await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "gifts_parcers/parce_tg_market_kurigram.py"],
            cwd=BASE_DIR,
            check=True,
        )
        await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "gifts_parcers/parce_thermos_gifts.py"],
            cwd=BASE_DIR,
            check=True,
        )
        src = PARSERS_DIR / "thermos_gifts.json"
        if src.exists():
            shutil.move(src, THERMOS_FILE)

    except subprocess.CalledProcessError as exc:
        return JSONResponse(
            {"status": "error", "detail": str(exc)}, status_code=500
        )
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
