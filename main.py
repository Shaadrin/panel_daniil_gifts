# main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import uvicorn
import json
import sys
import shutil
import asyncio
from pathlib import Path


# BASE_DIR = Path(__file__).resolve().parent
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent

PARSERS_DIR = BASE_DIR / "gifts_parcers"
THERMOS_FILE = BASE_DIR / "thermos_gifts.json"
TG_FILE = BASE_DIR / "tg_gifts_resale.json"
# Исполняемые файлы парсеров (учитываем расширение под Windows)
EXE_SUFFIX = ".exe" if sys.platform.startswith("win") else ""
TG_PARSER = PARSERS_DIR / f"parce_tg_market_kurigram{EXE_SUFFIX}"
THERMOS_PARSER = PARSERS_DIR / f"parce_thermos_gifts{EXE_SUFFIX}"

app = FastAPI()

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

templates = Environment(loader=FileSystemLoader(BASE_DIR / "templates"))

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
        async def run_parser(path: Path) -> None:
            proc = await asyncio.create_subprocess_exec(str(path), cwd=PARSERS_DIR)
            await proc.wait()
            if proc.returncode:
                raise RuntimeError(f"{path.name} exited with code {proc.returncode}")

        # Запускаем парсеры последовательно
        await run_parser(TG_PARSER)
        await run_parser(THERMOS_PARSER)

        # Переносим результаты в корень панели
        src = PARSERS_DIR / "tg_gifts_resale.json"
        if src.exists():
            shutil.move(src, TG_FILE)

        src = PARSERS_DIR / "thermos_gifts.json"
        if src.exists():
            shutil.move(src, THERMOS_FILE)

    except Exception as exc:
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=500)
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    import sys, uvicorn
    is_exe = getattr(sys, "frozen", False)
    uvicorn.run(
        app,                    # ← передаём прямо объект
        host="127.0.0.1",
        port=8000,
        reload=False
    )
