# main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import uvicorn
import json

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Environment(loader=FileSystemLoader("templates"))

# Здесь можно будет позже подтягивать реальные данные
DATA_FILE = "gifts_data.json"

def load_data():
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    data = load_data()
    template = templates.get_template("index.html")
    return template.render(request=request, gifts=data)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
