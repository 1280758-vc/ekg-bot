import os
import re
import logging
import time
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dateutil import tz
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
from contextlib import asynccontextmanager
from telegram import ReplyKeyboardMarkup, KeyboardButton, Update
from telegram.ext import Application, ContextTypes

# === НАЛАШТУВАННЯ ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
SHEET_ID = os.getenv("SHEET_ID")
CAL_ID = os.getenv("CAL_ID")
CREDS_S = "/etc/secrets/EKG_BOT_KEY"
CREDS_C = "/etc/secrets/CALENDAR_SERVICE_KEY"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/calendar.events"]
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

# === ЛОГІВАННЯ ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger(__name__)
log.info("Бот ініціалізований — початок роботи")

# === КОНСТАНТИ ===
LOCAL = tz.gettz('Europe/Kiev')
u, cache, reminded, last_rec, booked_slots, show_welcome = {}, {}, set(), {}, {}, {}
executor = ThreadPoolExecutor(max_workers=2)
lock = threading.Lock()

# === Telegram Application ===
application = Application.builder().token(BOT_TOKEN).build()

# === Клавіатури ===
main_kb = ReplyKeyboardMarkup([
    [KeyboardButton("Записатися на ЕКГ"), KeyboardButton("Скасувати запис")],
    [KeyboardButton("Список записів")]
], resize_keyboard=True)
cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("Скасувати")]], resize_keyboard=True)
gender_kb = ReplyKeyboardMarkup([[KeyboardButton("Чоловіча"), KeyboardButton("Жіноча")]], resize_keyboard=True)

def date_kb():
    today = datetime.now().strftime("%d.%m.%Y – Сьогодні")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y – Завтра")
    day_after = (datetime.now() + timedelta(days=2)).strftime("%d.%m.%Y – Післязавтра")
    return ReplyKeyboardMarkup([
        [KeyboardButton(today), KeyboardButton(tomorrow)],
        [KeyboardButton(day_after), KeyboardButton("Інша дата (ДД.ММ.РРРР)")],
        [KeyboardButton("Скасувати")]
    ], resize_keyboard=True)

email_kb = ReplyKeyboardMarkup([[KeyboardButton("Пропустити")]], resize_keyboard=True)

# === ВАЛІДАЦІЯ ===
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split()) >= 2 and all(re.match(r"^[А-ЯЁІЇЄҐ][а-яёіїєґ]+$", i) for i in p) else None
v_gender = lambda x: x.strip() if x.strip() in ["Чоловіча", "Жіноча"] else None
v_year = lambda x: int(x) if x.isdigit() and 1900 <= int(x) <= datetime.now().year else None
v_phone = lambda x: x.strip() if re.match(r"^380\d{9}$|^0\d{9}$", x.replace(" ", "")) else None
v_email = lambda x: x.strip() if x == "" or re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", x) else None

def v_date(x):
    x = x.strip()
    if "Сьогодні" in x: return datetime.now().date()
    if "Завтра" in x: return (datetime.now() + timedelta(days=1)).date()
    if "Післязавтра" in x: return (datetime.now() + timedelta(days=2)).date()
    try:
        if " – " in x: x = x.split(" – ")[0]
        d = datetime.strptime(x, "%d.%m.%Y").date()
        return d if d >= datetime.now().date() else None
    except: return None

# === КАЛЕНДАР ===
def get_events_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache and time.time() - cache[ds][1] < 300:
        return cache[ds][0]
    if not os.path.exists(CREDS_C):
        log.error(f"Ключ не знайдено: {CREDS_C}")
        return []
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES), cache_discovery=False)
        start = datetime.combine(d, datetime.min.time()).isoformat() + "Z"
        end = (datetime.combine(d, datetime.max.time()) - timedelta(seconds=1)).isoformat() + "Z"
        events = service.events().list(calendarId=CAL_ID, timeMin=start, timeMax=end, singleEvents=True).execute()
        events_list = events.get("items", [])
        cache[ds] = (events_list, time.time())
        log.info(f"get_events: {ds} — {len(events_list)} подій")
        return events_list
    except Exception as e:
        log.error(f"get_events error: {e}")
        return []

async def get_events(d):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, get_events_async, d)

async def is_slot_free(d, t):
    dt = datetime.combine(d, t).replace(tzinfo=LOCAL)
    start_check = dt - timedelta(minutes=60)
    end_check = dt + timedelta(minutes=60)

    with lock:
        for booked_dt in booked_slots.get(d.strftime("%Y-%m-%d"), []):
            if start_check <= booked_dt <= end_check:
                return False

    events = await get_events(d)
    for e in events:
        try:
            estart = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
            if start_check <= estart <= end_check:
                return False
        except: continue
    return True

async def free_slots_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache: del cache[ds]
   
