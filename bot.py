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

# НАЛАШТУВАННЯ
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
SHEET_ID = os.getenv("SHEET_ID")
CAL_ID = os.getenv("CAL_ID")
CREDS_S = "/etc/secrets/EKG_BOT_KEY"
CREDS_C = "/etc/secrets/CALENDAR_SERVICE_KEY"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/calendar.events"]
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

# ЛОГІВАННЯ
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger(__name__)
log.info("Бот ініціалізований — початок роботи 📡")

# FastAPI
app = FastAPI()

# КОНСТАНТИ
LOCAL = tz.gettz('Europe/Kiev')
u, cache, reminded, last_rec, booked_slots, show_welcome = {}, {}, set(), {}, {}, {}
executor = ThreadPoolExecutor(max_workers=2)
lock = threading.Lock()

# APPLICATION
application = Application.builder().token(BOT_TOKEN).build()

# КЛАВІАТУРИ
main_kb = ReplyKeyboardMarkup([
    [KeyboardButton("Записатися на ЕКГ 🎉"), KeyboardButton("Скасувати запис ❌")],
    [KeyboardButton("Список записів 📋"), KeyboardButton("Повторити запис 🔄")],
    [KeyboardButton("Редагувати запис ✏️")]
], resize_keyboard=True)
cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("Скасувати ❌")]], resize_keyboard=True)
gender_kb = ReplyKeyboardMarkup([[KeyboardButton("Чоловіча 🧑"), KeyboardButton("Жіноча 👩")]], resize_keyboard=True)

def date_kb():
    today = datetime.now().strftime("%d.%m.%Y – Сьогодні 📅")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y – Завтра 📅")
    day_after = (datetime.now() + timedelta(days=2)).strftime("%d.%m.%Y – Післязавтра 📅")
    log.info(f"date_kb: Кнопки: {today}, {tomorrow}, {day_after}")
    return ReplyKeyboardMarkup([
        [KeyboardButton(today), KeyboardButton(tomorrow)],
        [KeyboardButton(day_after), KeyboardButton("Інша дата (ДД.ММ.ЯЯЯЯ) 📅")],
        [KeyboardButton("Скасувати ❌")]
    ], resize_keyboard=True)

email_kb = ReplyKeyboardMarkup([[KeyboardButton("Пропустити ⏭️")]], resize_keyboard=True)

# ВАЛІДАЦІЯ
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split()) >= 2 and all(re.match(r"^[А-ЯЁІЇЄҐ][а-яёіїєґ]+$", i) for i in p) else None
v_gender = lambda x: re.sub(r'[^\w\s\u0400-\u04FF]', '', x).strip() if re.sub(r'[^\w\s\u0400-\u04FF]', '', x).strip() in ["Чоловіча", "Жіноча", "чоловіча", "жіноча"] else None
v_year = lambda x: int(x) if x.isdigit() and 1900 <= int(x) <= datetime.now().year else None
v_phone = lambda x: x.strip() if re.match(r"^380\d{9}$|^0\d{9}$", x.replace(" ", "")) else None
v_email = lambda x: x.strip() if x == "" or re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", x) else None

def v_date(x):
    log.info(f"v_date: '{x}'")
    x = x.strip()
    if "Сьогодні" in x:
        date_val = datetime.now().date()
        log.info(f"v_date: 'Сьогодні' → {date_val.strftime('%d.%m.%Y')}")
        return date_val
    if "Завтра" in x:
        date_val = (datetime.now() + timedelta(days=1)).date()
        log.info(f"v_date: 'Завтра' → {date_val.strftime('%d.%m.%Y')}")
        return date_val
    if "Післязавтра" in x:
        date_val = (datetime.now() + timedelta(days=2)).date()
        log.info(f"v_date: 'Післязавтра' → {date_val.strftime('%d.%m.%Y')}")
        return date_val
    try:
        if " – " in x: x = x.split(" – ")[0]
        date_val = datetime.strptime(x, "%d.%m.%Y").date()
        if date_val >= datetime.now().date():
            log.info(f"v_date: '{x}' → {date_val.strftime('%d.%m.%Y')}")
            return date_val
        log.warning(f"v_date: '{x}' в минулому")
        return None
    except ValueError as e:
        log.error(f"v_date: Помилка '{x}': {e}")
        return None

# КАЛЕНДАР
def get_events_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache and time.time() - cache[ds][1] < 300:
        log.info(f"get_events: Кеш для {ds}")
        return cache[ds][0]
    if not os.path.exists(CREDS_C):
        log.error(f"get_events: КЛЮЧ НЕ ЗНАЙДЕНО: {CREDS_C}")
        return []
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES), cache_discovery=False)
        start = datetime.combine(d, datetime.min.time()).isoformat() + "Z"
        end = (datetime.combine(d, datetime.max.time()) - timedelta(seconds=1)).isoformat() + "Z"
        events = service.events().list(calendarId=CAL_ID, timeMin=start, timeMax=end, singleEvents=True).execute(num_retries=3)
        events_list = events.get("items", [])
        cache[ds] = (events_list, time.time())
        log.info(f"get_events: {ds} — {len(events_list)} подій")
        return events_list
    except Exception as e:
        log.error(f"get_events: Помилка {ds}: {e}")
        return []

async def get_events(d):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, get_events_async, d)

async def free_60(d, t):
    dt = datetime.combine(d, t).replace(tzinfo=LOCAL)
    start_check = dt - timedelta(minutes=60)
    end_check = dt + timedelta(minutes=60)
    await get_events(d)
    events = cache.get(d.strftime("%Y-%m-%d"), [{}])[0]
    with lock:
        for booked_dt in booked_slots.get(d.strftime("%Y-%m-%d"), []):
            booked_start = booked_dt - timedelta(minutes=60)
            booked_end = booked_dt + timedelta(minutes=60)
            if start_check < booked_start < end_check or booked_start < dt < booked_end:
                log.debug(f"free_60: Зайнято booked {booked_dt}")
                return False
    for e in events:
        try:
            estart = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
            if start_check < estart < end_check:
                log.debug(f"free_60: Зайнято подія {e.get('summary', '—')} о {estart}")
                return False
        except Exception as e:
            log.error(f"free_60: Помилка події {e}")
            continue
    log.debug(f"free_60: Вільно {dt}")
    return True

async def free_slots_async(d):
    try:
        loop = asyncio.get_event_loop()
        ds = d.strftime("%Y-%m-%d")
        if ds in cache: del cache[ds]
        log.info(f"free_slots: Очищено кеш {ds}")
        start_time = datetime.strptime("09:00", "%H:%M").time()
        slots = []
        current = datetime.combine(d, start_time)
        end_time = datetime.strptime("18:00", "%H:%M").time()
        while current <= datetime.combine(d, end_time):
            if await free_60(d, current.time()):
                slots.append(current.strftime("%H:%M"))
            current += timedelta(hours=1)
        log.info(f"free_slots: {d.strftime('%d.%m.%Y')} — {slots}")
        return slots if slots else []
    except Exception as e:
        log.error(f"free_slots: Помилка {d.strftime('%d.%m.%Y')}: {e}")
        return []

# СКАСУВАННЯ
def cancel_record(chat_id, record_code=None):
    if chat_id in last_rec:
        if record_code:
            event_to_delete = next((r for r in last_rec[chat_id].values() if r.get("record_code") == record_code), None)
            if not event_to_delete:
                return False
