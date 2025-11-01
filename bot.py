# bot.py — WEBHOOK + FastAPI + Render (v21.5 + bot description + spam filter)
import os
import re
import logging
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

# === ІМПОРТИ TELEGRAM v21+ ===
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
logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# === FastAPI ===
app = FastAPI()

# === КОНСТАНТИ ===
LOCAL = tz.gettz('Europe/Kiev')
u, cache, reminded, last_rec, booked_slots, show_welcome = {}, {}, set(), {}, {}, {}  # show_welcome для відстеження
executor = ThreadPoolExecutor(max_workers=2)
lock = threading.Lock()

# === APPLICATION ===
application = Application.builder().token(BOT_TOKEN).build()

# === КЛАВІАТУРИ ===
main_kb = ReplyKeyboardMarkup([
    [KeyboardButton("Записатися на ЕКГ"), KeyboardButton("Скасувати запис")]
], resize_keyboard=True)

cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("Скасувати")]], resize_keyboard=True)
gender_kb = ReplyKeyboardMarkup([[KeyboardButton("Чоловіча"), KeyboardButton("Жіноча")]], resize_keyboard=True)

def date_kb():
    today = datetime.now().strftime("%d.%m – Сьогодні")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%d.%m – Завтра")
    day_after = (datetime.now() + timedelta(days=2)).strftime("%d.%m – Післязавтра")
    return ReplyKeyboardMarkup([
        [KeyboardButton(today), KeyboardButton(tomorrow)],
        [KeyboardButton(day_after), KeyboardButton("Інша дата (ДД.ММ)")],
        [KeyboardButton("Скасувати")]
    ], resize_keyboard=True)

email_kb = ReplyKeyboardMarkup([[KeyboardButton("Пропустити")]], resize_keyboard=True)

# === ВАЛИДАЦІЯ ===
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split())==3 and all(re.match(r"^[А-ЯЁІ ЇЄҐ][а-яёіїєґ]+$",i) for i in p) else None
v_gender = lambda x: x if x in ["Чоловіча","Жіноча"] else None
v_year = lambda x: int(x) if x.isdigit() and 1900 <= int(x) <= datetime.now().year else None
v_phone = lambda x: x.strip() if re.match(r"^(\+380|0)\d{9}$", x.replace(" ","")) else None
v_email = lambda x: x.strip() if x == "" or re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", x) else None
v_date = lambda x: (
    datetime.now().date() if "Сьогодні" in x else
    (datetime.now() + timedelta(days=1)).date() if "Завтра" in x else
    (datetime.now() + timedelta(days=2)).date() if "Післязавтра" in x else
    datetime.strptime(x.strip(),"%d.%m").replace(year=datetime.now().year).date()
    if datetime.strptime(x.strip(),"%d.%m").replace(year=datetime.now().year).date() >= datetime.now().date() else None
)

# === КАЛЕНДАР ===
def get_events_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache and time.time() - cache[ds][1] < 300:
        return cache[ds][0]
    if not os.path.exists(CREDS_C):
        log.error(f"КЛЮЧ НЕ ЗНАЙДЕНО: {CREDS_C}")
        return []
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES), cache_discovery=False)
        start = datetime.combine(d, datetime.min.time()).isoformat() + "Z"
        end = (datetime.combine(d, datetime.max.time()) - timedelta(seconds=1)).isoformat() + "Z"
        events = service.events().list(calendarId=CAL_ID, timeMin=start, timeMax=end, singleEvents=True).execute(num_retries=2)
        cache[ds] = (events.get("items", []), time.time())
        log.info(f"Кеш оновлено для {ds}: {len(events.get('items', []))} подій")
        return cache[ds][0]
    except Exception as e:
        log.error(f"get_events: {e}")
        return []

async def get_events(d):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, get_events_async, d)

def free_60(d, t):
    dt = datetime.combine(d, t).replace(tzinfo=LOCAL)
    start_check = dt - timedelta(minutes=30)
    end_check = dt + timedelta(minutes=30)
    asyncio.run(get_events(d))
    events = cache.get(d.strftime("%Y-%m-%d"), [{}])[0]
    with lock:
        for booked_dt in booked_slots.get(d.strftime("%Y-%m-%d"), []):
            booked_start = booked_dt - timedelta(minutes=30)
            booked_end = booked_dt + timedelta(minutes=30)
            if start_check < booked_start < end_check or booked_start < dt < booked_end:
                return False
    for e in events:
        try:
            estart = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
            if start_check < estart < end_check:
                return False
        except Exception as e:
            log.error(f"free_60: Помилка обробки події {e}")
            continue
    return True

async def free_slots_async(d):
    try:
        loop = asyncio.get_event_loop()
        slots = await loop.run_in_executor(executor, lambda: [
            cur.strftime("%H:%M") for cur in (
                datetime.combine(d, datetime.strptime("09:00", "%H:%M").time()),
                *[
                    cur + timedelta(minutes=15) for cur in [
                        datetime.combine(d, datetime.strptime("09:00", "%H:%M").time())
                        for _ in range(36)
                    ][1:]
                ]
            ) if cur <= datetime.combine(d, datetime.strptime("18:00", "%H:%M").time()) and asyncio.run_coroutine_threadsafe(free_60(d, cur.time()), loop).result()
        ])
        log.info(f"Знайдено слоти для {d.strftime('%d.%m')}: {slots}")
        return slots if slots else []
    except Exception as e:
        log.error(f"free_slots_async: Помилка {e}")
        return []

# === СКАСУВАННЯ ===
def cancel_record(cid):
    if cid in last_rec and os.path.exists(CREDS_C):
        try:
            service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
            event_id = last_rec[cid]["event_id"]
            dt = datetime.strptime(last_rec[cid]["full_dt"], "%d.%m %H:%M").replace(tzinfo=LOCAL)
            service.events().delete(calendarId=CAL_ID, eventId=event_id).execute()
            with lock:
                ds = dt.date().strftime("%Y-%m-%d")
                if ds in booked_slots:
                    booked_slots[ds].remove(dt)
                    if not booked_slots[ds]:
                        del booked_slots[ds]
            asyncio.create_task(application.bot.send_message(ADMIN_ID, f"Скасовано запис: {last_rec[cid]['full_dt']}"))
            last_rec.pop(cid, None)
            return True
        except Exception as e:
            log.error(f"cancel_record: {e}")
    return False

# === ЗАПИС ===
def init_sheet():
    if not os.path.exists(CREDS_S): return
    try:
        service = build("sheets", "v4", credentials=Credentials.from_service_account_file(CREDS_S, scopes=SCOPES))
        values = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range="A1:H1").execute().get("values", [])
        if not values or values[0] != ["Дата запису", "ПІБ", "Стать", "Р.н.", "Телефон", "Email", "Адреса", "Дата і час"]:
            service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID, range="A1", valueInputOption="RAW",
                body={"values": [["Дата запису", "ПІБ", "Стать", "Р.н.", "Телефон", "Email", "Адреса", "Дата і час"]]}
            ).execute()
    except Exception as e:
        log.error(f"init_sheet: {e}")

def add_sheet(data):
    if not os.path.exists(CREDS_S): return
    try:
        build("sheets","v4",credentials=Credentials.from_service_account_file(CREDS_S,scopes=SCOPES)).spreadsheets().values().append(
            spreadsheetId=SHEET_ID, range="A:H", valueInputOption="RAW",
            body={"values": [[datetime.now().strftime("%d.%m.%Y %H:%M"), data["pib"], data["gender"], data["year"], data["phone"], data.get("email",""), data["addr"], data["full"]]]}
        ).execute()
    except Exception as e:
        log.error(f"add_sheet: {e}")

def add_event(data):
    if not os.path.exists(CREDS_C): return False
    try:
        dt = datetime.combine(data["date"], data["time"]).replace(tzinfo=LOCAL)
        service = build("calendar","v3",credentials=Credentials.from_service_account_file(CREDS_C,scopes=SCOPES))
        event = service.events().insert(calendarId=CAL_ID, body={
            "summary": f"ЕКГ: {data['pib']} ({data['phone']})",
            "location": data["addr"],
            "description": f"Email: {data.get('email','—')}\nР.н.: {data['year']}\nСтать: {data['gender']}\nChat ID: {data['cid']}",
            "start": {"dateTime": (dt - timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"},
            "end": {"dateTime": (dt + timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"}
        }).execute()
        with lock:
            ds = data["date"].strftime("%Y-%m-%d")
            if ds not in booked_slots:
                booked_slots[ds] = []
            booked_slots[ds].append(dt)
        last_rec[data['cid']] = {"event_id": event["id"], "full_dt": data["full"]}
        return True
    except Exception as e:
        log.error(f"add_event: {e}")
        asyncio.create_task(application.bot.send_message(ADMIN_ID, f"ПОМИЛКА КАЛЕНДАРЯ: {e}"))
        return False

# === НАГАДУВАННЯ ===
async def check_reminders():
    now = datetime.now(LOCAL)
    for day in [now.date(), (now + timedelta(days=1)).date()]:
        for e in await get_events(day):
            try:
                start_str = e["start"]["dateTime"]
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(LOCAL)
                mins_left = int((start_dt - now).total_seconds() // 60)
                eid = e["id"]
                if mins_left in [30, 10] and (eid, mins_left) not in reminded:
                    desc = e.get("description", "")
                    cid_match = re.search(r"Chat ID: (\d+)", desc)
                    cid = int(cid_match.group(1)) if cid_match else None
                    msg = f"НАГАДУВАННЯ!\nЕКГ через {mins_left} хв\n{e['summary']}\nЧас: {start_dt.strftime('%H:%M')}"
                    if cid: await application.bot.send_message(cid, msg)
                    await application.bot.send_message(ADMIN_ID, f"НАГАДУВАННЯ:\n{msg}")
                    reminded.add((eid, mins_left))
            except: continue

# === ОБРОБКА ===
async def process_update(update: Update, context: Context
