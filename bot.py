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

# === FastAPI ===
app = FastAPI()

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

async def free_60(d, t):
    dt = datetime.combine(d, t).replace(tzinfo=LOCAL)
    start_check = dt - timedelta(minutes=60)
    end_check = dt + timedelta(minutes=60)
    await get_events(d)
    events = cache.get(d.strftime("%Y-%m-%d"), [{}])[0]
    with lock:
        for booked_dt in booked_slots.get(d.strftime("%Y-%m-%d"), []):
            if start_check < booked_dt < end_check:
                return False
    for e in events:
        try:
            estart = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
            if start_check < estart < end_check:
                return False
        except: continue
    return True

async def free_slots_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache: del cache[ds]
    slots = []
    current = datetime.combine(d, datetime.strptime("09:00", "%H:%M").time())
    while current.time() <= datetime.strptime("18:00", "%H:%M").time():
        if await free_60(d, current.time()):
            slots.append(current.strftime("%H:%M"))
        current += timedelta(hours=1)
    return slots

# === СКАСУВАННЯ ===
def cancel_record(chat_id, record_code=None):
    if chat_id not in last_rec: return False
    record = next((r for r in last_rec[chat_id].values() if r.get("record_code") == record_code), None) if record_code else list(last_rec[chat_id].values())[0]
    if not record: return False
    event_id = record["event_id"]
    dt = datetime.strptime(record["full_dt"], "%d.%m.%Y %H:%M").replace(tzinfo=LOCAL)
    if record_code:
        del last_rec[chat_id][next(k for k, v in last_rec[chat_id].items() if v["record_code"] == record_code)]
    else:
        last_rec.pop(chat_id, None)
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
        service.events().delete(calendarId=CAL_ID, eventId=event_id).execute()
        with lock:
            ds = dt.date().strftime("%Y-%m-%d")
            if ds in booked_slots and dt in booked_slots[ds]:
                booked_slots[ds].remove(dt)
        asyncio.create_task(application.bot.send_message(ADMIN_ID, f"Скасовано: {dt.strftime('%d.%m.%Y %H:%M')}"))
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
            service.spreadsheets().values().append(spreadsheetId=SHEET_ID, range="A1", valueInputOption="RAW",
                body={"values": [["Дата запису", "ПІБ", "Стать", "Р.н.", "Телефон", "Email", "Адреса", "Дата і час"]]}).execute()
    except Exception as e: log.error(f"init_sheet: {e}")

def add_sheet(data):
    if not os.path.exists(CREDS_S): return
    try:
        build("sheets", "v4", credentials=Credentials.from_service_account_file(CREDS_S, scopes=SCOPES)).spreadsheets().values().append(
            spreadsheetId=SHEET_ID, range="A:H", valueInputOption="RAW",
            body={"values": [[datetime.now().strftime("%d.%m.%Y %H:%M"), data["pib"], data["gender"], data["year"], data["phone"], data.get("email", ""), data["addr"], data["full"]]]}
        ).execute()
    except Exception as e: log.error(f"add_sheet: {e}")

def add_event(data):
    if not os.path.exists(CREDS_C): return False
    try:
        dt = datetime.combine(data["date"], data["time"]).replace(tzinfo=LOCAL)
        record_code = f"REC-{dt.strftime('%Y%m%d-%H%M')}"
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
        event = service.events().insert(calendarId=CAL_ID, body={
            "summary": f"ЕКГ: {data['pib']} ({data['phone']})",
            "location": data["addr"],
            "description": f"Email: {data.get('email', '—')}\nР.н.: {data['year']}\nСтать: {data['gender']}\nChat ID: {data['cid']}",
            "start": {"dateTime": (dt - timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"},
            "end": {"dateTime": (dt + timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"}
        }).execute()
        with lock:
            ds = data["date"].strftime("%Y-%m-%d")
            booked_slots.setdefault(ds, []).append(dt)
        last_rec.setdefault(data["cid"], {})[event["id"]] = {**record, "event_id": event["id"], "record_code": record_code}
        return True
    except Exception as e:
        log.error(f"add_event: {e}")
        asyncio.create_task(application.bot.send_message(ADMIN_ID, f"ПОМИЛКА: {e}"))
        return False

# === НАГАДУВАННЯ ===
async def check_reminders():
    now = datetime.now(LOCAL)
    for day in [now.date(), (now + timedelta(days=1)).date()]:
        events = await get_events(day)
        for e in events:
            try:
                start_dt = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
                mins_left = int((start_dt - now).total_seconds() // 60)
                eid = e["id"]
                if mins_left in [30, 10] and (eid, mins_left) not in reminded:
                    cid = int(re.search(r"Chat ID: (\d+)", e.get("description", "")).group(1)) if re.search(r"Chat ID: (\d+)", e.get("description", "")) else None
                    msg = f"НАГАДУВАННЯ! ЕКГ через {mins_left} хв\n{start_dt.strftime('%d.%m.%Y %H:%M')}\n{e['summary']}"
                    if cid: await application.bot.send_message(cid, msg)
                    await application.bot.send_message(ADMIN_ID, msg)
                    reminded.add((eid, mins_left))
            except: continue

# === ОБРОБКА ===
async def process_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global u, show_welcome
    msg = update.message
    if not msg: return
    chat_id = msg.chat_id
    text = msg.text.strip() if msg.text else ""
    log.info(f"Update: {chat_id} → {text}")

    if chat_id not in show_welcome:
        await msg.reply_text("Цей бот для запису на ЕКГ вдома!\nОберіть дію:", reply_markup=main_kb)
        show_welcome[chat_id] = True

    if text == "Скасувати":
        u.pop(chat_id, None)
        await msg.reply_text("Скасовано.", reply_markup=main_kb)
        return

    if text == "Скасувати запис":
        if chat_id not in last_rec:
            await msg.reply_text("Немає записів.", reply_markup=main_kb)
            return
        reply = "Ваші записи:\n" + "\n".join(f"{r['record_code']} — {r['full_dt']}" for r in last_rec[chat_id].values())
        reply += "\nВведіть код для скасування:"
        await msg.reply_text(reply, reply_markup=cancel_kb)
        return

    if text.startswith("REC-") and chat_id in last_rec and any(text == r["record_code"] for r in last_rec[chat_id].values()):
        if cancel_record(chat_id, text):
            await msg.reply_text(f"Запис {text} скасовано!", reply_markup=main_kb)
        else:
            await msg.reply_text("Помилка.", reply_markup=main_kb)
        return

    if text in ["/start", "Записатися на ЕКГ"]:
        u[chat_id] = {"step": "pib", "cid": chat_id}
        await msg.reply_text("ПІБ (Прізвище Ім'я По батькові):", reply_markup=cancel_kb)
        return

    if chat_id not in u: return
    data = u[chat_id]
    step = data["step"]

    steps = {
        "pib": (v_pib, "gender", "Стать:", gender_kb),
        "gender": (v_gender, "year", "Рік народження:", cancel_kb),
        "year": (v_year, "phone", "Телефон:", cancel_kb),
        "phone": (v_phone, "email", "Email (або Пропустити):", email_kb),
        "email": (v_email, "addr", "Адреса:", cancel_kb),
        "addr": (lambda x: x.strip(), "date", "Дата:", date_kb())
    }

    if step in steps:
        val = steps[step][0](text)
        if val is not None or (step == "email" and text == "Пропустити"):
            data[step] = val if val is not None else ""
            data["step"] = steps[
