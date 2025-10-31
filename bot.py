# bot.py — WEBHOOK + FastAPI + Render (v20.8 + Python 3.12)
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

# === ІМПОРТИ TELEGRAM ===
from telegram import ReplyKeyboardMarkup, KeyboardButton, Update
from telegram.ext import ApplicationBuilder, ContextTypes

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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# === FastAPI ===
app = FastAPI()

# === КОНСТАНТИ ===
LOCAL = tz.gettz('Europe/Kiev')
u, cache, reminded, last_rec = {}, {}, set(), {}

# === APPLICATION ===
application = ApplicationBuilder().token(BOT_TOKEN).build()

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

# === ВАЛИДАЦІЯ ===
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split())==3 and all(re.match(r"^[А-ЯЁІ ЇЄҐ][а-яёіїєґ]+$",i) for i in p) else None
v_gender = lambda x: x if x in ["Чоловіча","Жіноча"] else None
v_year = lambda x: int(x) if x.isdigit() and 1900 <= int(x) <= datetime.now().year else None
v_phone = lambda x: x.strip() if re.match(r"^(\+380|0)\d{9}$", x.replace(" ","")) else None
v_email = lambda x: x.strip() if x and re.match(r"^[a-zA-Z0.9._%+-]+@[a-zA-Z0.9.-]+\.[a-zA-Z]{2,}$", x) else ""
v_date = lambda x: (
    datetime.now().date() if "Сьогодні" in x else
    (datetime.now() + timedelta(days=1)).date() if "Завтра" in x else
    (datetime.now() + timedelta(days=2)).date() if "Післязавтра" in x else
    datetime.strptime(x.strip(),"%d.%m").replace(year=datetime.now().year).date()
    if datetime.strptime(x.strip(),"%d.%m").replace(year=datetime.now().year).date() >= datetime.now().date() else None
)

# === КАЛЕНДАР ===
def get_events(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache and time.time() - cache[ds][1] < 300:
        return cache[ds][0]
    if not os.path.exists(CREDS_C):
        log.error(f"КЛЮЧ НЕ ЗНАЙДЕНО: {CREDS_C}")
        return []
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
        start = datetime.combine(d, datetime.min.time()).isoformat() + "Z"
        end = (datetime.combine(d, datetime.max.time()) - timedelta(seconds=1)).isoformat() + "Z"
        events = service.events().list(calendarId=CAL_ID, timeMin=start, timeMax=end, singleEvents=True).execute().get("items", [])
        cache[ds] = (events, time.time())
        return events
    except Exception as e:
        log.error(f"get_events: {e}")
        return []

def free_60(d, t):
    dt = datetime.combine(d, t).replace(tzinfo=LOCAL)
    start_check = dt - timedelta(minutes=30)
    end_check = dt + timedelta(minutes=30)
    for e in get_events(d):
        try:
            estart = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
            if start_check < estart < end_check:
                return False
        except: continue
    return True

def free_slots(d):
    slots = []
    cur = datetime.combine(d, datetime.strptime("09:00", "%H:%M").time())
    end = datetime.combine(d, datetime.strptime("18:00","%H:%M").time())
    while cur <= end:
        if free_60(d, cur.time()):
            slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=15)
    return slots

# === СКАСУВАННЯ ===
def cancel_record(cid):
    if cid in last_rec and os.path.exists(CREDS_C):
        try:
            service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
            service.events().delete(calendarId=CAL_ID, eventId=last_rec[cid]["event_id"]).execute()
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
        for e in get_events(day):
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
async def process_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global u
    msg = update.message
    if not msg: return
    chat_id = msg.chat_id
    text = msg.text.strip() if msg.text else ""

    if text == "Скасувати":
        u.pop(chat_id, None)
        await msg.reply_text("Скасовано.", reply_markup=main_kb)
        return

    if text == "Скасувати запис":
        if cancel_record(chat_id):
            await msg.reply_text("Запис скасовано!", reply_markup=main_kb)
        else:
            await msg.reply_text("Запис не знайдено", reply_markup=main_kb)
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
        "phone": (v_phone, "email", "Email (можна пропустити):", cancel_kb),
        "email": (v_email, "addr", "Адреса:", cancel_kb),
        "addr": (lambda x: x.strip(), "date", "Дата:", date_kb())
    }

    if step in steps:
        val = steps[step][0](text)
        if val is not None:
            data[step] = val
            data["step"] = steps[step][1]
            await msg.reply_text(steps[step][2], reply_markup=steps[step][3])
        else:
            if step == "email" and (text == "" or text == "Скасувати"):
                data[step] = ""
                data["step"] = "addr"
                await msg.reply_text("Адреса:", reply_markup=cancel_kb)
            else:
                await msg.reply_text("Невірно", reply_markup=cancel_kb)
        return

    if step == "date":
        date_val = v_date(text)
        if date_val:
            data["date"] = date_val
            data["step"] = "time"
            slots = free_slots(date_val)
            await msg.reply_text(f"Вільно {date_val.strftime('%d.%m')} (60 хв):\n" + ("\n".join(f"• {s}" for s in slots) if slots else "• Немає") + "\n\nВведіть час (09:00–18:00):", reply_markup=cancel_kb)
        else:
            await msg.reply_text("Невірна дата", reply_markup=date_kb())

    if step == "time":
        try:
            time_val = datetime.strptime(text.strip(), "%H:%M").time()
            if not (datetime.strptime("09:00","%H:%M").time() <= time_val <= datetime.strptime("18:00","%H:%M").time()):
                raise ValueError
            if free_60(data["date"], time_val):
                full = f"{data['date'].strftime('%d.%m')} {text}"
                conf = f"Запис:\nПІБ: {data['pib']}\nСтать: {data['gender']}\nР.н.: {data['year']}\nТел: {data['phone']}\nEmail: {data.get('email','—')}\nАдреса: {data['addr']}\nЧас: {full} (±30 хв)"
                await msg.reply_text(f"{conf}\n\nДякую за запис!", reply_markup=main_kb)
                await application.bot.send_message(ADMIN_ID, f"НОВИЙ ЗАПИС!\n{conf}")
                add_event({**data, "time": time_val, "cid": chat_id, "full": full})
                add_sheet({**data, "full": full})
                u.pop(chat_id, None)
            else:
                await msg.reply_text("Зайнято (±30 хв)", reply_markup=cancel_kb)
        except:
            await msg.reply_text("Формат: ЧЧ:ХХ", reply_markup=cancel_kb)

# === WEBHOOK ===
@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    json_data = await request.json()
    update = Update.de_json(json_data, application.bot)
    asyncio.create_task(process_update(update, None))
    return JSONResponse({"ok": True})

@app.get(WEBHOOK_PATH)
async def webhook_get():
    return JSONResponse({"ok": True, "message": "Webhook active"})

# === НАГАДУВАННЯ В ФОНІ ===
async def reminder_loop():
    while True:
        await check_reminders()
        await asyncio.sleep(60)

# === ЗАПУСК ===
@app.on_event("startup")
async def startup():
    log.info("Бот запущено!")
    init_sheet()
    await application.initialize()
    await application.start()
    url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
    await application.bot.set_webhook(url=url)
    log.info(f"Webhook встановлено: {url}")
    asyncio.create_task(reminder_loop())

@app.on_event("shutdown")
async def shutdown():
    await application.stop()
    await application.shutdown()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
