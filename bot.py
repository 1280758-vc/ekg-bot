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
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
from contextlib import asynccontextmanager
from telegram import ReplyKeyboardMarkup, KeyboardButton, Update
from telegram.ext import Application, ContextTypes
import uvicorn

# ==================== НАЛАШТУВАННЯ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
SHEET_ID = os.getenv("SHEET_ID")
CAL_ID = os.getenv("CAL_ID")
CREDS_S = "/etc/secrets/EKG_BOT_KEY"
CREDS_C = "/etc/secrets/CALENDAR_SERVICE_KEY"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/calendar.events"]
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

# ==================== ЛОГІВАННЯ ====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
log = logging.getLogger(__name__)
log.info("Бот ініціалізований — початок роботи")

# ==================== КОНСТАНТИ ====================
LOCAL = tz.gettz('Europe/Kiev')
u, cache, reminded, last_rec, booked_slots, show_welcome = {}, {}, set(), {}, {}, {}
executor = ThreadPoolExecutor(max_workers=2)
lock = threading.Lock()

# ==================== APPLICATION ====================
application = Application.builder().token(BOT_TOKEN).build()

# ==================== КЛАВІАТУРИ ====================
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
        [KeyboardButton(day_after), KeyboardButton("Інша дата (ДД.ММ.ЯЯЯЯ)")],
        [KeyboardButton("Скасувати")]
    ], resize_keyboard=True)

email_kb = ReplyKeyboardMarkup([[KeyboardButton("Пропустити")]], resize_keyboard=True)

# ==================== ВАЛІДАЦІЯ ДАТИ ====================
def v_date(x):
    x = x.strip()
    if "Сьогодні" in x: return datetime.now().date()
    if "Завтра" in x: return (datetime.now() + timedelta(days=1)).date()
    if "Післязавтра" in x: return (datetime.now() + timedelta(days=2)).date()
    try:
        if " – " in x: x = x.split(" – ")[0]
        d = datetime.strptime(x, "%d.%m.%Y").date()
        return d if d >= datetime.now().date() else None
    except:
        return None

# ==================== КАЛЕНДАР ====================
def get_events_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache and time.time() - cache[ds][1] < 300:
        return cache[ds][0]
    if not os.path.exists(CREDS_C):
        return []
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES), cache_discovery=False)
        start = datetime.combine(d, datetime.min.time()).isoformat() + "Z"
        end = (datetime.combine(d, datetime.max.time()) - timedelta(seconds=1)).isoformat() + "Z"
        events = service.events().list(calendarId=CAL_ID, timeMin=start, timeMax=end, singleEvents=True).execute()
        el = events.get("items", [])
        cache[ds] = (el, time.time())
        return el
    except Exception as e:
        log.error(f"get_events error: {e}")
        return []

async def get_events(d):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, get_events_async, d)

# ВИПРАВЛЕНО: правильні дужки
async def free_60(d, t):
    dt = datetime.combine(d, t).replace(tzinfo=LOCAL)
    await get_events(d)
    events = cache.get(d.strftime("%Y-%m-%d"), [{}])[0]
    with lock:
        for b in booked_slots.get(d.strftime("%Y-%m-%d"), []):
            if abs((dt - b).total_seconds()) < 3600:
                return False
    for e in events:
        try:
            estart = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
            if abs((dt - estart).total_seconds()) < 3600:
                return False
        except:
            continue
    return True

async def free_slots_async(d):
    ds = d.strftime("%Y-%m-%d")
    if ds in cache:
        del cache[ds]
    slots = []
    current = datetime.combine(d, datetime.strptime("09:00", "%H:%M").time())
    while current.time() <= datetime.strptime("18:00", "%H:%M").time():
        if await free_60(d, current.time()):
            slots.append(current.strftime("%H:%M"))
        current += timedelta(hours=1)
    return slots

# ==================== СКАСУВАННЯ ====================
def cancel_record(chat_id, record_code=None):
    if chat_id not in last_rec:
        return False
    if record_code:
        rec = next((r for r in last_rec[chat_id].values() if r["record_code"] == record_code), None)
        if not rec:
            return False
        event_id = rec["event_id"]
        dt = datetime.strptime(rec["full_dt"], "%d.%m.%Y %H:%M").replace(tzinfo=LOCAL)
        last_rec[chat_id] = {k: v for k, v in last_rec[chat_id].items() if v.get("record_code") != record_code}
    else:
        event_id = list(last_rec[chat_id].values())[0]["event_id"]
        dt = datetime.strptime(list(last_rec[chat_id].values())[0]["full_dt"], "%d.%m.%Y %H:%M").replace(tzinfo=LOCAL)
        last_rec.pop(chat_id, None)

    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
        service.events().delete(calendarId=CAL_ID, eventId=event_id).execute()
        with lock:
            ds = dt.date().strftime("%Y-%m-%d")
            if ds in booked_slots and dt in booked_slots[ds]:
                booked_slots[ds].remove(dt)
                if not booked_slots[ds]:
                    del booked_slots[ds]
        asyncio.create_task(application.bot.send_message(ADMIN_ID, f"Скасовано: {dt.strftime('%d.%m.%Y %H:%M')}"))
        return True
    except Exception as e:
        log.error(f"cancel_record: {e}")
        return False

# ==================== ЗАПИС ====================
def add_sheet(data):
    if not os.path.exists(CREDS_S):
        return
    try:
        build("sheets", "v4", credentials=Credentials.from_service_account_file(CREDS_S, scopes=SCOPES)).spreadsheets().values().append(
            spreadsheetId=SHEET_ID, range="A:H", valueInputOption="RAW",
            body={"values": [[datetime.now().strftime("%d.%m.%Y %H:%M"), data["pib"], data["gender"], data["year"], data["phone"], data.get("email",""), data["addr"], data["full"]]]}
        ).execute()
    except Exception as e:
        log.error(f"add_sheet: {e}")

def add_event(data):
    if not os.path.exists(CREDS_C):
        return False
    try:
        dt = datetime.combine(data["date"], data["time"]).replace(tzinfo=LOCAL)
        record_code = f"REC-{dt.strftime('%Y%m%d-%H%M')}"
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
        event = service.events().insert(calendarId=CAL_ID, body={
            "summary": f"ЕКГ: {data['pib']} ({data['phone']})",
            "location": data["addr"],
            "description": f"Email: {data.get('email','—')}\nР.н.: {data['year']}\nСтать: {data['gender']}\nChat ID: {data['cid']}",
            "start": {"dateTime": (dt - timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"},
            "end": {"dateTime": (dt + timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"}
        }).execute()
        with lock:
            ds = data["date"].strftime("%Y-%m-%d")
            booked_slots.setdefault(ds, []).append(dt)
        last_rec.setdefault(data["cid"], {})[event["id"]] = {"event_id": event["id"], "full_dt": data["full"], "record_code": record_code}
        return True
    except Exception as e:
        log.error(f"add_event: {e}")
        return False

# ==================== НАГАДУВАННЯ ====================
async def check_reminders():
    now = datetime.now(LOCAL)
    for day in [now.date(), (now + timedelta(days=1)).date()]:
        events = await get_events(day)
        for e in events:
            try:
                start_dt = datetime.fromisoformat(e["start"]["dateTime"].replace("Z", "+00:00")).astimezone(LOCAL)
                mins = int((start_dt - now).total_seconds() // 60)
                eid = e["id"]
                if mins in [30, 10] and (eid, mins) not in reminded:
                    desc = e.get("description", "")
                    match = re.search(r"Chat ID: (\d+)", desc)
                    cid = int(match.group(1)) if match else None
                    msg = f"НАГАДУВАННЯ! ЕКГ через {mins} хв\n{start_dt.strftime('%d.%m.%Y %H:%M')}\n{e['summary']}"
                    if cid:
                        await application.bot.send_message(cid, msg)
                    await application.bot.send_message(ADMIN_ID, msg)
                    reminded.add((eid, mins))
            except:
                continue

async def reminder_loop():
    while True:
        await check_reminders()
        await asyncio.sleep(60)

# ==================== ОБРОБКА ====================
async def process_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global u, show_welcome
    msg = update.message
    if not msg:
        return
    chat_id = msg.chat_id
    text = msg.text.strip() if msg.text else ""

    if chat_id not in show_welcome:
        await msg.reply_text("Цей бот для запису на ЕКГ вдома!\nОберіть дію:", reply_markup=main_kb)
        show_welcome[chat_id] = True

    if text == "Скасувати":
        u.pop(chat_id, None)
        await msg.reply_text("Дію скасовано.", reply_markup=main_kb)
        return

    if text == "Скасувати запис":
        data = last_rec.get(chat_id, {})
        if not data:
            await msg.reply_text("У вас немає активних записів.", reply_markup=main_kb)
            return
        reply = "Ваші записи:\n\n"
        for i, rec in enumerate(data.values(), 1):
            reply += f"{i}. <b>ID запису:</b> <code>{rec['record_code']}</code>\n"
            reply += f"   Дата і час: <b>{rec['full_dt']}</b>\n\n"
        reply += "Надішліть тільки <b>ID запису</b> (наприклад, <code>REC-20251117-1300</code>), щоб скасувати:"
        await msg.reply_text(reply, reply_markup=cancel_kb, parse_mode="HTML")
        return

    if text == "Список записів":
        data = last_rec.get(chat_id, {})
        if not data:
            await msg.reply_text("У вас немає активних записів.", reply_markup=main_kb)
            return
        reply = "Ваші записи:\n\n"
        for i, rec in enumerate(data.values(), 1):
            reply += f"{i}. <b>ID запису:</b> <code>{rec['record_code']}</code>\n"
            reply += f"   Дата і час: <b>{rec['full_dt']}</b>\n\n"
        await msg.reply_text(reply, reply_markup=main_kb, parse_mode="HTML")
        return

    if text and chat_id in last_rec and any(text == r["record_code"] for r in last_rec[chat_id].values()):
        if cancel_record(chat_id, text):
            await msg.reply_text(f"Запис <code>{text}</code> скасовано!", reply_markup=main_kb, parse_mode="HTML")
        else:
            await msg.reply_text("Помилка скасування.", reply_markup=main_kb)
        return

    if text in ["/start", "Записатися на ЕКГ"]:
        u[chat_id] = {"step": "pib", "cid": chat_id}
        await msg.reply_text("Введіть ПІБ (Прізвище Ім'я По батькові):", reply_markup=cancel_kb)
        return

    if chat_id not in u:
        return

    data = u[chat_id]
    step = data.get("step")

    if step == "pib":
        parts = text.strip().split()
        if len(parts) == 3 and all(re.match(r"^[А-ЯЁІЇЄҐ][а-яёіїєґ]+$", p) for p in parts):
            data["pib"] = " ".join(parts)
            data["step"] = "gender"
            await msg.reply_text("Стать:", reply_markup=gender_kb)
        else:
            await msg.reply_text("Невірне ПІБ. Введіть три слова з великої літери.", reply_markup=cancel_kb)
        return

    if step == "gender":
        if text in ["Чоловіча", "Жіноча"]:
            data["gender"] = text
            data["step"] = "year"
            await msg.reply_text("Рік народження:", reply_markup=cancel_kb)
        else:
            await msg.reply_text("Оберіть зі списку.", reply_markup=gender_kb)
        return

    if step == "year":
        if text.isdigit() and 1900 <= int(text) <= datetime.now().year:
            data["year"] = int(text)
            data["step"] = "phone"
            await msg.reply_text("Телефон:", reply_markup=cancel_kb)
        else:
            await msg.reply_text("Невірний рік.", reply_markup=cancel_kb)
        return

    if step == "phone":
        cleaned = text.replace(" ", "")
        if re.match(r"^(\+380|0)\d{9}$", cleaned):
            data["phone"] = text.strip()
            data["step"] = "email"
            await msg.reply_text("Email (можна пропустити):", reply_markup=email_kb)
        else:
            await msg.reply_text("Невірний номер телефону.", reply_markup=cancel_kb)
        return

    if step == "email":
        if text in ["Пропустити", ""]:
            data["email"] = ""
        elif re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", text):
            data["email"] = text.strip()
        else:
            await msg.reply_text("Невірний email або натисніть Пропустити.", reply_markup=email_kb)
            return
        data["step"] = "addr"
        await msg.reply_text("Адреса (де робити ЕКГ):", reply_markup=cancel_kb)
        return

    if step == "addr":
        data["addr"] = text.strip()
        data["step"] = "date"
        await msg.reply_text("Оберіть дату:", reply_markup=date_kb())
        return

    if step == "date":
        if text == "Інша дата (ДД.ММ.ЯЯЯЯ)":
            await msg.reply_text("Введіть дату ДД.ММ.ЯЯЯЯ:", reply_markup=cancel_kb)
            return
        d = v_date(text)
        if d:
            data["date"] = d
            data["step"] = "time"
            slots = await free_slots_async(d)
            if not slots:
                await msg.reply_text(f"На {d.strftime('%d.%m.%Y')} немає вільного часу.", reply_markup=date_kb())
                data["step"] = "date"
            else:
                await msg.reply_text(f"Вільно {d.strftime('%d.%m.%Y')}:\n" + "\n".join(slots) + "\n\nВведіть час (наприклад 14:00):", reply_markup=cancel_kb)
        else:
            await msg.reply_text("Невірна дата.", reply_markup=cancel_kb)
        return

    if step == "time":
        try:
            t_str = text.strip()
            t = datetime.strptime(t_str, "%H:%M").time()
            if not ("09:00" <= t_str <= "18:00"):
                raise ValueError
            if await free_60(data["date"], t):
                full = f"{data['date'].strftime('%d.%m.%Y')} {t_str}"
                conf = f"Запис підтверджено!\n\nПІБ: {data['pib']}\nСтать: {data['gender']}\nР.н.: {data['year']}\nТелефон: {data['phone']}\nEmail: {data.get('email','—')}\nАдреса: {data['addr']}\nДата і час: {full}"
                await msg.reply_text(conf, reply_markup=main_kb)
                await application.bot.send_message(ADMIN_ID, f"НОВИЙ ЗАПИС:\n{conf}")
                add_event({**data, "time": t, "full": full, "cid": chat_id})
                add_sheet({**data, "full": full})
                u.pop(chat_id, None)
            else:
                await msg.reply_text("Цей час зайнятий. Оберіть інший.", reply_markup=cancel_kb)
        except:
            await msg.reply_text("Формат: ЧЧ:ХХ (наприклад 14:00)", reply_markup=cancel_kb)
        return

# ==================== FASTAPI + RENDER ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Запуск бота")
    await application.initialize()
    await application.start()
    url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
    await application.bot.set_webhook(url=url)
    log.info(f"Webhook встановлено: {url}")
    asyncio.create_task(reminder_loop())
    yield
    await application.stop()
    await application.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    json_data = await request.json()
    update = Update.de_json(json_data, application.bot)
    asyncio.create_task(process_update(update, None))
    return JSONResponse({"ok": True})

@app.get("/")
async def root():
    return {"message": "EKG Bot працює"}

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    log.info(f"Запускаємо сервер на порту {port}")
    uvicorn.run("bot:app", host="0.0.0.0", port=port, log_level="info")
