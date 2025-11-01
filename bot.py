# bot.py — WEBHOOK + FastAPI + Render (v21.5 + lifespan + welcome message)
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
u, cache, reminded, last_rec, booked_slots, seen_users = {}, {}, set(), {}, {}, {}  # Додано seen_users для відстеження нових користувачів
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

# === ВАЛИДАЦІЯ ===
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split())==3 and all(re.match(r"^[А-ЯЁІ ЇЄҐ][а-яёіїєґ]+$",i) for i in p) else None
v_gender = lambda x: x if x in ["Чоловіча","Жіноча"] else None
v_year = lambda x: int(x) if x.isdigit() and 1900 <= int(x) <= datetime.now().year else None
v_phone = lambda x: x.strip() if re.match(r"^(\+380|0)\d{9}$", x.replace(" ","")) else None
v_email = lambda x: x.strip() if x and re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", x) else ""
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
async def process_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global u, seen_users
    msg = update.message
    if not msg:
        log.warning(f"Отримано оновлення без повідомлення: {update}")
        return
    chat_id = msg.chat_id
    text = msg.text.strip() if msg.text else ""
    log.info(f"Отримано повідомлення від {chat_id}: '{text}'")

    # Вітальне повідомлення для нового користувача
    if chat_id not in seen_users:
        welcome_message = (
            "Ласкаво просимо! 🎉\n"
            "Це бот для запису на електрокардіограму (ЕКГ) вдома.\n"
            "Щоб почати, натисніть /start або 'Записатися на ЕКГ'.\n"
            "Для скасування запису використовуйте 'Скасувати запис'."
        )
        await msg.reply_text(welcome_message, reply_markup=main_kb)
        seen_users[chat_id] = True

    if text == "Скасувати":
        u.pop(chat_id, None)
        await msg.reply_text("Скасовано.", reply_markup=main_kb)
        log.info(f"Користувач {chat_id} скасував")
        return

    if text == "Скасувати запис":
        if cancel_record(chat_id):
            await msg.reply_text("Запис скасовано!", reply_markup=main_kb)
        else:
            await msg.reply_text("Запис не знайдено", reply_markup=main_kb)
        log.info(f"Скасування запису для {chat_id}")
        return

    if text in ["/start", "Записатися на ЕКГ"]:
        u[chat_id] = {"step": "pib", "cid": chat_id}
        await msg.reply_text("ПІБ (Прізвище Ім'я По батькові):", reply_markup=cancel_kb)
        log.info(f"Користувач {chat_id} почав запис")
        return

    if chat_id not in u:
        log.warning(f"Невідомий чат {chat_id} відправив: '{text}'")
        return
    data = u[chat_id]
    step = data["step"]
    log.info(f"Крок для {chat_id}: {step}, введено: '{text}'")

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
        log.debug(f"Валідація {step}: '{text}' → {val}")
        if val is not None:
            data[step] = val
            data["step"] = steps[step][1]
            await msg.reply_text(steps[step][2], reply_markup=steps[step][3])
            log.info(f"Крок {chat_id} змінено на {steps[step][1]}")
        else:
            if step == "email" and (text == "" or text == "Скасувати"):
                data[step] = ""
                data["step"] = "addr"
                await msg.reply_text("Адреса:", reply_markup=cancel_kb)
                log.info(f"Пропущено email для {chat_id}")
            else:
                await msg.reply_text("Невірно", reply_markup=cancel_kb)
                log.warning(f"Невірний ввід для {chat_id} на кроці {step}")
        return

    if step == "date":
        date_val = v_date(text)
        log.debug(f"Валідація дати: '{text}' → {date_val}")
        if date_val:
            data["date"] = date_val
            data["step"] = "time"
            try:
                slots = await free_slots_async(date_val)
                if not slots:
                    await msg.reply_text(f"Вільно {date_val.strftime('%d.%m')} (60 хв): • Немає\nСпробуйте іншу дату.", reply_markup=date_kb())
                    log.warning(f"Немає вільних слотів для {date_val.strftime('%d.%m')}")
                else:
                    await msg.reply_text(f"Вільно {date_val.strftime('%d.%m')} (60 хв):\n" + "\n".join(f"• {s}" for s in slots) + "\n\nВведіть час (09:00–18:00):", reply_markup=cancel_kb)
                    log.info(f"Крок {chat_id} змінено на time, слоти: {slots}")
            except Exception as e:
                await msg.reply_text("Помилка при отриманні слотів. Спробуйте пізніше.", reply_markup=date_kb())
                log.error(f"Помилка отримання слотів для {date_val}: {e}")
        else:
            await msg.reply_text("Невірна дата", reply_markup=date_kb())
            log.warning(f"Невірна дата від {chat_id}")

    if step == "time":
        try:
            time_val = datetime.strptime(text.strip(), "%H:%M").time()
            log.debug(f"Валідація часу: '{text}' → {time_val}")
            if not (datetime.strptime("09:00","%H:%M").time() <= time_val <= datetime.strptime("18:00","%H:%M").time()):
                raise ValueError
            dt = datetime.combine(data["date"], time_val).replace(tzinfo=LOCAL)
            if await asyncio.to_thread(free_60, data["date"], time_val):
                full = f"{data['date'].strftime('%d.%m')} {text}"
                conf = f"Запис:\nПІБ: {data['pib']}\nСтать: {data['gender']}\nР.н.: {data['year']}\nТел: {data['phone']}\nEmail: {data.get('email','—')}\nАдреса: {data['addr']}\nЧас: {full} (±30 хв)"
                await msg.reply_text(f"{conf}\n\nДякую за запис!", reply_markup=main_kb)
                await application.bot.send_message(ADMIN_ID, f"НОВИЙ ЗАПИС!\n{conf}")
                if add_event({**data, "time": time_val, "cid": chat_id, "full": full}):
                    add_sheet({**data, "full": full})
                    u.pop(chat_id, None)
                    log.info(f"Запис завершено для {chat_id}")
            else:
                await msg.reply_text("Зайнято (±30 хв)", reply_markup=cancel_kb)
                log.warning(f"Час зайнято для {chat_id}")
        except Exception as e:
            await msg.reply_text("Формат: ЧЧ:ХХ", reply_markup=cancel_kb)
            log.error(f"Помилка обробки часу для {chat_id}: {e}")

# === LIFESPAN ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Бот запущено!")
    init_sheet()
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

# === WEBHOOK ===
@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    json_data = await request.json()
    update = Update.de_json(json_data, application.bot)
    log.info(f"Отримано webhook: {update}")
    asyncio.create_task(process_update(update, None))
    return JSONResponse({"ok": True})

@app.get("/")
async def root():
    return {"message": "EKG Bot is running!"}

# === НАГАДУВАННЯ ===
async def reminder_loop():
    while True:
        await check_reminders()
        await asyncio.sleep(60)

# === HEALTH CHECK ===
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
