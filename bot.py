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
import unicodedata

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
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split())==3 and all(re.match(r"^[А-ЯЁІЇЄҐ][а-яёіїєґ]+$",i) for i in p) else None
v_gender = lambda x: "Чоловіча" if unicodedata.normalize("NFKD", x).encode("ASCII", "ignore").decode("ASCII").strip() == "Чоловіча" else \
                    "Жіноча" if unicodedata.normalize("NFKD", x).encode("ASCII", "ignore").decode("ASCII").strip() == "Жіноча" else None
v_year = lambda x: int(x) if x.isdigit() and 1900 <= int(x) <= datetime.now().year else None
v_phone = lambda x: x.strip() if re.match(r"^(\+380|0)\d{9}$", x.replace(" ","")) else None
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
            event_id = event_to_delete["event_id"]
            dt = datetime.strptime(event_to_delete["full_dt"], "%d.%m.%Y %H:%M").replace(tzinfo=LOCAL)
            last_rec[chat_id] = {k: v for k, v in last_rec[chat_id].items() if v.get("record_code") != record_code}
        else:
            event_id = list(last_rec[chat_id].values())[0]["event_id"]
            dt = datetime.strptime(list(last_rec[chat_id].values())[0]["full_dt"], "%d.%m.%Y %H:%M").replace(tzinfo=LOCAL)
            last_rec.pop(chat_id, None)
        if not os.path.exists(CREDS_C):
            return False
        try:
            service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
            service.events().delete(calendarId=CAL_ID, eventId=event_id).execute()
            with lock:
                ds = dt.date().strftime("%Y-%m-%d")
                if ds in booked_slots:
                    booked_slots[ds].remove(dt)
                    if not booked_slots[ds]:
                        del booked_slots[ds]
            asyncio.create_task(application.bot.send_message(ADMIN_ID, f"Скасовано: {dt.strftime('%d.%m.%Y %H:%M')}"))
            return True
        except Exception as e:
            log.error(f"cancel_record: {e}")
    return False

# РЕДАГУВАННЯ
def update_event(chat_id, record_code, new_data):
    if chat_id not in last_rec or not any(r["record_code"] == record_code for r in last_rec[chat_id].values()):
        return False
    event_to_update = next(r for r in last_rec[chat_id].values() if r["record_code"] == record_code)
    event_id = event_to_update["event_id"]
    old_dt = datetime.strptime(event_to_update["full_dt"], "%d.%m.%Y %H:%M").replace(tzinfo=LOCAL)
    new_dt = datetime.combine(new_data["date"], new_data["time"]).replace(tzinfo=LOCAL)

    if not os.path.exists(CREDS_C):
        return False
    try:
        service = build("calendar", "v3", credentials=Credentials.from_service_account_file(CREDS_C, scopes=SCOPES))
        event = service.events().get(calendarId=CAL_ID, eventId=event_id).execute()
        event["summary"] = f"ЕКГ: {new_data['pib']} ({new_data['phone']})"
        event["location"] = new_data["addr"]
        event["description"] = f"Email: {new_data.get('email', '—')}\nР.н.: {new_data['year']}\nСтать: {new_data['gender']}\nChat ID: {chat_id}"
        event["start"]["dateTime"] = (new_dt - timedelta(minutes=30)).isoformat()
        event["end"]["dateTime"] = (new_dt + timedelta(minutes=30)).isoformat()
        service.events().update(calendarId=CAL_ID, eventId=event_id, body=event).execute()

        with lock:
            ds_old = old_dt.date().strftime("%Y-%m-%d")
            if ds_old in booked_slots:
                booked_slots[ds_old].remove(old_dt)
                if not booked_slots[ds_old]:
                    del booked_slots[ds_old]
            ds_new = new_dt.date().strftime("%Y-%m-%d")
            if ds_new not in booked_slots:
                booked_slots[ds_new] = []
            booked_slots[ds_new].append(new_dt)

        full = f"{new_data['date'].strftime('%d.%m.%Y')} {new_data['time'].strftime('%H:%M')}"
        event_to_update["full_dt"] = full
        return True
    except Exception as e:
        log.error(f"update_event: {e}")
        return False

def update_sheet(data, record_code):
    if not os.path.exists(CREDS_S):
        return
    try:
        service = build("sheets", "v4", credentials=Credentials.from_service_account_file(CREDS_S, scopes=SCOPES))
        sheet = service.spreadsheets()
        values = sheet.values().get(spreadsheetId=SHEET_ID, range="A2:H").execute().get("values", [])
        for i, row in enumerate(values, 2):
            if len(row) > 7 and row[7] and re.search(rf"REC-{record_code.split('-')[1]}", row[7]):
                sheet.values().update(
                    spreadsheetId=SHEET_ID, range=f"A{i}:H{i}", valueInputOption="RAW",
                    body={"values": [[datetime.now().strftime("%d.%m.%Y %H:%M"), data["pib"], data["gender"], data["year"], data["phone"], data.get("email", ""), data["addr"], data["full"]]]}
                ).execute()
                break
    except Exception as e:
        log.error(f"update_sheet: {e}")

# ЗАПИС
def init_sheet():
    if not os.path.exists(CREDS_S):
        return
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
    if not os.path.exists(CREDS_S):
        return
    try:
        build("sheets", "v4", credentials=Credentials.from_service_account_file(CREDS_S, scopes=SCOPES)).spreadsheets().values().append(
            spreadsheetId=SHEET_ID, range="A:H", valueInputOption="RAW",
            body={"values": [[datetime.now().strftime("%d.%m.%Y %H:%M"), data["pib"], data["gender"], data["year"], data["phone"], data.get("email", ""), data["addr"], data["full"]]]}
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
            "description": f"Email: {data.get('email', '—')}\nР.н.: {data['year']}\nСтать: {data['gender']}\nChat ID: {data['cid']}",
            "start": {"dateTime": (dt - timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"},
            "end": {"dateTime": (dt + timedelta(minutes=30)).isoformat(), "timeZone": "Europe/Kiev"}
        }).execute()
        with lock:
            ds = data["date"].strftime("%Y-%m-%d")
            if ds not in booked_slots:
                booked_slots[ds] = []
            booked_slots[ds].append(dt)
        if data["cid"] not in last_rec:
            last_rec[data["cid"]] = {}
        last_rec[data["cid"]][event["id"]] = {"event_id": event["id"], "full_dt": data["full"], "record_code": record_code}
        return True
    except Exception as e:
        log.error(f"add_event: {e}")
        asyncio.create_task(application.bot.send_message(ADMIN_ID, f"ПОМИЛКА КАЛЕНДАРЯ: {e}"))
        return False

# НАГАДУВАННЯ
async def check_reminders():
    now = datetime.now(LOCAL)
    for day in [now.date(), (now + timedelta(days=1)).date()]:
        events = await get_events(day)
        for e in events:
            try:
                start_str = e["start"]["dateTime"]
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(LOCAL)
                mins_left = int((start_dt - now).total_seconds() // 60)
                eid = e["id"]
                if mins_left in [30, 10] and (eid, mins_left) not in reminded:
                    desc = e.get("description", "")
                    cid_match = re.search(r"Chat ID: (\d+)", desc)
                    cid = int(cid_match.group(1)) if cid_match else None
                    msg = f"🔔 НАГАДУВАННЯ! ЕКГ через {mins_left} хв\n📅 Дата: {start_dt.strftime('%d.%m.%Y')}\n⏰ Час: {start_dt.strftime('%H:%M')}\n{e['summary']}"
                    if cid:
                        await application.bot.send_message(cid, msg)
                    await application.bot.send_message(ADMIN_ID, f"🔔 НАГАДУВАННЯ:\n{msg}")
                    reminded.add((eid, mins_left))
            except Exception:
                continue

# ОБРОБКА
async def process_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global u, show_welcome
    msg = update.message
    if not msg:
        log.warning("process_update: Оновлення без повідомлення")
        return
    chat_id = msg.chat_id
    text = msg.text.strip() if msg.text else ""
    log.info(f"process_update: {chat_id}: '{text}'")
    if chat_id not in show_welcome:
        await msg.reply_text(
            "Цей бот для запису на ЕКГ вдома! 🏠\n"
            "Оберіть: 'Записатися на ЕКГ 🎉', 'Скасувати запис ❌' або 'Список записів 📋'.",
            reply_markup=main_kb
        )
        show_welcome[chat_id] = True
        log.info(f"process_update: Вітання для {chat_id}")
    if text == "Скасувати ❌":
        u.pop(chat_id, None)
        await msg.reply_text("Скасовано. ✅", reply_markup=main_kb)
        return
    if text == "Скасувати запис ❌":
        data = last_rec.get(chat_id, {})
        if not data:
            await msg.reply_text("У вас немає активних записів. 📭", reply_markup=main_kb)
            return
        reply_text = "Ваші записи:\n"
        for i, (event_id, record) in enumerate(data.items(), 1):
            reply_text += f"{i}. ID запису: {record['record_code']} - {record['full_dt']}\n"
        reply_text += "Введи ID запису для скасування (наприклад, REC-20251116-1532):"
        await msg.reply_text(reply_text, reply_markup=cancel_kb)
        return
    if text and chat_id in last_rec and any(text == r["record_code"] for r in last_rec[chat_id].values()):
        if cancel_record(chat_id, text):
            await msg.reply_text(f"Запис з ID {text} скасовано! ✅", reply_markup=main_kb)
            show_welcome[chat_id] = False
        else:
            await msg.reply_text("Помилка скасування. 😞", reply_markup=main_kb)
        return
    if text == "Редагувати запис ✏️":
        data = last_rec.get(chat_id, {})
        if not data:
            await msg.reply_text("У вас немає активних записів для редагування. 📭", reply_markup=main_kb)
            return
        reply_text = "Ваші записи:\n"
        for i, (event_id, record) in enumerate(data.items(), 1):
            reply_text += f"{i}. ID запису: {record['record_code']} - {record['full_dt']}\n"
        reply_text += "Введи ID запису для редагування (наприклад, REC-20251116-1532):"
        await msg.reply_text(reply_text, reply_markup=cancel_kb)
        u[chat_id] = {"step": "edit_record", "cid": chat_id}
        return
    if text and chat_id in u and u[chat_id].get("step") == "edit_record" and any(text == r["record_code"] for r in last_rec[chat_id].values()):
        record = next(r for r in last_rec[chat_id].values() if r["record_code"] == text)
        dt = datetime.strptime(record["full_dt"], "%d.%m.%Y %H:%M")
        u[chat_id] = {
            "step": "edit_pib", "cid": chat_id,
            "record_code": text,
            "pib": record.get("pib", ""),
            "gender": record.get("gender", ""),
            "year": record.get("year", ""),
            "phone": record.get("phone", ""),
            "email": record.get("email", ""),
            "addr": record.get("addr", ""),
            "date": dt.date(),
            "time": dt.time()
        }
        await msg.reply_text(f"Редагування запису з ID {text}.\nПІБ (Прізвище Ім'я По батькові): 👤\nПоточне значення: {record.get('pib', '—')}", reply_markup=cancel_kb)
        return
    if text == "/list" or text == "Список записів 📋":
        data = last_rec.get(chat_id, {})
        if not data:
            await msg.reply_text("У вас немає активних записів. 📭", reply_markup=main_kb)
            return
        reply_text = "Ваші записи:\n"
        for i, (event_id, record) in enumerate(data.items(), 1):
            reply_text += f"{i}. ID запису: {record['record_code']} - {record['full_dt']}\n"
        await msg.reply_text(reply_text, reply_markup=main_kb)
        return
    if text in ["/start", "Записатися на ЕКГ 🎉"]:
        u[chat_id] = {"step": "pib", "cid": chat_id}
        await msg.reply_text("ПІБ (Прізвище Ім'я По батькові): 👤", reply_markup=cancel_kb)
        show_welcome[chat_id] = False
        return
    if text == "Повторити запис 🔄":
        if chat_id not in last_rec or not last_rec[chat_id]:
            await msg.reply_text("Щоб повторити запис, спочатку зроби хоча б один запис! 📝", reply_markup=main_kb)
            return
        last_record = list(last_rec[chat_id].values())[0]
        last_dt = datetime.strptime(last_record["full_dt"], "%d.%m.%Y %H:%M")
        last_date = last_dt.date()
        last_time = last_dt.time()
        if await free_60(last_date, last_time):
            u[chat_id] = {"step": "pib", "cid": chat_id, "date": last_date, "time": last_time}
            await msg.reply_text(
                f"Повторний запис на 📅 {last_date.strftime('%d.%m.%Y')} ⏰ {last_time.strftime('%H:%M')}.\n"
                "ПІБ (Прізвище Ім'я По батькові): 👤",
                reply_markup=cancel_kb
            )
            show_welcome[chat_id] = False
        else:
            await msg.reply_text("Цей час уже зайнятий (±60 хв). Обери інший слот. 📅", reply_markup=main_kb)
        return
    if chat_id not in u:
        log.warning(f"process_update: Невідомий чат {chat_id}")
        return
    data = u[chat_id]
    step = data["step"]
    steps = {
        "pib": (v_pib, "gender", "Стать: 🧑👩", gender_kb),
        "gender": (v_gender, "year", "Рік народження: 📅", cancel_kb),
        "year": (v_year, "phone", "Телефон: 📞", cancel_kb),
        "phone": (v_phone, "email", "Email (необов'язково, введи символ або натисни 'Пропустити ⏭️'): ✉️", email_kb),
        "email": (v_email, "addr", "Адреса: 🏠", cancel_kb),
        "addr": (lambda x: x.strip(), "date", "Дата: 📅", date_kb()),
        "edit_pib": (v_pib, "edit_gender", "Стать: 🧑👩", gender_kb),
        "edit_gender": (v_gender, "edit_year", "Рік народження: 📅", cancel_kb),
        "edit_year": (v_year, "edit_phone", "Телефон: 📞", cancel_kb),
        "edit_phone": (v_phone, "edit_email", "Email (необов'язково, введи символ або натисни 'Пропустити ⏭️'): ✉️", email_kb),
        "edit_email": (v_email, "edit_addr", "Адреса: 🏠", cancel_kb),
        "edit_addr": (lambda x: x.strip(), "edit_date", "Дата: 📅", date_kb())
    }
    if step in steps:
        val = steps[step][0](text)
        if val is not None:
            data[step.replace("edit_", "")] = val
            data["step"] = steps[step][1]
            if step.startswith("edit_"):
                current = data.get(step.replace("edit_", ""), "—")
                await msg.reply_text(f"{steps[step][2]}\nПоточне значення: {current}", reply_markup=steps[step][3])
            else:
                await msg.reply_text(steps[step][2], reply_markup=steps[step][3])
            log.info(f"process_update: Крок {chat_id}: {steps[step][1]}")
        else:
            if step in ["edit_email", "email"] and (text == "" or text == "Пропустити ⏭️"):
                data[step.replace("edit_", "")] = ""
                data["step"] = steps[step][1]
                await msg.reply_text(f"Адреса: 🏠\nПоточне значення: {data.get('addr', '—')}", reply_markup=cancel_kb)
            else:
                await msg.reply_text("Невірно. 😞", reply_markup=cancel_kb)
        return
    if step == "edit_date":
        if text == "Інша дата (ДД.ММ.ЯЯЯЯ) 📅":
            await msg.reply_text("Введи дату у форматі ДД.ММ.ЯЯЯЯ (наприклад, 12.11.2025): 📅", reply_markup=cancel_kb)
            return
        date_val = v_date(text)
        if date_val:
            data["date"] = date_val
            data["step"] = "edit_time"
            slots = await free_slots_async(date_val)
            if not slots:
                await msg.reply_text(f"На {date_val.strftime('%d.%m.%Y')} вільних 60-хвилинних слотів немає. 📭", reply_markup=date_kb())
                data["step"] = "edit_date"
            else:
                await msg.reply_text(f"Вільно {date_val.strftime('%d.%m.%Y')} (60 хв): 📅\n" + "\n".join(f"• {s}" for s in slots) + "\n\nВибери час: ⏰\nПоточне значення: {data['time'].strftime('%H:%M')}", reply_markup=cancel_kb)
            log.info(f"process_update: Дата {chat_id}: {date_val.strftime('%d.%m.%Y')}, слоти {slots}")
        else:
            await msg.reply_text("Невірний формат. Введи ДД.ММ.ЯЯЯЯ (наприклад, 12.11.2025). 📅", reply_markup=cancel_kb)
        return
    if step == "edit_time":
        try:
            time_val = datetime.strptime(text.strip(), "%H:%M").time()
            if not ("09:00" <= text <= "18:00"):
                raise ValueError
            if await free_60(data["date"], time_val):
                full = f"{data['date'].strftime('%d.%m.%Y')} {time_val.strftime('%H:%M')}"
                conf = f"Запис оновлено! ✅\nПІБ: {data['pib']}\nСтать: {data['gender']}\nР.н.: {data['year']}\nТел: {data['phone']}\nEmail: {data.get('email','—')}\nАдреса: {data['addr']}\n📅 Дата і час: {full} (±30 хв)"
                await msg.reply_text(conf, reply_markup=main_kb)
                await application.bot.send_message(ADMIN_ID, f"🔔 ОНОВЛЕНО ЗАПИС:\n{conf}")
                update_event(chat_id, data["record_code"], data)
                update_sheet(data, data["record_code"])
                u.pop(chat_id, None)
                show_welcome[chat_id] = True
                log.info(f"process_update: Редагування {chat_id}: {full}")
            else:
                await msg.reply_text("Цей час зайнятий (±60 хв). Обери інший. 📅", reply_markup=cancel_kb)
        except ValueError:
            await msg.reply_text("Формат: ЧЧ:ХХ (09:00–18:00). ⏰", reply_markup=cancel_kb)
        return
    if step == "date":
        if text == "Інша дата (ДД.ММ.ЯЯЯЯ) 📅":
            await msg.reply_text("Введи дату у форматі ДД.ММ.ЯЯЯЯ (наприклад, 12.11.2025): 📅", reply_markup=cancel_kb)
            return
        date_val = v_date(text)
        if date_val:
            data["date"] = date_val
            data["step"] = "time"
            slots = await free_slots_async(date_val)
            if not slots:
                await msg.reply_text(f"На {date_val.strftime('%d.%m.%Y')} вільних 60-хвилинних слотів немає. 📭", reply_markup=date_kb())
                data["step"] = "date"
            else:
                await msg.reply_text(f"Вільно {date_val.strftime('%d.%m.%Y')} (60 хв): 📅\n" + "\n".join(f"• {s}" for s in slots) + "\n\nВибери час: ⏰", reply_markup=cancel_kb)
            log.info(f"process_update: Дата {chat_id}: {date_val.strftime('%d.%m.%Y')}, слоти {slots}")
        else:
            await msg.reply_text("Невірний формат. Введи ДД.ММ.ЯЯЯЯ (наприклад, 12.11.2025). 📅", reply_markup=cancel_kb)
        return
    if step == "time":
        try:
            time_val = datetime.strptime(text.strip(), "%H:%M").time()
            if not ("09:00" <= text <= "18:00"):
                raise ValueError
            if await free_60(data["date"], time_val):
                full = f"{data['date'].strftime('%d.%m.%Y')} {text}"
                conf = f"Запис підтверджено! ✅\nПІБ: {data['pib']}\nСтать: {data['gender']}\nР.н.: {data['year']}\nТел: {data['phone']}\nEmail: {data.get('email','—')}\nАдреса: {data['addr']}\n📅 Дата і час: {full} (±30 хв)"
                await msg.reply_text(conf, reply_markup=main_kb)
                await application.bot.send_message(ADMIN_ID, f"🔔 НОВИЙ ЗАПИС:\n{conf}")
                add_event({**data, "time": time_val, "cid": chat_id, "full": full})
                add_sheet({**data, "full": full})
                u.pop(chat_id, None)
                show_welcome[chat_id] = True
                log.info(f"process_update: Запис {chat_id}: {full}")
            else:
                await msg.reply_text("Цей час зайнятий (±60 хв). Обери інший. 📅", reply_markup=cancel_kb)
        except ValueError:
            await msg.reply_text("Формат: ЧЧ:ХХ (09:00–18:00). ⏰", reply_markup=cancel_kb)
        return

# LIFESPAN
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("lifespan: Запуск бота 🚀")
    await application.initialize()
    await application.start()
    url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}{WEBHOOK_PATH}"
    await application.bot.set_webhook(url=url)
    log.info(f"lifespan: Webhook: {url}")
    asyncio.create_task(reminder_loop())
    yield
    await application.stop()
    await application.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    log.info("webhook: Запит отримано 📥")
    json_data = await request.json()
    update = Update.de_json(json_data, application.bot)
    log.info(f"webhook: Оновлення: {update}")
    asyncio.create_task(process_update(update, None))
    return JSONResponse({"ok": True})

@app.get("/")
async def root():
    log.info("root: Сервер живий ✅")
    return {"message": "EKG Bot is running!"}

async def reminder_loop():
    while True:
        await check_reminders()
        await asyncio.sleep(60)

@app.get("/health")
async def health_check():
    log.info("health: Перевірка ✅")
    return {"status": "healthy"}

if __name__ == "__main__":
    log.info("main: Сервер стартує 🚀")
    uvicorn.run(app, host="0.0.0.0", port=10000)
