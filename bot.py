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
v_pib = lambda x: " ".join(x.strip().split()) if len(p:=x.strip().split())==3 and all(re.match(r"^[А-ЯЁІЇЄҐ][а-яёіїєґ]+$",i) for i in p) else None
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
    except ValueError
