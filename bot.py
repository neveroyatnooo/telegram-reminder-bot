#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import logging
import datetime
import asyncio
from datetime import timedelta, timezone
from pathlib import Path

import psycopg2.errors
from timezonefinder import TimezoneFinder
from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_STOPPED

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters
)

# ——— Карты дней недели —————————————————————————————————
RU_TO_CRON_DAY = {
    "понедельник": "mon",
    "вторник":     "tue",
    "среда":       "wed",
    "четверг":     "thu",
    "пятница":     "fri",
    "суббота":     "sat",
    "воскресенье": "sun"
}
NUM_TO_RU_DAY = {
    "1": "понедельник", "2": "вторник", "3": "среда", "4": "четверг",
    "5": "пятница", "6": "суббота", "7": "воскресенье", "0": "воскресенье"
}

HAS_THREAD_COL = False
ADD_INPUT, DELETE_INPUT = range(2)

def get_thread_id(update: Update) -> int | None:
    return getattr(update.effective_message, "message_thread_id", None)

def with_thread(kwargs: dict, update: Update) -> dict:
    tid = get_thread_id(update)
    if tid is not None:
        kwargs["message_thread_id"] = tid
    return kwargs

def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("Добавить"), KeyboardButton("Список")],
            [KeyboardButton("Удалить"),  KeyboardButton("Помощь")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

INLINE_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("Добавить", callback_data="add")],
    [InlineKeyboardButton("Список",   callback_data="list")],
    [InlineKeyboardButton("Удалить",  callback_data="delete")],
    [InlineKeyboardButton("Помощь",   callback_data="help")],
])

# ——— Загрузка .env ————————————————————————————————————
env = Path(__file__).parent / ".env"
load_dotenv(env)

BOT_TOKEN   = os.getenv("BOT_TOKEN")
DB_HOST     = os.getenv("DB_HOST",    "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT",    "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_IDS   = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.isdigit()}

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

# ——— Пул соединений PostgreSQL —————————————————————————
db_pool = ThreadedConnectionPool(
    1, 10,
    host=DB_HOST, port=DB_PORT,
    dbname=DB_NAME, user=DB_USER,
    password=DB_PASSWORD
)
def get_conn(): return db_pool.getconn()
def put_conn(conn): db_pool.putconn(conn)

# ——— Планировщик APScheduler ——————————————————————————
scheduler = AsyncIOScheduler()

# время до удаления (минуты)
DELETE_DELAY_MINUTES = 5

async def delete_msg(chat_id: int, message_id: int):
    try:
        await application.bot.delete_message(chat_id=chat_id,
                                             message_id=message_id)
    except Exception as e:
        logger.warning("Не удалось удалить сообщение %s:%s — %s",
                       chat_id, message_id, e)

def schedule_deletion(chat_id: int, message_id: int,
                      delay_minutes: int = DELETE_DELAY_MINUTES):
    run_date = datetime.datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
    scheduler.add_job(delete_msg, trigger="date", run_date=run_date,
                      args=[chat_id, message_id])

tf = TimezoneFinder()

# ——— Инициализация и миграция схемы ——————————————————————
def init_db():
    global HAS_THREAD_COL
    conn = get_conn()
    try:
        cur = conn.cursor()
        # 1) Создать таблицы
        cur.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
          user_id BIGINT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS reminders (
          id                  SERIAL PRIMARY KEY,
          user_id             BIGINT NOT NULL REFERENCES allowed_users(user_id) ON DELETE CASCADE,
          chat_id             BIGINT NOT NULL,
          message_thread_id   BIGINT,
          day_of_week         TEXT    NOT NULL,
          time                TIME    NOT NULL,
          text                TEXT    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS user_timezones (
          user_id  BIGINT PRIMARY KEY,
          timezone VARCHAR(50) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS auto_delete_users (
          user_id BIGINT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS config (
          key   TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        """)
        conn.commit()

        # 1.1) Инициализировать allowed_users админами
        if ADMIN_IDS:
            cur.executemany(
                "INSERT INTO allowed_users(user_id) VALUES(%s) ON CONFLICT DO NOTHING",
                [(aid,) for aid in ADMIN_IDS]
            )
            conn.commit()

        # 1.2) Инициализировать config для автоудаления
        cur.execute(
            "INSERT INTO config(key,value) VALUES('auto_delete_enabled','false') "
            "ON CONFLICT(key) DO NOTHING"
        )
        conn.commit()

        # 2) Добавить message_thread_id если нет
        try:
            cur.execute(
                "ALTER TABLE reminders ADD COLUMN IF NOT EXISTS message_thread_id BIGINT"
            )
            conn.commit()
        except psycopg2.errors.InsufficientPrivilege:
            conn.rollback()
            logger.warning("Нет прав на добавление message_thread_id — пропускаем.")

        # 3) Миграция day_of_week -> TEXT
        try:
            cur.execute(
                "ALTER TABLE reminders ALTER COLUMN day_of_week TYPE TEXT USING day_of_week::text"
            )
            conn.commit()
        except psycopg2.errors.InsufficientPrivilege:
            conn.rollback()
            logger.warning(
                "Нет прав на изменение day_of_week — "
                "попросите DBA выполнить:\n"
                "  ALTER TABLE reminders ALTER COLUMN day_of_week TYPE TEXT USING day_of_week::text;"
            )
        except Exception as e:
            conn.rollback()
            logger.error("Ошибка миграции day_of_week: %s", e)

        # 4) Проверить наличие message_thread_id
        cur.execute("""
          SELECT 1 FROM information_schema.columns
           WHERE table_name='reminders'
             AND column_name='message_thread_id'
        """)
        HAS_THREAD_COL = cur.fetchone() is not None
        cur.close()
    finally:
        put_conn(conn)

# ——— Проверка доступа ——————————————————————————————————
async def is_allowed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM allowed_users WHERE user_id=%s", (user_id,))
        ok = cur.fetchone() is not None
        cur.close()
        return ok
    finally:
        put_conn(conn)

# ——— Глобальный чек статуса автоудаления ——————————————————
def global_autodel_enabled() -> bool:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM config WHERE key='auto_delete_enabled'")
        row = cur.fetchone()
        cur.close()
        return bool(row and row[0] == 'true')
    finally:
        put_conn(conn)

# ——— Отправка отложенных напоминаний —————————————————————
async def send_reminder(chat_id: int, thread_id: int|None, text: str):
    kwargs = {"chat_id": chat_id, "text": text}
    if thread_id is not None:
        kwargs["message_thread_id"] = thread_id
    msg = await application.bot.send_message(**kwargs)
    schedule_deletion(msg.chat_id, msg.message_id)

# ——— Загрузка задач из БД ————————————————————————————
def load_jobs():
    conn = get_conn()
    try:
        cur = conn.cursor()
        if HAS_THREAD_COL:
            cur.execute("""
              SELECT r.id, r.day_of_week, r.time, r.text,
                     r.chat_id, r.message_thread_id,
                     COALESCE(ut.timezone,'UTC')
                FROM reminders r
                LEFT JOIN user_timezones ut ON r.user_id = ut.user_id
            """)
            rows = cur.fetchall()
        else:
            cur.execute("""
              SELECT r.id, r.day_of_week, r.time, r.text,
                     r.chat_id,
                     COALESCE(ut.timezone,'UTC')
                FROM reminders r
                LEFT JOIN user_timezones ut ON r.user_id = ut.user_id
            """)
            tmp = cur.fetchall()
            rows = [(rid,d,tm,txt,cid,None,tz) for (rid,d,tm,txt,cid,tz) in tmp]
        cur.close()
    finally:
        put_conn(conn)
    for rid, d, tm, txt, cid, thr, tz in rows:
        hh, mm = (tm.hour, tm.minute) if hasattr(tm, "hour") else map(int, tm.split(":"))
        scheduler.add_job(
            send_reminder, trigger="cron", id=str(rid),
            day_of_week=RU_TO_CRON_DAY[d],
            hour=hh, minute=mm, timezone=tz,
            args=[cid, thr, txt]
        )

# ——— Handlers ——————————————————————————————————————

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tzrec = None
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT timezone FROM user_timezones WHERE user_id=%s",
                    (update.effective_user.id,))
        tzrec = cur.fetchone()
        cur.close()
    finally:
        put_conn(conn)
    chat_id = update.effective_chat.id
    if not tzrec:
        kb = [[KeyboardButton("📍 Отправить местоположение", request_location=True)]]
        await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": "Привет! Чтобы работать с напоминаниями, отправьте геолокацию:",
            "reply_markup": ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)
        }, update))
    else:
        await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": "С возвращением! Выберите действие:",
            "reply_markup": get_main_keyboard()
        }, update))
        await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": "Или нажмите на inline-кнопку:",
            "reply_markup": INLINE_KB
        }, update))
    try:
        await ctx.bot.delete_message(chat_id, update.message.message_id)
    except:
        pass

async def location_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    loc = update.message.location
    if not loc:
        return
    tz_str = tf.timezone_at(lat=loc.latitude, lng=loc.longitude) or "UTC"
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO user_timezones(user_id, timezone)
            VALUES(%s, %s)
          ON CONFLICT(user_id) DO UPDATE SET timezone = EXCLUDED.timezone
        """, (update.effective_user.id, tz_str))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    chat_id = update.effective_chat.id
    await ctx.bot.send_message(**with_thread({
        "chat_id": chat_id,
        "text": f"Часовой пояс установлен: {tz_str}",
        "reply_markup": get_main_keyboard()
    }, update))
    schedule_deletion(chat_id, update.message.message_id)

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    chat_id = update.effective_chat.id
    try:
        if update.message:
            await ctx.bot.delete_message(chat_id, update.message.message_id)
    except:
        pass
    text = (
        "Команды:\n"
        "/add — добавить напоминание\n"
        "/list — список напоминаний\n"
        "/delete — удалить напоминание по ID\n\n"
        
    )
    msg = await ctx.bot.send_message(**with_thread({
        "chat_id": chat_id,
        "text": text,
        "reply_markup": get_main_keyboard()
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)

async def list_reminders(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        chat_id = update.callback_query.message.chat_id
        uid     = update.callback_query.from_user.id
    else:
        chat_id = update.effective_chat.id
        uid     = update.effective_user.id
        try:
            await ctx.bot.delete_message(chat_id, update.message.message_id)
        except:
            pass
    if not await is_allowed(uid):
        msg = await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": "Доступ запрещён.",
            "reply_markup": get_main_keyboard()
        }, update))
        schedule_deletion(msg.chat_id, msg.message_id)
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
          SELECT id, day_of_week, time, text
            FROM reminders
           WHERE user_id=%s AND chat_id=%s
           ORDER BY id
        """, (uid, chat_id))
        rows = cur.fetchall()
        cur.close()
    finally:
        put_conn(conn)
    text = "Нет напоминаний." if not rows else (
        "Ваши напоминания:\n" +
        "\n".join(f"{r[0]} — {r[1]}, {r[2]}, {r[3]}" for r in rows)
    )
    msg = await ctx.bot.send_message(**with_thread({
        "chat_id": chat_id,
        "text": text,
        "reply_markup": get_main_keyboard()
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)

async def add_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.args or not ctx.args[0].isdigit():
        msg = await update.message.reply_text(
            "Использование: /adduser <user_id>",
            reply_markup=get_main_keyboard()
        )
        schedule_deletion(msg.chat_id, msg.message_id)
        return
    new_id = int(ctx.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO allowed_users(user_id) VALUES(%s) ON CONFLICT DO NOTHING",
            (new_id,)
        )
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    msg = await update.message.reply_text(f"Пользователь {new_id} добавлен.",
                                          reply_markup=get_main_keyboard())
    schedule_deletion(msg.chat_id, msg.message_id)

async def remove_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.args or not ctx.args[0].isdigit():
        msg = await update.message.reply_text(
            "Использование: /removeuser <user_id>",
            reply_markup=get_main_keyboard()
        )
        schedule_deletion(msg.chat_id, msg.message_id)
        return
    rem_id = int(ctx.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM allowed_users WHERE user_id=%s", (rem_id,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    msg = await update.message.reply_text(f"Пользователь {rem_id} удалён.",
                                          reply_markup=get_main_keyboard())
    schedule_deletion(msg.chat_id, msg.message_id)

# ——— Админ: авто-удаление конкретных юзеров ——————————————————
async def add_auto_del_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Использование: /adddeluser <user_id>")
        return
    target = int(ctx.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO auto_delete_users(user_id) VALUES(%s) ON CONFLICT DO NOTHING",
            (target,)
        )
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    await update.message.reply_text(f"Сообщения пользователя {target} будут авто-удаляться.")

async def remove_auto_del_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Использование: /removedeluser <user_id>")
        return
    target = int(ctx.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM auto_delete_users WHERE user_id=%s", (target,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    await update.message.reply_text(f"Пользователь {target} удалён из авто-удаления.")

async def list_auto_del_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM auto_delete_users ORDER BY user_id")
        rows = cur.fetchall()
        cur.close()
    finally:
        put_conn(conn)
    if not rows:
        text = "Список авто-удаляемых пользователей пуст."
    else:
        text = "Авто-удаление сообщений у:\n" + "\n".join(str(r[0]) for r in rows)
    await update.message.reply_text(text)

# ——— Админ: глобальный toggle авто-удаления ——————————————————
async def enable_autodel_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE config SET value='true' WHERE key='auto_delete_enabled'"
        )
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    await update.message.reply_text("Глобальное авто-удаление ВКЛЮЧЕНО.")

async def disable_autodel_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE config SET value='false' WHERE key='auto_delete_enabled'"
        )
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    await update.message.reply_text("Глобальное авто-удаление ОТКЛЮЧЕНО.")

async def status_autodel_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    enabled = global_autodel_enabled()
    await update.message.reply_text(
        f"Глобальное авто-удаление {'ВКЛЮЧЕНО' if enabled else 'ОТКЛЮЧЕНО'}."
    )

async def start_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        chat_id = update.callback_query.message.chat_id
        uid     = update.callback_query.from_user.id
    else:
        chat_id = update.effective_chat.id
        uid     = update.effective_user.id
        try:
            await ctx.bot.delete_message(chat_id, update.message.message_id)
        except:
            pass
    if not await is_allowed(uid):
        msg = await ctx.bot.send_message(chat_id=chat_id,
                                         text="Доступ запрещён.",
                                         reply_markup=get_main_keyboard())
        schedule_deletion(msg.chat_id, msg.message_id)
        return ConversationHandler.END
    ctx.user_data["thread_id"] = get_thread_id(update)
    msg = await ctx.bot.send_message(**with_thread({
        "chat_id": chat_id,
        "text": "Введите напоминание в формате:\n<день недели> <HH:MM> <текст>"
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)
    return ADD_INPUT

async def add_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    parts = text.split(" ", 2)
    if len(parts) < 3:
        msg = await ctx.bot.send_message(**with_thread({
            "chat_id": update.effective_chat.id,
            "text": "Неверный формат. Попробуйте ещё раз или /cancel.",
            "reply_markup": get_main_keyboard()
        }, update))
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    day_raw, time_str, rem_text = parts
    if day_raw.isdigit():
        day = NUM_TO_RU_DAY.get(day_raw)
        if not day:
            msg = await ctx.bot.send_message(**with_thread({
                "chat_id": update.effective_chat.id,
                "text": "Неверный номер дня. Введите 1–7 или название дня.",
                "reply_markup": get_main_keyboard()
            }, update))
            schedule_deletion(msg.chat_id, msg.message_id)
            return ADD_INPUT
    else:
        day = day_raw.lower()
    if day not in RU_TO_CRON_DAY:
        msg = await ctx.bot.send_message(**with_thread({
                "chat_id": update.effective_chat.id,
                "text": "Неверный день недели.",
                "reply_markup": get_main_keyboard()
        }, update))
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    try:
        hh, mm = map(int, time_str.split(":"))
        assert 0 <= hh < 24 and 0 <= mm < 60
    except:
        msg = await ctx.bot.send_message(**with_thread({
                "chat_id": update.effective_chat.id,
                "text": "Неверный формат времени.",
                "reply_markup": get_main_keyboard()
        }, update))
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    uid       = update.effective_user.id
    chat_id   = update.effective_chat.id
    thread_id = ctx.user_data.get("thread_id")
    if HAS_THREAD_COL:
        sql = ("INSERT INTO reminders(user_id,chat_id,message_thread_id,"
               "day_of_week,time,text) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id")
        params = (uid, chat_id, thread_id, day, time_str, rem_text)
    else:
        sql = ("INSERT INTO reminders(user_id,chat_id,day_of_week,time,text)"
               " VALUES(%s,%s,%s,%s,%s) RETURNING id")
        params = (uid, chat_id, day, time_str, rem_text)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rid = cur.fetchone()[0]
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT timezone FROM user_timezones WHERE user_id=%s", (uid,))
        tzrow = cur.fetchone()
        cur.close()
    finally:
        put_conn(conn)
    tz = tzrow[0] if tzrow else "UTC"

    scheduler.add_job(
        send_reminder,
        trigger="cron",
        id=str(rid),
        day_of_week=RU_TO_CRON_DAY[day],
        hour=hh, minute=mm,
        timezone=tz,
        args=[chat_id, thread_id, rem_text]
    )

    msg = await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": f"Напоминание #{rid} добавлено.",
            "reply_markup": get_main_keyboard()
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)
    return ConversationHandler.END

async def start_delete(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        chat_id = update.callback_query.message.chat_id
        uid     = update.callback_query.from_user.id
    else:
        chat_id = update.effective_chat.id
        uid     = update.effective_user.id
        try:
            await ctx.bot.delete_message(chat_id, update.message.message_id)
        except:
            pass
    if not await is_allowed(uid):
        msg = await ctx.bot.send_message(chat_id=chat_id,
                                         text="Доступ запрещён.",
                                         reply_markup=get_main_keyboard())
        schedule_deletion(msg.chat_id, msg.message_id)
        return ConversationHandler.END
    ctx.user_data["thread_id"] = get_thread_id(update)
    msg = await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": "Введите ID напоминания для удаления:"
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)
    return DELETE_INPUT

async def delete_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text or ""
    if not txt.isdigit():
        msg = await ctx.bot.send_message(**with_thread({
                "chat_id": update.effective_chat.id,
                "text": "ID должен быть числом.",
                "reply_markup": get_main_keyboard()
        }, update))
        schedule_deletion(msg.chat_id, msg.message_id)
        return DELETE_INPUT

    rid     = int(txt)
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM reminders WHERE id=%s AND user_id=%s AND chat_id=%s",
            (rid, uid, chat_id)
        )
        if cur.fetchone() is None:
            cur.close()
            msg = await ctx.bot.send_message(**with_thread({
                    "chat_id": chat_id,
                    "text": "Напоминание не найдено.",
                    "reply_markup": get_main_keyboard()
            }, update))
            schedule_deletion(msg.chat_id, msg.message_id)
            return ConversationHandler.END

        cur.execute("DELETE FROM reminders WHERE id=%s", (rid,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    try:
        scheduler.remove_job(str(rid))
    except:
        pass

    msg = await ctx.bot.send_message(**with_thread({
            "chat_id": chat_id,
            "text": f"Напоминание #{rid} удалено.",
            "reply_markup": get_main_keyboard()
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)
    return ConversationHandler.END

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message:
        try:
            await ctx.bot.delete_message(update.effective_chat.id,
                                         update.message.message_id)
        except:
            pass
    msg = await ctx.bot.send_message(**with_thread({
            "chat_id": update.effective_chat.id,
            "text": "Операция отменена.",
            "reply_markup": get_main_keyboard()
    }, update))
    schedule_deletion(msg.chat_id, msg.message_id)
    return ConversationHandler.END

# ——— Удаление сообщений только у выбранных пользователей ————————
async def delete_user_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.from_user:
        return
    # глобальный switch
    if not global_autodel_enabled():
        return
    user_id = msg.from_user.id
    # не удаляем бота
    if user_id == ctx.bot.id:
        return
    # проверяем, включено ли авто-удаление для этого user_id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM auto_delete_users WHERE user_id=%s", (user_id,))
        to_del = cur.fetchone() is not None
        cur.close()
    finally:
        put_conn(conn)
    if to_del:
        try:
            await ctx.bot.delete_message(chat_id=msg.chat_id,
                                         message_id=msg.message_id)
        except:
            pass

async def on_startup(app):
    init_db()
    if scheduler.state == STATE_STOPPED:
        scheduler.start()
        logger.info("Scheduler started")
    load_jobs()

if __name__ == "__main__":
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    # 1) ConversationHandlers
    add_conv = ConversationHandler(
        entry_points=[
            CommandHandler("add", start_add),
            MessageHandler(filters.Regex(r"^Добавить$"), start_add),
            CallbackQueryHandler(start_add, pattern="^add$")
        ],
        states={ADD_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_input)]},
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start",  cancel),
            CommandHandler("help",   cancel),
        ],
        per_chat=True, per_user=True,
        allow_reentry=True
    )
    del_conv = ConversationHandler(
        entry_points=[
            CommandHandler("delete", start_delete),
            MessageHandler(filters.Regex(r"^Удалить$"), start_delete),
            CallbackQueryHandler(start_delete, pattern="^delete$")
        ],
        states={DELETE_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_input)]},
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start",  cancel),
            CommandHandler("help",   cancel),
        ],
        per_chat=True, per_user=True,
        allow_reentry=True
    )
    application.add_handler(add_conv)
    application.add_handler(del_conv)

    # 2) Основные хендлеры
    application.add_handler(CommandHandler("start",  start))
    application.add_handler(MessageHandler(filters.LOCATION, location_handler))
    application.add_handler(CommandHandler("help",   help_cmd))

    # 3) Список и inline-кнопки
    application.add_handler(MessageHandler(filters.Regex(r"^Список$"), list_reminders))
    application.add_handler(CallbackQueryHandler(list_reminders, pattern="^list$"))
    application.add_handler(CallbackQueryHandler(help_cmd,        pattern="^help$"))

    # 4) Админские команды
    application.add_handler(CommandHandler("list",       list_reminders))
    application.add_handler(CommandHandler("adduser",    add_user))
    application.add_handler(CommandHandler("removeuser", remove_user))

    # 5) Авто-удаление конкретных юзеров
    application.add_handler(CommandHandler("adddeluser",    add_auto_del_user))
    application.add_handler(CommandHandler("removedeluser", remove_auto_del_user))
    application.add_handler(CommandHandler("listdelusers",  list_auto_del_users))

    # 6) Глобальные переключатели авто-удаления
    application.add_handler(CommandHandler("enableautodel",  enable_autodel_all))
    application.add_handler(CommandHandler("disableautodel", disable_autodel_all))
    application.add_handler(CommandHandler("autodelstatus",  status_autodel_all))

    # 7) Удаление сообщений выбранных пользователей (после всех)
    application.add_handler(
        MessageHandler(filters.ALL, delete_user_message),
        group=99
    )

    application.run_polling()
