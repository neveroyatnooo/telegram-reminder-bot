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

# ——— Карта дней недели (0=понедельник, …, 6=воскресенье) —————————————
RU_TO_CRON_DAY = {
    "понедельник": 0,
    "вторник":     1,
    "среда":       2,
    "четверг":     3,
    "пятница":     4,
    "суббота":     5,
    "воскресенье": 6
}

# флаг: есть ли колонка message_thread_id в reminders
HAS_THREAD_COL = False

# ——— Helpers для форумных тем (threads) ————————————————————————

def get_thread_id(update: Update) -> int | None:
    return getattr(update.effective_message, "message_thread_id", None)

def with_thread(kwargs: dict, update: Update) -> dict:
    tid = get_thread_id(update)
    if tid is not None:
        kwargs["message_thread_id"] = tid
    return kwargs

# ——— Клавиатуры ——————————————————————————————————————

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

# ——— Load .env ——————————————————————————————————————————

env = Path(__file__).parent / ".env"
load_dotenv(env)

BOT_TOKEN   = os.getenv("BOT_TOKEN")
DB_HOST     = os.getenv("DB_HOST",    "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT",    "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_IDS   = set(int(x) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip().isdigit())

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

# ——— Пул соединений с БД —————————————————————————————————

db_pool = ThreadedConnectionPool(
    1, 10,
    host=DB_HOST, port=DB_PORT,
    dbname=DB_NAME, user=DB_USER,
    password=DB_PASSWORD
)
def get_conn(): return db_pool.getconn()
def put_conn(conn): return db_pool.putconn(conn)

# ——— Планировщик ——————————————————————————————————————

scheduler = AsyncIOScheduler()
DELETE_DELAY_HOURS = 2
ADD_INPUT, DELETE_INPUT = range(2)

def delete_msg(chat_id: int, message_id: int):
    asyncio.create_task(
        application.bot.delete_message(chat_id=chat_id, message_id=message_id)
    )

def schedule_deletion(chat_id: int, message_id: int,
                      delay_hours: int = DELETE_DELAY_HOURS):
    run_date = datetime.datetime.now(timezone.utc) + timedelta(hours=delay_hours)
    scheduler.add_job(delete_msg,
                      trigger="date",
                      run_date=run_date,
                      args=[chat_id, message_id])

tf = TimezoneFinder()

# ——— Инициализация БД + миграция —————————————————————————

def init_db():
    global HAS_THREAD_COL
    conn = get_conn()
    try:
        cur = conn.cursor()
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
        """)
        conn.commit()
        try:
            cur.execute("ALTER TABLE reminders ADD COLUMN IF NOT EXISTS message_thread_id BIGINT")
            conn.commit()
        except psycopg2.errors.InsufficientPrivilege:
            conn.rollback()
            logger.warning("Нет прав на ALTER TABLE reminders — пропускаем")
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

# ——— Отправка отложенных напоминаний ——————————————————————

async def send_reminder(chat_id: int, thread_id: int | None, text: str):
    kwargs = {"chat_id": chat_id, "text": text}
    if thread_id is not None:
        kwargs["message_thread_id"] = thread_id
    msg = await application.bot.send_message(**kwargs)
    schedule_deletion(msg.chat_id, msg.message_id)

# ——— Загрузка задач в планировщик ——————————————————————

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
            rows = [(rid, day, tm, txt, cid, None, tz)
                    for (rid, day, tm, txt, cid, tz) in tmp]
        cur.close()
    finally:
        put_conn(conn)

    for rid, day, tm, txt, cid, thr_id, tz in rows:
        if hasattr(tm, "hour"):
            hh, mm = tm.hour, tm.minute
        else:
            hh, mm = map(int, tm.split(":"))
        scheduler.add_job(
            send_reminder,
            trigger="cron",
            id=str(rid),
            day_of_week=RU_TO_CRON_DAY[day],
            hour=hh, minute=mm,
            timezone=tz,
            args=[cid, thr_id, txt]
        )

# ——— Handlers ——————————————————————————————————————

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT timezone FROM user_timezones WHERE user_id=%s", (uid,))
        tzrec = cur.fetchone()
        cur.close()
    finally:
        put_conn(conn)

    if not tzrec:
        kb = [[KeyboardButton("📍 Отправить местоположение", request_location=True)]]
        msg = await update.message.reply_text(
            "Привет! Отправьте геолокацию для установки часового пояса:",
            **with_thread({"reply_markup": ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)}, update)
        )
        schedule_deletion(msg.chat_id, msg.message_id)
    else:
        msg1 = await update.message.reply_text(
            "С возвращением! Выберите действие:",
            **with_thread({"reply_markup": get_main_keyboard()}, update)
        )
        schedule_deletion(msg1.chat_id, msg1.message_id)
        msg2 = await update.message.reply_text(
            "Или нажмите на inline-кнопку:",
            **with_thread({"reply_markup": INLINE_KB}, update)
        )
        schedule_deletion(msg2.chat_id, msg2.message_id)

    try:
        await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
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
          INSERT INTO user_timezones(user_id,timezone)
            VALUES(%s,%s)
          ON CONFLICT(user_id) DO UPDATE SET timezone=EXCLUDED.timezone
        """, (update.effective_user.id, tz_str))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    msg = await update.message.reply_text(
        f"Часовой пояс установлен: {tz_str}",
        **with_thread({"reply_markup": get_main_keyboard()}, update)
    )
    schedule_deletion(msg.chat_id, msg.message_id)
    try:
        await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except:
        pass

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    if update.message:
        try:
            await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
        except:
            pass
    text = (
      "Команды:\n"
      "/add — добавить напоминание\n"
      "/list — список напоминаний\n"
      "/delete — удалить напоминание по ID\n\n"
      "Админ:\n"
      "/adduser — добавить пользователя\n"
      "/removeuser — удалить пользователя"
    )
    msg = await ctx.bot.send_message(
        **with_thread({"chat_id": update.effective_chat.id,
                       "text": text,
                       "reply_markup": get_main_keyboard()}, update)
    )
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
        msg = await ctx.bot.send_message(
            **with_thread({"chat_id": chat_id,
                           "text": "Доступ запрещён.",
                           "reply_markup": get_main_keyboard()}, update)
        )
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

    if not rows:
        text = "Нет напоминаний."
    else:
        lines = [f"{r[0]} — {r[1]}, {r[2]}, {r[3]}" for r in rows]
        text = "Ваши напоминания:\n" + "\n".join(lines)

    msg = await ctx.bot.send_message(
        **with_thread({"chat_id": chat_id,
                       "text": text,
                       "reply_markup": get_main_keyboard()}, update)
    )
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
        cur.execute("INSERT INTO allowed_users(user_id) VALUES(%s) ON CONFLICT DO NOTHING",
                    (new_id,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    msg = await update.message.reply_text(
        f"Пользователь {new_id} добавлен.",
        reply_markup=get_main_keyboard()
    )
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
    msg = await update.message.reply_text(
        f"Пользователь {rem_id} удалён.",
        reply_markup=get_main_keyboard()
    )
    schedule_deletion(msg.chat_id, msg.message_id)

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
        msg = await ctx.bot.send_message(chat_id, "Доступ запрещён.", reply_markup=get_main_keyboard())
        schedule_deletion(msg.chat_id, msg.message_id)
        return ConversationHandler.END

    msg = await ctx.bot.send_message(chat_id,
        "Введите напоминание в формате:\n<день недели> <HH:MM> <текст>"
    )
    schedule_deletion(msg.chat_id, msg.message_id)
    return ADD_INPUT

async def add_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    parts = text.split(" ", 2)
    if len(parts) < 3:
        msg = await update.message.reply_text(
            "Неверный формат. Попробуйте ещё раз или /cancel.",
            reply_markup=get_main_keyboard()
        )
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    day, time_str, rem_text = parts
    day = day.lower()
    if day not in RU_TO_CRON_DAY:
        msg = await update.message.reply_text(
            "Неверный день недели.",
            reply_markup=get_main_keyboard()
        )
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    try:
        hh, mm = map(int, time_str.split(":"))
        assert 0 <= hh < 24 and 0 <= mm < 60
    except:
        msg = await update.message.reply_text(
            "Неверный формат времени.",
            reply_markup=get_main_keyboard()
        )
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    uid       = update.effective_user.id
    chat_id   = update.effective_chat.id
    thread_id = get_thread_id(update)

    if HAS_THREAD_COL:
        sql    = ("INSERT INTO reminders(user_id,chat_id,message_thread_id,"
                  "day_of_week,time,text) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id")
        params = (uid, chat_id, thread_id, day, time_str, rem_text)
    else:
        sql    = ("INSERT INTO reminders(user_id,chat_id,day_of_week,time,text)"
                  " VALUES(%s,%s,%s,%s,%s) RETURNING id")
        params = (uid, chat_id,       day,    time_str, rem_text)

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

    msg = await update.message.reply_text(
        f"Напоминание #{rid} добавлено.",
        reply_markup=get_main_keyboard()
    )
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
        msg = await ctx.bot.send_message(chat_id, "Доступ запрещён.", reply_markup=get_main_keyboard())
        schedule_deletion(msg.chat_id, msg.message_id)
        return ConversationHandler.END

    msg = await ctx.bot.send_message(chat_id, "Введите ID напоминания для удаления:")
    schedule_deletion(msg.chat_id, msg.message_id)
    return DELETE_INPUT

async def delete_input(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text or ""
    if not txt.isdigit():
        msg = await update.message.reply_text(
            "ID должен быть числом.",
            reply_markup=get_main_keyboard()
        )
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
            msg = await update.message.reply_text(
                "Напоминание не найдено.",
                reply_markup=get_main_keyboard()
            )
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

    msg = await update.message.reply_text(
        f"Напоминание #{rid} удалено.",
        reply_markup=get_main_keyboard()
    )
    schedule_deletion(msg.chat_id, msg.message_id)
    return ConversationHandler.END

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message:
        try:
            await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
        except:
            pass
    msg = await ctx.bot.send_message(update.effective_chat.id, "Операция отменена.",
                                     reply_markup=get_main_keyboard())
    schedule_deletion(msg.chat_id, msg.message_id)
    return ConversationHandler.END

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

    # /start, /help, локация
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help",  help_cmd))
    application.add_handler(MessageHandler(filters.LOCATION, location_handler))

    # Conversation для /add и кнопки «Добавить»
    add_conv = ConversationHandler(
        entry_points=[
            CommandHandler("add", start_add),
            MessageHandler(filters.Regex(r"^Добавить$"), start_add),
            CallbackQueryHandler(start_add, pattern="^add$")
        ],
        states={ ADD_INPUT: [ MessageHandler(filters.TEXT & ~filters.COMMAND, add_input) ]},
        fallbacks=[ CommandHandler("cancel", cancel) ],
        per_chat=True, per_user=True
    )
    application.add_handler(add_conv)

    # Conversation для /delete и кнопки «Удалить»
    del_conv = ConversationHandler(
        entry_points=[
            CommandHandler("delete", start_delete),
            MessageHandler(filters.Regex(r"^Удалить$"), start_delete),
            CallbackQueryHandler(start_delete, pattern="^delete$")
        ],
        states={ DELETE_INPUT: [ MessageHandler(filters.TEXT & ~filters.COMMAND, delete_input) ]},
        fallbacks=[ CommandHandler("cancel", cancel) ],
        per_chat=True, per_user=True
    )
    application.add_handler(del_conv)

    # «Список» и «Помощь» (reply и inline)
    application.add_handler(MessageHandler(filters.Regex(r"^Список$"), list_reminders))
    application.add_handler(CallbackQueryHandler(list_reminders, pattern="^list$"))
    application.add_handler(MessageHandler(filters.Regex(r"^Помощь$"), help_cmd))
    application.add_handler(CallbackQueryHandler(help_cmd,      pattern="^help$"))

    # Админские
    application.add_handler(CommandHandler("list",     list_reminders))
    application.add_handler(CommandHandler("adduser",  add_user))
    application.add_handler(CommandHandler("removeuser",remove_user))

    application.run_polling()
