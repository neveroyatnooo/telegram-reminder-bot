
import os
import logging
import time
import signal
import datetime                                    
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_STOPPED

from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler


env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

BOT_TOKEN   = os.getenv("BOT_TOKEN")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_IDS   = set(
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
)


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


db_pool = ThreadedConnectionPool(
    1, 10,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
def get_conn():
    return db_pool.getconn()
def put_conn(conn):
    db_pool.putconn(conn)


scheduler = AsyncIOScheduler(timezone="Asia/Yekaterinburg")
RU_TO_CRON_DAY = {
    "понедельник":"mon","вторник":"tue","среда":"wed",
    "четверг":"thu","пятница":"fri","суббота":"sat",
    "воскресенье":"sun"
}


async def is_allowed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM allowed_users WHERE user_id = %s", (user_id,))
        ok = cur.fetchone() is not None
        cur.close()
        return ok
    finally:
        put_conn(conn)

async def send_reminder(chat_id: int, text: str):
    await application.bot.send_message(chat_id=chat_id, text=text)

def load_jobs():
    """
    Загружает из БД все напоминания и регистрирует их в планировщике.
    Поле time может приходить как datetime.time или строкой "HH:MM".
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, day_of_week, time, text, chat_id FROM reminders")
        rows = cur.fetchall()
        cur.close()
    finally:
        put_conn(conn)

    for rid, day, tm, txt, cid in rows:
        if isinstance(tm, datetime.time):
            hh, mm = tm.hour, tm.minute
        else:
            hh, mm = map(int, tm.split(":"))
        scheduler.add_job(
            send_reminder,
            trigger="cron",
            id=str(rid),
            day_of_week=RU_TO_CRON_DAY[day],
            hour=hh, minute=mm,
            args=[cid, txt]
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я бот-напоминалка.\n"
        "Используй /help для списка команд."
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Команды:\n"
        "/add <день> <HH:MM> <текст> — добавить напоминание\n"
        "/list — показать напоминания\n"
        "/delete <id> — удалить напоминание\n\n"
        "Админ:\n"
        "/adduser <user_id> — разрешить доступ\n"
        "/removeuser <user_id> — запретить доступ"
    )

async def add_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await is_allowed(uid):
        return await update.message.reply_text("Доступ запрещён.")

    parts = update.message.text.partition(" ")[2].split("/", 2)
    if len(parts) < 3:
        return await update.message.reply_text("Неверный формат. /add день/HH:MM/текст")
    day_str, time_str, text = parts
    day = day_str.strip().lower()
    if day not in RU_TO_CRON_DAY:
        return await update.message.reply_text("Неверный день недели.")

    try:
        hh, mm = map(int, time_str.split(":"))
        if not (0 <= hh < 24 and 0 <= mm < 60):
            raise ValueError
    except:
        return await update.message.reply_text("Неверное время. HH:MM")

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO reminders(user_id,chat_id,day_of_week,time,text) "
            "VALUES(%s,%s,%s,%s,%s) RETURNING id",
            (uid, update.effective_chat.id, day, time_str, text)
        )
        rid = cur.fetchone()[0]
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    
    scheduler.add_job(
        send_reminder,
        trigger="cron",
        id=str(rid),
        day_of_week=RU_TO_CRON_DAY[day],
        hour=hh, minute=mm,
        args=[update.effective_chat.id, text]
    )
    await update.message.reply_text(f"Напоминание #{rid} добавлено.")

async def list_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await is_allowed(uid):
        return await update.message.reply_text("Доступ запрещён.")
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, day_of_week, time, text FROM reminders "
            "WHERE user_id=%s ORDER BY id", (uid,)
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        put_conn(conn)

    if not rows:
        return await update.message.reply_text("Напоминаний нет.")
    msg = "\n".join(f"{r[0]} — {r[1]}, {r[2]}, {r[3]}" for r in rows)
    await update.message.reply_text("Ваши напоминания:\n" + msg)

async def delete_reminder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await is_allowed(uid):
        return await update.message.reply_text("Доступ запрещён.")
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Использование: /delete <id>")
    rid = int(context.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM reminders WHERE id=%s AND user_id=%s", (rid, uid)
        )
        if cur.fetchone() is None:
            cur.close()
            return await update.message.reply_text("Напоминание не найдено.")
        cur.execute("DELETE FROM reminders WHERE id=%s", (rid,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    try:
        scheduler.remove_job(str(rid))
    except:
        pass
    await update.message.reply_text(f"Напоминание #{rid} удалено.")

async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Использование: /adduser <user_id>")
    new_id = int(context.args[0])
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
    await update.message.reply_text(f"Пользователь {new_id} добавлен.")

async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args or not context.args[0].isdigit():
        return await update.message.reply_text("Использование: /removeuser <user_id>")
    rem_id = int(context.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM allowed_users WHERE user_id=%s", (rem_id,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)
    await update.message.reply_text(f"Пользователь {rem_id} удалён.")


async def on_startup(app):
    if scheduler.state == STATE_STOPPED:
        scheduler.start()
        logger.info("Scheduler started")
        load_jobs()


if __name__ == '__main__':
application = ApplicationBuilder().token(BOT_TOKEN).build()


application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("help", help_cmd))
application.add_handler(CommandHandler("add", add_reminder))
application.add_handler(CommandHandler("list", list_reminders))
application.add_handler(CommandHandler("delete", delete_reminder))
application.add_handler(CommandHandler("adduser", add_user))
application.add_handler(CommandHandler("removeuser", remove_user))


application.run_polling(stop_signals=())










