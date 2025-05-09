import os
import logging
import datetime
import asyncio
from datetime import timedelta, timezone
from pathlib import Path
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
    ReplyKeyboardRemove,
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
from telegram import ReplyKeyboardMarkup, KeyboardButton


def get_main_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("➕ Добавить"), KeyboardButton("📋 Список")],
            [KeyboardButton("❌ Удалить"), KeyboardButton("/help")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )



env = Path(__file__).parent / ".env"
load_dotenv(env)
BOT_TOKEN   = os.getenv("BOT_TOKEN")
DB_HOST     = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_IDS   = set(int(x) for x in os.getenv("ADMIN_IDS","").split(",") if x.strip().isdigit())

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


db_pool = ThreadedConnectionPool(
    1, 10,
    host=DB_HOST, port=DB_PORT,
    dbname=DB_NAME, user=DB_USER,
    password=DB_PASSWORD
)
def get_conn(): return db_pool.getconn()
def put_conn(conn): db_pool.putconn(conn)


scheduler = AsyncIOScheduler()
RU_TO_CRON_DAY = {
    "понедельник":"mon","вторник":"tue","среда":"wed",
    "четверг":"thu","пятница":"fri","суббота":"sat",
    "воскресенье":"sun"
}
DELETE_DELAY_HOURS = 2    
ADD_INPUT, DELETE_INPUT = range(2)


def delete_msg(chat_id: int, message_id: int):
    asyncio.create_task(
        application.bot.delete_message(chat_id=chat_id, message_id=message_id)
    )


def schedule_deletion(chat_id: int, message_id: int, delay_hours: int = DELETE_DELAY_HOURS):
    run_date = datetime.datetime.now(timezone.utc) + timedelta(hours=delay_hours)
    scheduler.add_job(delete_msg, trigger="date", run_date=run_date, args=[chat_id, message_id])

tf = TimezoneFinder()


def init_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
          user_id BIGINT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS reminders (
          id           SERIAL PRIMARY KEY,
          user_id      BIGINT NOT NULL REFERENCES allowed_users(user_id) ON DELETE CASCADE,
          chat_id      BIGINT NOT NULL,
          day_of_week  VARCHAR(10) NOT NULL,
          time         TIME NOT NULL,
          text         TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS user_timezones (
          user_id  BIGINT PRIMARY KEY,
          timezone VARCHAR(50) NOT NULL
        );
        """)
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)


async def is_allowed(user_id:int) -> bool:
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


async def send_reminder(chat_id:int, text:str):
    msg = await application.bot.send_message(chat_id=chat_id, text=text)
    schedule_deletion(msg.chat_id, msg.message_id)


def load_jobs():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
          SELECT r.id, r.day_of_week, r.time, r.text, r.chat_id,
                 COALESCE(ut.timezone,'UTC')
          FROM reminders r
          LEFT JOIN user_timezones ut ON r.user_id = ut.user_id
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        put_conn(conn)

    for rid, day, tm, txt, cid, tz in rows:
        hh, mm = (tm.hour, tm.minute) if isinstance(tm, datetime.time) else map(int, tm.split(":"))
        scheduler.add_job(
            send_reminder,
            trigger="cron",
            id=str(rid),
            day_of_week=RU_TO_CRON_DAY[day],
            hour=hh, minute=mm,
            timezone=tz,
            args=[cid, txt]
        )


async def start(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT timezone FROM user_timezones WHERE user_id=%s", (uid,))
        row = cur.fetchone()
        cur.close()
    finally:
        put_conn(conn)

    if not row:
        kb_loc = [[KeyboardButton("📍 Отправить местоположение", request_location=True)]]
        msg = await update.message.reply_text(
            "Привет! Чтобы работать с напоминаниями, мне нужен Ваш часовой пояс.\n"
            "Пожалуйста, поделитесь геолокацией:",
            reply_markup=ReplyKeyboardMarkup(kb_loc, resize_keyboard=True, one_time_keyboard=True)
        )
    else:
         msg = await update.message.reply_text(
             "С возвращением! Выберите действие:",
             reply_markup=get_main_keyboard()
         )

    schedule_deletion(msg.chat_id, msg.message_id)
    try:
        await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except:
        pass


async def location_handler(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
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
        f"Часовой пояс установлен: {tz_str}\n"
        "Теперь вы можете добавлять напоминания.\n"
        "Используйте /help для списка команд.",
        reply_markup=ReplyKeyboardRemove()
    )
    schedule_deletion(msg.chat_id, msg.message_id)
    try:
        await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except:
        pass


async def help_cmd(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    text = (
      "Команды:\n"
      "/add — добавить напоминание\n"
      "/list — список напоминаний\n"
      "/delete — удалить напоминание по ID\n\n"
      "Админ:\n"
      "/adduser — добавить пользователя\n"
      "/removeuser — удалить пользователя"
    )
    msg = await update.message.reply_text(text, reply_markup=get_main_keyboard())
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass



async def help_button_handler(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    msg = await update.callback_query.message.reply_text(
        "Команды:\n"
        "/add — добавить напоминание\n"
        "/list — список напоминаний\n"
        "/delete — удалить напоминание по ID\n\n"
        "Админ:\n"
        "/adduser — добавить пользователя\n"
        "/removeuser — удалить пользователя"
    )
    schedule_deletion(msg.chat_id, msg.message_id)


async def list_reminders(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await is_allowed(uid):
        msg = await update.message.reply_text("Доступ запрещён.")
        schedule_deletion(msg.chat_id, msg.message_id)
        try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
        except: pass
        return

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
          "SELECT id, day_of_week, time, text FROM reminders WHERE user_id=%s ORDER BY id",
          (uid,)
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        put_conn(conn)

    if not rows:
        text = "Нет напоминаний."
    else:
        text = "Ваши напоминания:\n" + "\n".join(f"{r[0]} — {r[1]}, {r[2]}, {r[3]}" for r in rows)

    msg = await update.message.reply_text(text)
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass


async def add_user(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.args or not ctx.args[0].isdigit():
        msg = await update.message.reply_text("Использование: /adduser <user_id>")
        schedule_deletion(msg.chat_id, msg.message_id)
        try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
        except: pass
        return

    new_id = int(ctx.args[0])
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO allowed_users(user_id) VALUES(%s) ON CONFLICT DO NOTHING", (new_id,))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    msg = await update.message.reply_text(f"Пользователь {new_id} добавлен.")
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass


async def remove_user(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.args or not ctx.args[0].isdigit():
        msg = await update.message.reply_text("Использование: /removeuser <user_id>")
        schedule_deletion(msg.chat_id, msg.message_id)
        try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
        except: pass
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

    msg = await update.message.reply_text(f"Пользователь {rem_id} удалён.")
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass


async def start_add(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await is_allowed(uid):
        msg = await update.message.reply_text("Доступ запрещён.")
        schedule_deletion(msg.chat_id, msg.message_id)
        return ConversationHandler.END

    msg = await update.message.reply_text(
        "Отправьте напоминание в формате:\n<день недели> <HH:MM> <текст>"
    )
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass
    return ADD_INPUT


async def add_input(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    parts = text.split(" ", 2)
    if len(parts) < 3:
        msg = await update.message.reply_text("Неверный формат. Попробуйте ещё раз или /cancel.")
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    day, time_str, reminder_text = parts
    day = day.lower()
    if day not in RU_TO_CRON_DAY:
        msg = await update.message.reply_text("Неверный день недели. Попробуйте ещё раз.")
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    try:
        hh, mm = map(int, time_str.split(":"))
        assert 0 <= hh < 24 and 0 <= mm < 60
    except:
        msg = await update.message.reply_text("Неверный формат времени. Попробуйте ещё раз.")
        schedule_deletion(msg.chat_id, msg.message_id)
        return ADD_INPUT

    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
          "INSERT INTO reminders(user_id,chat_id,day_of_week,time,text) "
          "VALUES(%s,%s,%s,%s,%s) RETURNING id",
          (uid, chat_id, day, time_str, reminder_text)
        )
        rid = cur.fetchone()[0]
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT timezone FROM user_timezones WHERE user_id=%s", (uid,))
        row = cur.fetchone()
        cur.close()
    finally:
        put_conn(conn)

    tz = row[0] if row else "UTC"
    scheduler.add_job(
        send_reminder,
        trigger="cron",
        id=str(rid),
        day_of_week=RU_TO_CRON_DAY[day],
        hour=hh, minute=mm,
        timezone=tz,
        args=[chat_id, reminder_text]
    )

    msg = await update.message.reply_text(f"Напоминание #{rid} добавлено.")
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(chat_id, update.message.message_id)
    except: pass
    return ConversationHandler.END


async def start_delete(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not await is_allowed(uid):
        msg = await update.message.reply_text("Доступ запрещён.")
        schedule_deletion(msg.chat_id, msg.message_id)
        return ConversationHandler.END

    msg = await update.message.reply_text("Отправьте ID напоминания для удаления:")
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass
    return DELETE_INPUT


async def delete_input(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    txt = update.message.text or ""
    if not txt.isdigit():
        msg = await update.message.reply_text("ID должен быть числом. Попробуйте ещё раз.")
        schedule_deletion(msg.chat_id, msg.message_id)
        return DELETE_INPUT

    rid = int(txt)
    uid = update.effective_user.id
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM reminders WHERE id=%s AND user_id=%s", (rid, uid))
        if cur.fetchone() is None:
            cur.close()
            msg = await update.message.reply_text("Напоминание не найдено.")
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

    msg = await update.message.reply_text(f"Напоминание #{rid} удалено.")
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass
    return ConversationHandler.END


async def cancel(update:Update, ctx:ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("Операция отменена.")
    schedule_deletion(msg.chat_id, msg.message_id)
    try: await ctx.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except: pass
    return ConversationHandler.END


async def on_startup(app):
    if scheduler.state == STATE_STOPPED:
        init_db()
        scheduler.start()
        logger.info("Scheduler started")
        load_jobs()




if __name__ == '__main__':
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

   
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CallbackQueryHandler(help_button_handler, pattern="^help$"))
    application.add_handler(MessageHandler(filters.LOCATION, location_handler))

    
    application.add_handler(
        MessageHandler(filters.Regex(r"^📋 Список$"), list_reminders)
        )
    application.add_handler(
        MessageHandler(filters.Regex(r"^/help$"), help_cmd)
        )


    add_conv = ConversationHandler(
        entry_points=[
            CommandHandler("add", start_add),
            MessageHandler(filters.Regex(r"^➕ Добавить$"), start_add),
         ],
        states={
            ADD_INPUT: [
            
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_input)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        )
    application.add_handler(add_conv)


    del_conv = ConversationHandler(
        entry_points=[
            CommandHandler("delete", start_delete),
            MessageHandler(filters.Regex(r"^❌ Удалить$"), start_delete),
            ],
        states={
            DELETE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, delete_input)
                ],
            },
        fallbacks=[CommandHandler("cancel", cancel)],
        )
    application.add_handler(del_conv)


    
    application.add_handler(CommandHandler("list", list_reminders))
    application.add_handler(CommandHandler("adduser", add_user))
    application.add_handler(CommandHandler("removeuser", remove_user))

    application.run_polling()
