import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, PicklePersistence, BasePersistence
import uuid

from typing import Any, Callable, Dict, Optional, Set, Tuple, Type, TypeVar, cast, overload
import time
from copy import deepcopy
import os

from telegram.ext import BasePersistence, PersistenceInput
from telegram.ext._contexttypes import ContextTypes
from telegram.ext._utils.types import BD, CD, UD, CDCData, ConversationDict, ConversationKey
from telegram._utils.types import FilePathInput
from pathlib import Path

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN = os.environ.get("TELEGRAM_TOKEN")

ALLOWED_CHATS = {
    "test_chat": -762022584,
    "alexey": 316821571
}
ALLOWED_CHAT_IDS = ALLOWED_CHATS.values()

AVAILABLE_DATES = [
    "12.8",
    "19.8"
]

COLUMN_TYPES = {"username": str, "date": str, "num_participants": int, "timestamp": float}

def _remove_entry(d: dict, username: str, date: str):
    to_pop = []
    for k, v in d.items():
        if v["username"] == username and v["date"] == date:
            to_pop.append(k)
    for k in to_pop:
        d.pop(k)

async def signup(update, context):
    logging.info(f"Signup from chat_id {update.effective_chat.id}")
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        username = update.message.from_user.username
        date = context.args[0]
        if date not in AVAILABLE_DATES:
            available_str = ", ".join(AVAILABLE_DATES)
            msg = f"дата {date} не доступна, доступные даты: {available_str}"
        else:
            num_participants = int(context.args[1]) if len(context.args) > 1 else 1

            print(context.chat_data)
            _remove_entry(context.chat_data, username, date)
            print(context.chat_data)
            context.chat_data[str(uuid.uuid4())] = {
                "username": username,
                "date": date,
                "num_participants": num_participants,
                "timestamp": time.time()
            }
            msg = f"@{username} записан на {date}, {num_participants} человек"
    await update.message.reply_text(msg)

async def checkme(update, context):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        username = update.message.from_user.username
        user_data = [v for k, v in context.chat_data.items() if v["username"] == username]
        user_data = sorted(user_data, key = lambda x: x["timestamp"])
        msg = "\n".join([f"{row['date']}: {row['num_participants']}" for row in user_data])
        if not msg:
            msg = f"@{username} ни на что не записан"
    await update.message.reply_text(msg)

async def checkdate(update, context):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        date = context.args[0]
        date_data = [v for k, v in context.chat_data.items() if v["date"] == date]
        date_data = sorted(date_data, key = lambda x: x["timestamp"])
        msg = "\n".join([f"@{row['username']}: {row['num_participants']}" for row in date_data])
    await update.message.reply_text(msg)


async def cancel(update, context):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        username = update.message.from_user.username
        date = context.args[0]
        _remove_entry(context.chat_data, username, date)
        msg = f"Бронь @{username} на {date} отменена"
    await update.message.reply_text(msg)
    

if __name__ == '__main__':
    persistence = PicklePersistence(filepath='data')
    # application = ApplicationBuilder().token(TOKEN).build()
    application = ApplicationBuilder().token(TOKEN).persistence(persistence=persistence).build()
    
    application.add_handler(CommandHandler('signup', signup))
    application.add_handler(CommandHandler('checkme', checkme))
    application.add_handler(CommandHandler('checkdate', checkdate))
    application.add_handler(CommandHandler('cancel', cancel))

    application.run_polling()
