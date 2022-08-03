import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, PicklePersistence, BasePersistence

from typing import Any, Callable, Dict, Optional, Set, Tuple, Type, TypeVar, cast, overload
import pandas as pd
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
    # "alexey": 316821571
}
ALLOWED_CHAT_IDS = ALLOWED_CHATS.values()

AVAILABLE_DATES = [
    "12.8",
    "19.8"
]

COLUMN_TYPES = {"username": str, "date": str, "num_participants": int, "timestamp": float}

class PandasPersistence(BasePersistence):

    def __init__(
        self,
        filepath: FilePathInput,
        store_data: PersistenceInput = None,
        on_flush: bool = False,
        update_interval: float = 60,
    ):
        super().__init__(
            store_data=PersistenceInput(bot_data=False, chat_data=True, user_data=False, callback_data=False),
            update_interval=update_interval
        )
        self.filepath = Path(filepath)
        self.on_flush = on_flush
        self.chat_data: Optional[Dict[int, CD]] = None

    def _dump_data(self, data, filepath):
        for k, v in data.items():
            v["data"].to_csv(os.path.join(filepath, f"{k}.csv"), index=False)

    def _load_data(self, filepath):
        data = {}
        for filename in os.listdir(filepath):
            if filename.endswith(".csv"):
                data[int(filename.split(".")[0])] = {
                    "data": pd.read_csv(
                        os.path.join(filepath, filename),
                        dtype=COLUMN_TYPES)
                }
        return data

    # def _data_equals(data1, data2):
    #     if set(data1.keys()) != set(data2.keys()):
    #         return False
    #     for k in data1.keys():
    #         if not data1[k]["data"].equals(data2[k]["data"]):
    #             return False
    #     return True
    
    def _check_valid_data(self, data):
        return type(data) == dict and "data" in data and type(data["data"]) == pd.DataFrame

    def _data_equals(self, data1, data2):
        if not (self._check_valid_data(data1) and self._check_valid_data(data2)):
            return False
        return data1["data"].equals(data2["data"])

    def get_bot_data(self):
        pass

    def update_bot_data(self):
        pass

    def refresh_bot_data(self):
        pass

    async def get_chat_data(self):
        if self.chat_data:
            pass
        else:
            self.chat_data = self._load_data(os.path.join(self.filepath, "chat_data"))
        logging.info(f"get_chat_data: {self.chat_data}")
        return deepcopy(self.chat_data)

    async def update_chat_data(self, chat_id: int, data) -> None:
        """Will update the chat_data and depending on :attr:`on_flush` save the pickle file.
        Args:
            chat_id (:obj:`int`): The chat the data might have been changed for.
            data (:obj:`dict`): The :attr:`telegram.ext.Application.chat_data` ``[chat_id]``.
        """
        if self.chat_data is None:
            self.chat_data = {}
        if self._data_equals(self.chat_data.get(chat_id), data):
            return
        self.chat_data[chat_id] = data
        if not self.on_flush:
            self._dump_data(self.chat_data, os.path.join(self.filepath, "chat_data"))

    async def refresh_chat_data(self, chat_id: int, chat_data):
        pass

    async def drop_chat_data(self, chat_id: int) -> None:
        """Will delete the specified key from the ``chat_data`` and depending on
        :attr:`on_flush` save the pickle file.
        .. versionadded:: 20.0
        Args:
            chat_id (:obj:`int`): The chat id to delete from the persistence.
        """
        if self.chat_data is None:
            return
        self.chat_data.pop(chat_id, None)  # type: ignore[arg-type]

        if not self.on_flush:
            self._dump_data(self.chat_data, os.path.join(self.filepath, "chat_data"))

    def get_user_data(self):
        pass

    def update_user_data(self):
        pass

    def refresh_user_data(self):
        pass

    def drop_user_data(self):
        pass

    def get_callback_data(self):
        pass

    def update_callback_data(self):
        pass

    def get_conversations(self):
        pass

    def update_conversation(self):
        pass

    async def flush(self):
        if self.chat_data:
            self._dump_data(self.chat_data, os.path.join(self.filepath, "chat_data"))


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
            timestamp = time.time()
            row = {
                "username": username,
                "date": date,
                "num_participants": num_participants,
                "timestamp": timestamp
            }

            if "data" not in context.chat_data:
                context.chat_data["data"] = pd.DataFrame(columns=row.keys())
            df = context.chat_data["data"]
            print(df)
            df = df[(df["username"] != username) | (df["date"] != date)]
            print(df)
            df = pd.concat([df, pd.DataFrame.from_dict([row])])
            context.chat_data["data"] = df
            msg = f"@{username} записан на {date}, {num_participants} человек"
    await update.message.reply_text(msg)

async def checkme(update, context):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        username = update.message.from_user.username
        df = context.chat_data["data"]
        df = df[df["username"] == username]
        df = df.sort_values("timestamp")
        msg = "\n".join([f"{row['date']}: {row['num_participants']}" for _, row in df.iterrows()])
    await update.message.reply_text(msg)

async def checkdate(update, context):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        date = context.args[0]
        df = context.chat_data["data"]
        df = df[df["date"] == date]
        df = df.sort_values("timestamp")
        msg = "\n".join([f"@{row['username']}: {row['num_participants']}" for _, row in df.iterrows()])
    await update.message.reply_text(msg)

async def cancel(update, context):
    if update.effective_chat.id not in ALLOWED_CHAT_IDS:
        msg = "Бот спит"
    else:
        username = update.message.from_user.username
        date = context.args[0]
        df = context.chat_data["data"]
        df = df[(df["username"] != username) | (df["date"] != date)]
        context.chat_data["data"] = df
        msg = f"Бронь @{username} на {date} отменена"
    await update.message.reply_text(msg)
    

if __name__ == '__main__':
    my_persistence = PandasPersistence(filepath='data')
    # application = ApplicationBuilder().token(TOKEN).build()
    application = ApplicationBuilder().token(TOKEN).persistence(persistence=my_persistence).build()
    
    application.add_handler(CommandHandler('signup', signup))
    application.add_handler(CommandHandler('checkme', checkme))
    application.add_handler(CommandHandler('checkdate', checkdate))
    application.add_handler(CommandHandler('cancel', cancel))

    application.run_polling()
