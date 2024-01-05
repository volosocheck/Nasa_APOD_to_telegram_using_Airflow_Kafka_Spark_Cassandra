import asyncio
from telegram import Bot
from telegram.error import TelegramError
from cassandra.cluster import Cluster
from datetime import datetime


async def async_send_daily_picture():
    bot = Bot(token='YOUR TOKEN')
    chat_id = "YOUR CHAT ID"

    cluster = Cluster(['cassandra'])
    session = cluster.connect('nasa')

    today_date = datetime.now().strftime("%Y-%m-%d")
    query = f"SELECT * FROM nasa.nasa_apod WHERE date = '{today_date}'"

    row = session.execute(query).one()
    if row:
        message = f"<a href='{row.hdurl}'>{row.title}</a>\n\n{row.explanation}"
        await bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
    else:
        print("No picture available for today.")


def send_daily_picture_func():
    asyncio.run(async_send_daily_picture())