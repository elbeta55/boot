import aiohttp
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from bs4 import BeautifulSoup
import asyncio
import logging
import sys

# ... (resto del código anterior)

# Variable global para controlar la ejecución del bot
bot_running = True

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detiene el bot."""
    global bot_running
    await update.message.reply_text('Deteniendo el bot. Esto puede tomar unos segundos...')
    bot_running = False
    await context.bot.close()
    sys.exit(0)

async def periodic_task():
    """Tarea periódica para buscar y notificar nuevos trabajos."""
    while bot_running:
        await notify_new_jobs()
        await asyncio.sleep(300)  # Espera 5 minutos

def main() -> None:
    """Arranca el bot."""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))

    loop = asyncio.get_event_loop()
    loop.create_task(periodic_task())

    application.run_polling(stop_signals=None)

if __name__ == '__main__':
    main()
