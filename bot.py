import requests
import aiohttp
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from bs4 import BeautifulSoup
import asyncio
import datetime
import logging

# Configuración de logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Token del bot y ID del canal
TOKEN = '7502094787:AAEKk69WI3S6l6ufllz7oxVQqE6Lv5Cv6vo'
CHANNEL_ID = '-1002227999301'  # ID del canal

# Términos de búsqueda y empresas asociadas
SEARCH_TERMS = {
    'spark driver': 'Walmart Inc',
    'doordash': 'Doordash',
}

# Almacenar los últimos resultados y el estado de las búsquedas
last_results = {term: set() for term in SEARCH_TERMS}
search_enabled = {term: True for term in SEARCH_TERMS}

async def fetch_jobs(query: str, company: str) -> list:
    """Busca empleos relacionados con el término de búsqueda en la empresa especificada."""
    url = f'https://www.talent.com/jobs?k={query.replace(" ", "+")}&l=Estados+unidos'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    jobs = []
                    for job_element in soup.find_all('div', class_='card__job-c'):
                        title = job_element.find('h2', class_='card__job-title')
                        company_element = job_element.find('div', class_='card__job-empname-label')
                        location = job_element.find('div', class_='card__job-location')
                        date_posted = job_element.find('div', class_='c-card__jobDatePosted')
                        
                        if title and company_element and location and company.lower() in company_element.get_text(strip=True).lower():
                            job_info = (
                                f"**{title.get_text(strip=True)}**\n"
                                f"*{company_element.get_text(strip=True)}*\n"
                                f"Location: {location.get_text(strip=True)}\n"
                                f"Date Posted: {date_posted.get_text(strip=True) if date_posted else 'No date provided'}"
                            )
                            jobs.append(job_info)
                    return jobs
                else:
                    logger.error(f"Error fetching jobs: HTTP {response.status} for URL {url}")
                    return []
    except aiohttp.ClientError as e:
        logger.error(f"Network error while fetching jobs: {str(e)} for URL {url}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error while fetching jobs: {str(e)} for URL {url}")
        return []

async def search_jobs(query: str, num_searches: int) -> list:
    """Realiza múltiples búsquedas para el término dado y devuelve todos los resultados."""
    all_jobs = set()
    tasks = []
    for _ in range(num_searches):
        for term, company in SEARCH_TERMS.items():
            if search_enabled[term]:
                tasks.append(fetch_jobs(query, company))
    results = await asyncio.gather(*tasks)
    for job_list in results:
        all_jobs.update(job_list)
    return list(all_jobs)

async def notify_new_jobs() -> None:
    """Busca y notifica nuevos empleos en los términos especificados cada 5 minutos."""
    global last_results
    all_new_jobs = {term: [] for term in SEARCH_TERMS}

    for query in SEARCH_TERMS.keys():
        if search_enabled[query]:
            num_searches = 30  # Número de veces que se realiza la búsqueda
            new_jobs = await search_jobs(query, num_searches)
            new_posts = [job for job in new_jobs if job not in last_results[query]]
            if new_posts:
                last_results[query].update(new_jobs)
                all_new_jobs[query].extend(new_posts)

    bot = Bot(token=TOKEN)
    for term, jobs in all_new_jobs.items():
        if jobs:
            message = f"Nuevas publicaciones para {term}:\n\n" + "\n\n".join(jobs)
            await send_long_message(bot, CHANNEL_ID, message)

async def send_long_message(bot: Bot, chat_id: str, message: str):
    """Envía un mensaje largo dividiéndolo en partes si es necesario."""
    max_length = 4096
    for i in range(0, len(message), max_length):
        await bot.send_message(chat_id=chat_id, text=message[i:i+max_length])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje de bienvenida cuando se usa el comando /start."""
    await update.message.reply_text('¡Hola! Usa /on <empresa> para encender la búsqueda, /off <empresa> para apagarla, y /1 <término> [número] para buscar empleos.')

async def toggle_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Activa o desactiva las búsquedas para spark driver y doordash."""
    if len(context.args) < 1:
        await update.message.reply_text('Uso: /on <empresa> o /off <empresa>. Ejemplo: /on spark')
        return

    company = ' '.join(context.args).lower()
    matched_company = next((term for term in SEARCH_TERMS if term.startswith(company)), None)

    if not matched_company:
        await update.message.reply_text('Empresa desconocida. Usa "spark driver" o "doordash".')
        return

    if context.command[0] == 'on':
        search_enabled[matched_company] = True
        await update.message.reply_text(f'Búsqueda para {matched_company} encendida con éxito.')
        await notify_new_jobs()
    elif context.command[0] == 'off':
        search_enabled[matched_company] = False
        await update.message.reply_text(f'Búsqueda para {matched_company} apagada con éxito.')

async def perform_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Realiza búsquedas basadas en el término dado y número de resultados."""
    if len(context.args) < 1:
        await update.message.reply_text('Por favor, proporciona un término de búsqueda. Ejemplo: /1 spark driver 20')
        return

    query = ' '.join(context.args[:-1])
    num_searches = int(context.args[-1]) if context.args[-1].isdigit() else 1

    results = await search_jobs(query, num_searches)
    await send_long_message(context.bot, update.effective_chat.id, "\n\n".join(results))

async def periodic_task():
    """Tarea periódica para buscar y notificar nuevos trabajos."""
    while True:
        await notify_new_jobs()
        await asyncio.sleep(300)  # Espera 5 minutos

def main() -> None:
    """Arranca el bot."""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("on", toggle_search))
    application.add_handler(CommandHandler("off", toggle_search))
    application.add_handler(CommandHandler("1", perform_search))

    loop = asyncio.get_event_loop()
    loop.create_task(periodic_task())

    application.run_polling()

if __name__ == '__main__':
    main()
