import aiohttp
from telegram import Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from bs4 import BeautifulSoup
import asyncio
import logging

# Configuración de logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Token del bot y ID del canal
TOKEN = '7502094787:AAEKk69WI3S6l6ufllz7oxVQqE6Lv5Cv6vo'
CHANNEL_ID = '-1002227999301'  # ID del canal

# Términos de búsqueda y empresas asociadas
SEARCH_TERMS = {
    'spark driver walmart': 'Walmart Inc',
    'spark driver': 'Spark Driver',
}

# Número de búsquedas a realizar
NUM_SEARCHES = 30

# Almacenar los últimos resultados
last_results = set()

async def fetch_jobs(query: str, company: str) -> list:
    """Busca empleos relacionados con Spark Driver en la empresa especificada."""
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
    except Exception as e:
        logger.error(f"Error while fetching jobs: {str(e)} for URL {url}")
        return []

async def search_jobs() -> set:
    """Realiza múltiples búsquedas para todos los términos y devuelve los resultados únicos."""
    all_jobs = set()
    for _ in range(NUM_SEARCHES):
        tasks = []
        for query, company in SEARCH_TERMS.items():
            tasks.append(fetch_jobs(query, company))
        results = await asyncio.gather(*tasks)
        for job_list in results:
            all_jobs.update(job_list)
    return all_jobs

async def notify_new_jobs() -> None:
    """Busca y notifica nuevos empleos cada 5 minutos."""
    global last_results
    new_jobs = await search_jobs()
    new_posts = new_jobs - last_results
    if new_posts:
        last_results = new_jobs  # Actualiza los últimos resultados con todos los trabajos encontrados
        bot = Bot(token=TOKEN)
        message = f"Nuevas ofertas de trabajo para Spark Driver (Total: {len(new_posts)}):\n\n" + "\n\n".join(new_posts)
        await send_long_message(bot, CHANNEL_ID, message)
    else:
        logger.info("No se encontraron nuevas ofertas de trabajo.")

async def send_long_message(bot: Bot, chat_id: str, message: str):
    """Envía un mensaje largo dividiéndolo en partes si es necesario."""
    max_length = 4096
    for i in range(0, len(message), max_length):
        await bot.send_message(chat_id=chat_id, text=message[i:i+max_length])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje de bienvenida cuando se usa el comando /start."""
    await update.message.reply_text('¡Hola! Este bot busca automáticamente ofertas de trabajo para Spark Driver en Walmart Inc. y Spark Driver, realizando 30 búsquedas cada 5 minutos.')

async def periodic_task():
    """Tarea periódica para buscar y notificar nuevos trabajos."""
    while True:
        await notify_new_jobs()
        await asyncio.sleep(300)  # Espera 5 minutos

def main() -> None:
    """Arranca el bot."""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))

    loop = asyncio.get_event_loop()
    loop.create_task(periodic_task())

    application.run_polling()

if __name__ == '__main__':
    main()
