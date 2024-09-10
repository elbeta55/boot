import httpx
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from bs4 import BeautifulSoup
import asyncio
import logging
import os

# Configuración de logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Token del bot y ID del canal
TOKEN = os.getenv('7531466772:AAEh8GgLMBQFJV1_J1rHfSqa7yYMmVQ9G-I')  # Usar variable de entorno para el token
CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID')

# Términos de búsqueda y empresas asociadas
SEARCH_TERMS = {
    'spark driver walmart': 'Walmart Inc',
    'spark driver': 'Spark Driver',
}

# Número de búsquedas a realizar
NUM_SEARCHES = 50

# Almacenar los últimos resultados
last_results = set()

async def fetch_jobs(query: str, company: str) -> list:
    """Busca empleos relacionados con Spark Driver en la empresa especificada."""
    url = f'https://www.talent.com/jobs?k={query.replace(" ", "+")}&l=Estados+unidos'
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:  # Añadir timeout
            response = await client.get(url)
            response.raise_for_status()  # Levantar error para respuestas no exitosas
            soup = BeautifulSoup(response.text, 'html.parser')
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
    except httpx.RequestError as exc:
        logger.error(f"Request error while fetching jobs: {exc} for URL {url}")
        return []
    except httpx.HTTPStatusError as exc:
        logger.error(f"HTTP error while fetching jobs: {exc} for URL {url}")
        return []
    except Exception as e:
        logger.error(f"Error while fetching jobs: {str(e)} for URL {url}")
        return []

async def search_jobs() -> set:
    """Realiza múltiples búsquedas para todos los términos y devuelve los resultados únicos."""
    all_jobs = set()
    for _ in range(NUM_SEARCHES):
        tasks = [fetch_jobs(query, company) for query, company in SEARCH_TERMS.items()]
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
        last_results = new_jobs
        bot = Bot(token=TOKEN)
        message = f"Nuevas ofertas de trabajo para Spark Driver (Total: {len(new_posts)}):\n\n" + "\n\n".join(new_posts)
        await send_long_message(CHANNEL_ID, message)
    else:
        logger.info("No se encontraron nuevas ofertas de trabajo.")

async def send_long_message(chat_id: str, message: str):
    """Envía un mensaje largo dividiéndolo en partes si es necesario."""
    max_length = 4096
    bot = Bot(token=TOKEN)
    for i in range(0, len(message), max_length):
        await bot.send_message(chat_id=chat_id, text=message[i:i+max_length])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje de bienvenida cuando se usa el comando /start."""
    await update.message.reply_text('¡Hola! Este bot busca automáticamente ofertas de trabajo para Spark Driver en Walmart Inc. y Spark Driver, realizando 50 búsquedas cada 5 minutos.')

async def buscar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Realiza una búsqueda manual cuando se usa el comando /buscar y envía los resultados al usuario."""
    user_id = update.effective_chat.id
    await update.message.reply_text('Buscando empleos, por favor espera...')

    # Realiza la búsqueda
    new_jobs = await search_jobs()
    
    if new_jobs:
        message = f"Nuevas ofertas de trabajo para Spark Driver (Total: {len(new_jobs)}):\n\n" + "\n\n".join(new_jobs)
        await send_long_message(user_id, message)
    else:
        await update.message.reply_text('No se encontraron nuevas ofertas de trabajo en esta búsqueda.')

async def periodic_task():
    """Tarea periódica para buscar y notificar nuevos trabajos."""
    while True:
        await notify_new_jobs()
        await asyncio.sleep(1200)  # Espera 15 minutos

def main() -> None:
    """Arranca el bot."""
    application = Application.builder().token(TOKEN).build()

    # Añadir manejadores de comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("buscar", buscar))  # Añadir el comando /buscar

    # Añadir la tarea periódica al bucle de eventos
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_task())

    application.run_polling()

if __name__ == '__main__':
    main()
