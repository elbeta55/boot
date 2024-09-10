import httpx
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
from bs4 import BeautifulSoup
import asyncio
import logging
import datetime

# Configuración de logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Token del bot y ID del canal
TOKEN = '7531466772:AAEh8GgLMBQFJV1_J1rHfSqa7yYMmVQ9G-I'
CHANNEL_ID = '-1002439937008'  # ID del canal

# Términos de búsqueda y empresas asociadas
SEARCH_TERMS = {
    'spark driver walmart': 'Walmart Inc',
    'spark driver': 'Spark Driver',
}

# Número de búsquedas a realizar
NUM_SEARCHES = 10  # Reducido para eficiencia

# Almacenar los últimos resultados
last_results = set()

async def fetch_time_from_talent() -> str:
    """Obtiene la hora actual desde Talent.com (si está disponible en la página)."""
    url = 'https://www.talent.com'  # URL donde se espera encontrar la hora
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Suponiendo que la hora está en un elemento específico:
                time_element = soup.find('div', class_='current-time')  # Ajusta el selector según corresponda
                if time_element:
                    return time_element.get_text(strip=True)
                else:
                    return 'Hora no encontrada en la página.'
            else:
                return f"Error al acceder a la página: HTTP {response.status_code}"
    except Exception as e:
        return f"Error al obtener la hora: {str(e)}"

async def fetch_jobs(query: str, company: str) -> list:
    """Busca empleos relacionados con Spark Driver en la empresa especificada."""
    url = f'https://www.talent.com/jobs?k={query.replace(" ", "+")}&l=Estados+unidos'
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
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
            else:
                logger.error(f"Error fetching jobs: HTTP {response.status_code} for URL {url}")
                return []
    except Exception as e:
        logger.error(f"Error while fetching jobs: {str(e)} for URL {url}")
        return []

async def search_jobs() -> set:
    """Realiza múltiples búsquedas para todos los términos y devuelve los resultados únicos."""
    all_jobs = set()
    tasks = [fetch_jobs(query, company) for query, company in SEARCH_TERMS.items()]
    results = await asyncio.gather(*tasks)
    for job_list in results:
        all_jobs.update(job_list)
    return all_jobs

async def notify_new_jobs() -> None:
    """Busca y notifica nuevos empleos a la hora exacta de Talent.com."""
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
        try:
            await bot.send_message(chat_id=chat_id, text=message[i:i+max_length])
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")

async def wait_until_talent_hour():
    """Calcula el tiempo restante hasta la próxima hora exacta de Talent.com y espera hasta entonces."""
    current_time_str = await fetch_time_from_talent()
    try:
        current_time = datetime.datetime.strptime(current_time_str, '%Y-%m-%d %H:%M:%S')  # Ajusta el formato si es necesario
    except ValueError:
        logger.error("No se pudo analizar la hora obtenida de Talent.com.")
        return
    
    now = datetime.datetime.now()
    next_hour = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    seconds_until_next_hour = (next_hour - now).total_seconds()
    await asyncio.sleep(seconds_until_next_hour)

async def periodic_task():
    """Tarea periódica para buscar y notificar nuevos trabajos a la hora exacta de Talent.com."""
    while True:
        await notify_new_jobs()
        await wait_until_talent_hour()  # Espera hasta la próxima hora exacta

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje de bienvenida cuando se usa el comando /start."""
    await update.message.reply_text('¡Hola! Este bot busca automáticamente ofertas de trabajo para Spark Driver en Walmart Inc. y Spark Driver, realizando búsquedas periódicas. Usa /time para obtener la hora actual de Talent.com.')

async def time_from_talent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía la hora actual desde Talent.com cuando se usa el comando /time."""
    time_message = await fetch_time_from_talent()
    await update.message.reply_text(time_message)

def main() -> None:
    """Arranca el bot."""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("time", time_from_talent))  # Usa el nuevo comando

    # Añadir la tarea periódica al bucle de eventos
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_task())

    application.run_polling()

if __name__ == '__main__':
    main()
