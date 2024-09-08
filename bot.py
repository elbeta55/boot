from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
import requests
from bs4 import BeautifulSoup
import asyncio

# Token del bot
TOKEN = '7502094787:AAEKk69WI3S6l6ufllz7oxVQqE6Lv5Cv6vo'
CHANNEL_ID = '-1002227999301'  # ID del canal

# Términos de búsqueda y empresas asociadas
SEARCH_TERMS = {
    'spark driver': 'Walmart Inc',
    'doordash': 'Doordash',
}

# Almacenar los últimos resultados
last_results = {}

async def fetch_jobs(query: str, company: str, num_results: int) -> list:
    """Busca empleos relacionados con el término de búsqueda en la empresa especificada."""
    url = f'https://www.talent.com/jobs?k={query.replace(" ", "+")}&l=Estados+unidos'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    jobs = []
    
    # Buscar los elementos de empleo
    for job_element in soup.find_all('div', class_='card__job-c'):
        title = job_element.find('h2', class_='card__job-title')
        company_element = job_element.find('div', class_='card__job-empname-label')
        location = job_element.find('div', class_='card__job-location')
        date_posted = job_element.find('div', class_='c-card__jobDatePosted')

        if title and company_element and location:
            # Filtrar por empresa
            if company.lower() in company_element.get_text(strip=True).lower():
                job_info = (
                    f"**{title.get_text(strip=True)}**\n"
                    f"*{company_element.get_text(strip=True)}*\n"
                    f"Location: {location.get_text(strip=True)}\n"
                    f"Date Posted: {date_posted.get_text(strip=True) if date_posted else 'No date provided'}"
                )
                jobs.append(job_info)
                if len(jobs) >= num_results:
                    break

    return jobs

async def notify_new_jobs() -> None:
    """Busca y notifica nuevos empleos en los términos especificados."""
    global last_results

    all_new_jobs = []

    for query, company in SEARCH_TERMS.items():
        new_jobs = await fetch_jobs(query, company, 20)

        # Filtrar nuevas publicaciones comparando con los resultados anteriores
        new_posts = [job for job in new_jobs if job not in last_results.get(query, [])]
        if new_posts:
            # Actualizar los resultados almacenados
            last_results[query] = new_jobs
            all_new_jobs.extend(new_posts)

    if all_new_jobs:
        message = f"Nuevas publicaciones encontradas:\n\n" + "\n\n".join(all_new_jobs)
        await bot.send_message(chat_id=CHANNEL_ID, text=message)
    else:
        message = "No hay nuevas publicaciones."
        await bot.send_message(chat_id=CHANNEL_ID, text=message)

async def periodic_task() -> None:
    """Ejecuta tareas periódicas para buscar nuevos empleos cada 5 minutos."""
    while True:
        await notify_new_jobs()
        await asyncio.sleep(300)  # Espera 5 minutos antes de buscar nuevamente

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje de bienvenida cuando se usa el comando /start."""
    await update.message.reply_text('¡Hola! Usa /buscar <término> [número] para buscar empleos. Ejemplo: /buscar spark driver 20')

async def buscar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Busca empleos basados en el término dado y número de resultados."""
    args = context.args
    if len(args) < 1:
        await update.message.reply_text('Por favor, proporciona un término de búsqueda. Ejemplo: /buscar spark driver 20')
        return

    query = ' '.join(args[:-1])
    num_results = int(args[-1]) if args[-1].isdigit() else 10

    # Determinar la empresa
    company = None
    for term, comp in SEARCH_TERMS.items():
        if term in query.lower():
            company = comp
            break
    
    if not company:
        await update.message.reply_text('No se encontró una empresa para el término de búsqueda proporcionado.')
        return

    results = await fetch_jobs(query, company, num_results)

    # Enviar los resultados al canal
    bot = Bot(token=TOKEN)
    await bot.send_message(chat_id=CHANNEL_ID, text="\n\n".join(results))

    # Enviar los resultados al usuario privado
    await update.message.reply_text("\n\n".join(results))

def main() -> None:
    """Arranca el bot."""
    global bot
    bot = Bot(token=TOKEN)
    application = Application.builder().token(TOKEN).build()

    # Añadir manejadores de comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("buscar", buscar))

    # Ejecutar la tarea periódica
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_task())

    # Ejecutar el bot
    application.run_polling()

if __name__ == '__main__':
    main()
