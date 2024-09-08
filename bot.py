from telegram import Bot
from telegram.ext import Application
import requests
from bs4 import BeautifulSoup
import asyncio
import datetime

# Token del bot y ID del canal
TOKEN = '7502094787:AAEKk69WI3S6l6ufllz7oxVQqE6Lv5Cv6vo'
CHANNEL_ID = '-1002227999301'  # ID del canal

# Términos de búsqueda y empresas asociadas
SEARCH_TERMS = {
    'spark driver': 'Walmart Inc',
    'doordash': 'Doordash',
}

# Almacenar los últimos resultados y el estado de las búsquedas
last_results = {'spark driver': set(), 'doordash': set()}
search_enabled = {'spark driver': False, 'doordash': False}

async def fetch_jobs(query: str, company: str) -> list:
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

    return jobs

async def search_jobs(query: str, num_searches: int) -> list:
    """Realiza múltiples búsquedas para el término dado y devuelve todos los resultados."""
    all_jobs = set()
    for _ in range(num_searches):
        for term, company in SEARCH_TERMS.items():
            if search_enabled[term]:
                new_jobs = await fetch_jobs(query, company)
                # Agregar trabajos únicos al conjunto
                all_jobs.update(new_jobs)
                await asyncio.sleep(1)  # Pequeña pausa para evitar sobrecarga del servidor
    return list(all_jobs)

async def notify_new_jobs() -> None:
    """Busca y notifica nuevos empleos en los términos especificados cada 5 minutos."""
    global last_results

    all_new_jobs = {'spark driver': [], 'doordash': []}

    for query in SEARCH_TERMS.keys():
        if search_enabled[query]:
            num_searches = 30  # Número de veces que se realiza la búsqueda
            new_jobs = await search_jobs(query, num_searches)

            # Filtrar nuevas publicaciones comparando con los resultados anteriores
            new_posts = [job for job in new_jobs if job not in last_results[query]]
            if new_posts:
                # Actualizar los resultados almacenados
                last_results[query].update(new_jobs)
                all_new_jobs[query].extend(new_posts)

    # Enviar los resultados al canal en partes si es necesario
    bot = Bot(token=TOKEN)

    for term, jobs in all_new_jobs.items():
        if jobs:
            # Divide el mensaje en partes si es demasiado largo
            message = f"Nuevas publicaciones para {term}:\n\n" + "\n\n".join(jobs)
            while len(message) > 4096:  # Telegram message size limit
                await bot.send_message(chat_id=CHANNEL_ID, text=message[:4096])
                message = message[4096:]
            await bot.send_message(chat_id=CHANNEL_ID, text=message)

async def periodic_task() -> None:
    """Ejecuta la tarea periódica cada 5 minutos."""
    while True:
        await notify_new_jobs()
        await asyncio.sleep(300)  # Espera 5 minutos

def main() -> None:
    """Arranca el bot."""
    application = Application.builder().token(TOKEN).build()

    # Ejecutar la tarea periódica
    asyncio.create_task(periodic_task())

    # Ejecutar el bot
    application.run_polling()

if __name__ == '__main__':
    main()
