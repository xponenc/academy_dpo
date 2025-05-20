import asyncio
import json
import os
import time
from pprint import pprint
from typing import Dict, Any

import aiohttp
import ijson

from kb_services.parse import extract_article_data, parse_sitemap
from kb_services.process_file import merge_chunks_to_output
from kb_services.reports import summarize
from project.knowledge_base.parsing_config import TEST_MODE, TEMP_CHUNKS_DIR, TEST_REQUEST_LENGTH, CONCURRENCY_LIMIT, \
    SITEMAP_DATA_JSON, PARSING_OUTPUT_JSON
from services.setup_logger import setup_logger
from services.setup_webderiver import get_driver

from selenium.webdriver.common.by import By

# Настройка логирования
LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


def fetch_page_with_selenium(url: str, index: int) -> Dict[str, Any]:
    """
    Загружает страницу с использованием Selenium и извлекает данные.

    :param url: URL страницы для загрузки.
    :param index: Индекс страницы в списке, используется для логирования.
    :return: Словарь с результатами парсинга или ошибкой.
    """
    logger.info(f"Парсинг страницы #{index + 1}: {url}")
    driver = get_driver()  # get_driver — функция для инициализации Selenium-драйвера

    try:
        driver.get(url)  # Загрузка страницы
        driver.execute_script("return document.readyState")  # Ожидание полной загрузки страницы
        title_elements = driver.find_elements(By.TAG_NAME, "h1")  # Поиск заголовка h1
        title = title_elements[0].text.strip() if title_elements else None  # Извлечение текста заголовка
        html = driver.page_source  # Получение HTML-контента страницы
        article_data = extract_article_data(html, url)  # Вызов функции extract_article_data для обработки контента
        logger.info(f"Успешно извлечено {len(article_data['page_content'])} символов контента для URL: {url}")

        return {
            "url": url,
            "status": 200,
            "title": title,
            "page_categories": article_data["page_categories"],
            "page_content": article_data["page_content"],
            "page_images": article_data["page_images"]
        }
    except Exception as e:
        # Обработка ошибок при загрузке страницы
        logger.error(f"Ошибка при загрузке страницы #{index + 1} ({url}): {e}")
        return {
            "url": url,
            "status": None,
            "title": None,
            "page_categories": [],
            "page_content": str(e),  # Текст ошибки сохраняется в page_content
            "page_images": []
        }
    finally:
        driver.quit()  # Закрытие Selenium-драйвера


def main(url: str):
    data = fetch_page_with_selenium(url=url, index=0)
    pprint(data)

if __name__ == "__main__":
    start_time = time.monotonic()
    TEST_URL = "https://academydpo.org/napravleniya"
    if TEST_MODE:
        print(f"[!] Замер скорости работы")
        main(url=TEST_URL)
    if TEST_MODE:
        print(f"[!] Замер скорости работы. Время выполнения: {time.monotonic() - start_time} сек")