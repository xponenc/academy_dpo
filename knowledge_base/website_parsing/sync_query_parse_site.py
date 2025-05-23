import asyncio
import json
import os
import time
from pprint import pprint
from typing import Dict, Any

import aiohttp
import ijson

from knowledge_base.website_parsing.website_services.parse import extract_article_data, parse_sitemap
from knowledge_base.website_parsing.website_services.process_file import merge_chunks_to_output
from knowledge_base.website_parsing.website_services.reports import summarize
from knowledge_base.website_parsing.parsing_config import TEST_MODE, TEMP_CHUNKS_DIR, TEST_REQUEST_LENGTH, CONCURRENCY_LIMIT, \
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


async def process_urls_from_file(input_file: str,) -> None:
    """
    Обрабатывает URL-адреса из файла и сохраняет результаты в файлы-чанки.

    :param input_file: Файл с URL
    """
    os.makedirs(TEMP_CHUNKS_DIR, exist_ok=True)
    logger.info(f"[✓] Временная директория {TEMP_CHUNKS_DIR} создана.")

    with open(input_file, encoding="utf-8") as f:
        urls = json.load(f)

    buffer = []
    first_entry = True
    index = 0
    chunk_index = 1
    # На случай аварийного перезапуска - проверяются фалы чанков и собираются успешно обработанные ссылки
    processed_urls_set = set()
    if os.path.exists(TEMP_CHUNKS_DIR):
        for filename in os.listdir(TEMP_CHUNKS_DIR):
            if filename.startswith("chunk") and filename.endswith(".json"):
                chunk_path = os.path.join(TEMP_CHUNKS_DIR, filename)
                try:
                    with open(chunk_path, encoding="utf-8") as f:
                        for item in ijson.items(f, "item"):
                            if item.get("status") == 200:
                                processed_urls_set.add(item.get("url"))
                except Exception as e:
                    logger.warning(f"Не удалось прочитать {chunk_path}: {e}")


    to_process = [
                     u for u in urls
                     if not u["processed"] and u["loc"] not in processed_urls_set
                 ][:TEST_REQUEST_LENGTH if TEST_MODE else None] # отфильтровываются успешно обработанные ссылки
    for chunk in [to_process[i:i + CONCURRENCY_LIMIT] for i in range(0, len(to_process), CONCURRENCY_LIMIT)]:
        for i, u in enumerate(chunk):
            result = fetch_page_with_selenium(url=u["loc"], index=index + i)
            result.update({
                "loc": u["loc"],
                "lastmod": u["lastmod"],
                "changefreq": u["changefreq"],
                "priority": u["priority"]
            })
            buffer.append(result)
            u["processed"] = result["status"] == 200

        chunk_filename = os.path.join(TEMP_CHUNKS_DIR, f"chunk_{chunk_index}.json")
        with open(chunk_filename, "w", encoding="utf-8") as f:
            json.dump(buffer, f, ensure_ascii=False, indent=4)
        logger.info(f"Обработка пакета {chunk_index} завершена. Результаты сохранены в {chunk_filename}")

        buffer.clear()
        chunk_index += 1
        index += len(chunk)


async def main(url: str = None):
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        await parse_sitemap(session)
    await process_urls_from_file(SITEMAP_DATA_JSON)
    merge_chunks_to_output(PARSING_OUTPUT_JSON, TEMP_CHUNKS_DIR)
    summary_data = summarize(PARSING_OUTPUT_JSON)
    if TEST_MODE:
        # Вывод примера результатов из файла с потоковым чтением
        logger.info("\n--- Пример результата ---")
        try:
            with open(PARSING_OUTPUT_JSON, mode="r", encoding="utf-8") as f:
                for i, item in enumerate(ijson.items(f, "item")):
                    if i >= TEST_REQUEST_LENGTH:
                        break
                    logger.info(item)
                    pprint(item)
        except Exception as e:
            logger.error(f"Ошибка при потоковом чтении JSON-файла для вывода примеров: {e}")

    logger.info("\n--- Отчёт ---")
    for key, value in summary_data.items():
        logger.info(f"{key}: {value}")

if __name__ == "__main__":
    start_time = time.monotonic()
    if TEST_MODE:
        print(f"[!] Замер скорости работы")
        asyncio.run(main())
    if TEST_MODE:
        print(f"[!] Замер скорости работы. Время выполнения: {time.monotonic() - start_time} сек")