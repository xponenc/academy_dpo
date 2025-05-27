import asyncio
import os
import time
from datetime import datetime
from pprint import pprint

import aiohttp
import ijson

from knowledge_base.website_parsing.website_services.process_file import merge_chunks_to_output
from knowledge_base.website_parsing.website_services.reports import summarize
from knowledge_base.website_parsing.website_services.parse import parse_sitemap, process_urls_from_file
from parsing_config import TEST_MODE, SITEMAP_DATA_JSON, PARSING_OUTPUT_JSON, TEMP_CHUNKS_DIR, \
    TEST_REQUEST_LENGTH, PARSING_OUTPUT_DIR
from services.setup_logger import setup_logger


# Настройка логирования
LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


async def main():
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        await parse_sitemap(session)
    await process_urls_from_file(SITEMAP_DATA_JSON)

    if not os.path.exists(PARSING_OUTPUT_DIR):
        os.makedirs(PARSING_OUTPUT_DIR)
    output_file = os.path.join(PARSING_OUTPUT_DIR,
                               f"{PARSING_OUTPUT_JSON}_{datetime.now().strftime('%Y_%m_%d_%H-%M')}.json")

    merge_chunks_to_output(output_file, TEMP_CHUNKS_DIR)
    summary_data = summarize(output_file)

    if TEST_MODE:
        # Вывод примера результатов из файла с потоковым чтением
        logger.info("\n--- Пример результата ---")
        try:
            with open(output_file, mode="r", encoding="utf-8") as f:
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