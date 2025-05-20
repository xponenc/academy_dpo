import asyncio
import os
import time
from pprint import pprint

import aiohttp
import ijson

from kb_services.process_file import merge_chunks_to_output
from kb_services.reports import summarize
from kb_services.parse import parse_sitemap, process_urls_from_file
from parsing_config import TEST_MODE, SITEMAP_DATA_JSON, PARSING_OUTPUT_JSON, TEMP_CHUNKS_DIR, \
    TEST_REQUEST_LENGTH
from services.setup_logger import setup_logger


# Настройка логирования
LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


async def main():
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