import asyncio
import os
import time
from pprint import pprint
from typing import Dict, Any

import aiohttp

from knowledge_base.website_parsing.parsing_config import TEST_MODE
from services.setup_logger import setup_logger

from bs4 import BeautifulSoup, Tag, NavigableString, Comment


async def fetch_page(url: str) -> dict:
    """
    Асинхронно загружает страницу по заданному URL, ожидает 5 секунд после получения ответа,
    и возвращает словарь с данными о странице.

    :param url: URL страницы для запроса.
    :return: Словарь с ключами "url", "status" и "content".
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                text = await response.text()
                await asyncio.sleep(5)
                return {
                    "url": url,
                    "status": response.status,
                    "content": text
                }
    except Exception as e:
        print(f"[!] Ошибка при загрузке страницы ({url}): {e}")
        return {
            "url": url,
            "status": None,
            "content": str(e)
        }

def parse_result(html: str, parse_config: dict):
    soup = BeautifulSoup(html, "html.parser")
    # Поиск основного контента
    content_element = None
    main_content_search_config = parse_config.get("main_content")
    if not main_content_search_config:
        content_element = soup.body
    else:
        main_content_tag = main_content_search_config.get("tag")
        main_content_id = main_content_search_config.get("tag")
        main_content_class = main_content_search_config.get("tag")

        # Попытка точного поиска
        if main_content_tag and main_content_class and main_content_id:
            content_element = soup.find(name=main_content_tag, class_=main_content_class, id=main_content_id)
        elif main_content_tag and main_content_class:
            content_element = soup.find(name=main_content_tag, class_=main_content_class)
        elif main_content_tag and main_content_id:
            content_element = soup.find(name=main_content_tag, id=main_content_id)
        elif main_content_class and main_content_id:
            content_element = soup.find(class_=main_content_class, id=main_content_id)
        elif main_content_tag:
            content_element = soup.find(name=main_content_tag)
        elif main_content_class:
            content_element = soup.find(class_=main_content_class)
        elif main_content_id:
            content_element = soup.find(id=main_content_id)

# Пример использования:
async def main():
    DOC_URL = "http://government.ru/docs/all/130013/"
    PARSE_CONFIG = {
        "main_content": {
            "class": None,
            "id": "begin",
            "tag": None,
        }
    }
    result = await fetch_page(DOC_URL)
    if result and result.get("status") == 200:
        parse_result(html=result.get("content", ""))

if __name__ == "__main__":
    start_time = time.monotonic()
    if TEST_MODE:
        print(f"[!] Замер скорости работы")
        asyncio.run(main())
    if TEST_MODE:
        print(f"[!] Замер скорости работы. Время выполнения: {time.monotonic() - start_time} сек")