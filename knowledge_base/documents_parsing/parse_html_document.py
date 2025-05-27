import asyncio
import os
import time
from pprint import pprint
from typing import Dict, Any, Optional, Union
from urllib.parse import urlparse, urlunparse, urljoin

import aiohttp

from knowledge_base.website_parsing.parsing_config import TEST_MODE
from knowledge_base.website_parsing.website_services.parse import analyze_element, to_markdown
from services.setup_logger import setup_logger

from bs4 import BeautifulSoup, Tag, PageElement, Comment

# Настройка логирования
LOG_FILE = "parse_html_document.log"
LOGS_DIR = os.path.join("logs", "documents_parsing")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


async def fetch_page(url: str) -> dict:
    """
    Асинхронно загружает страницу по заданному URL, ожидает 5 секунд после получения ответа,
    и возвращает словарь с данными о странице.

    :param url: URL страницы для запроса.
    :return: Словарь с ключами "url", "status" и "content".
    """
    logger.info(f"Запрос: {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                text = await response.text()
                await asyncio.sleep(5)
                logger.info(f"Получен ответ для: {url} status={response.status}")

                return {
                    "url": url,
                    "status": response.status,
                    "content": text
                }
    except Exception as e:
        logger.info(f"Ошибка при загрузке страницы ({url}): {e}")
        print(f"[!] Ошибка при загрузке страницы ({url}): {e}")
        return {
            "url": url,
            "status": None,
            "content": str(e)
        }

def clean_soup(soup: Union[BeautifulSoup, PageElement], url: str, parse_config: dict) -> BeautifulSoup:
    """
    Очищает HTML-содержимое от ненужных тегов, классов и элементов.

    :param parse_config:
    :param soup: Объект BeautifulSoup с HTML-контентом.
    :param url: URL страницы, используется для преобразования относительных ссылок.
    :return: Очищенный объект BeautifulSoup.
    """
    excluded_content_config = parse_config.get("excluded_content")
    EXCLUDE_TAGS = excluded_content_config.get("tags")
    EXCLUDE_CLASSES = excluded_content_config.get("classes")
    EXCLUDE_IDS = excluded_content_config.get("ids")
    if EXCLUDE_TAGS:
        # Удаление тегов из EXCLUDE_TAGS (например, скрипты, формы)
        for tag in soup(EXCLUDE_TAGS):
            tag.decompose()

    if EXCLUDE_CLASSES:
        # Удаление элементов с классами из EXCLUDE_CLASSES
        for element in soup.find_all(class_=EXCLUDE_CLASSES):
            element.decompose()

    if EXCLUDE_IDS:
    # Удаление элементов с ID, содержащими ключевые слова из EXCLUDE_IDS
        for el in soup.find_all(attrs={"id": True}):
            if any(k in el['id'].lower() for k in EXCLUDE_IDS):
                el.decompose()

    # Удаление HTML-комментариев
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    return soup

def get_element_by(
    soup: BeautifulSoup,
    _tag: Optional[str] = None,
    _class: Optional[str] = None,
    _id: Optional[str] = None,
    _text_contains: Optional[str] = None
):
    elements = []

    if _tag and _class and _id:
        elements = soup.find_all(name=_tag, class_=_class, id=_id)
    elif _tag and _class:
        elements = soup.find_all(name=_tag, class_=_class)
    elif _tag and _id:
        elements = soup.find_all(name=_tag, id=_id)
    elif _class and _id:
        elements = soup.find_all(class_=_class, id=_id)
    elif _tag:
        elements = soup.find_all(name=_tag)
    elif _class:
        elements = soup.find_all(class_=_class)
    elif _id:
        elements = soup.find_all(id=_id)
    else:
        elements = soup.find_all()

    # Фильтрация по тексту, если указано
    if _text_contains:
        for el in elements:
            if el.get_text(strip=True).find(_text_contains) != -1:
                return el
        return None
    elif elements:
        return elements[0]
    else:
        return None


def parse_result(html: str, parse_config: dict, url: str):
    soup = BeautifulSoup(html, "html.parser")

    next_page_link = None
    # Поиск перехода к следующей странице
    next_page_config = parse_config.get("next_page")
    if next_page_config:
        next_page_tag = next_page_config.get("tag")
        next_page_id = next_page_config.get("id")
        next_page_class = next_page_config.get("class")
        next_page_text_contains = next_page_config.get("text_contains")
        next_page_element = get_element_by(soup=soup,
                                           _tag=next_page_tag,
                                           _class=next_page_class,
                                           _id=next_page_id,
                                           _text_contains=next_page_text_contains
                                           )

        if next_page_element:
            next_page_data_src = next_page_config.get("data-src")
            next_page_link = next_page_element.get(next_page_data_src)

    # Поиск основного контента
    main_content_search_config = parse_config.get("main_content")
    if not main_content_search_config:
        content_element = soup.body
    else:
        main_content_tag = main_content_search_config.get("tag")
        main_content_id = main_content_search_config.get("id")
        main_content_class = main_content_search_config.get("class")

        content_element = get_element_by(soup=soup,
                                         _tag=main_content_tag,
                                         _class=main_content_class,
                                         _id=main_content_id)

    if not content_element:
        logger.warning(f"Не найден основной контент для {url}, PARSE_CONFIG={parse_config}")
        markdown_content = None
    else:
        cleaned_content_element = clean_soup(soup=content_element, url=url, parse_config=parse_config)

        # Преобразование контента в структурированный формат
        html_structure = analyze_element(cleaned_content_element, 0)  # analyze_element рекурсивно разбирает HTML-структуру
        # print(f"{html_structure=}")
        # Преобразование структуры в Markdown
        markdown_content = to_markdown(html_structure)  # to_markdown преобразует структурированный формат в Markdown



    return markdown_content, next_page_link

def url_to_filename(url: str) -> str:
    """
    Преобразует URL в безопасное имя файла, заменяя точки и слэши подчёркиваниями.

    :param url: Строка с полным URL (например, "http://government.ru/docs/all/130013/")
    :return: Строка с безопасным именем файла (например, "government_ru_docs_all_130013")
    """
    parsed = urlparse(url)
    hostname = parsed.hostname.replace('.', '_') if parsed.hostname else ''
    path = parsed.path.strip('/').replace('/', '_')
    return f"{hostname}_{path}"


async def main():
    DOC_URL = "http://government.ru/docs/all/130013/"
    OUTPUT_FILE_NAME= url_to_filename(url=DOC_URL) + ".md"
    BASE_DIR = os.path.dirname(__file__)
    OUTPUT_DIR_NAME = os.path.join(BASE_DIR, "results")
    if not os.path.exists(OUTPUT_DIR_NAME):
        os.makedirs(OUTPUT_DIR_NAME)
    PARSE_CONFIG = {
        "main_content": {
            "class": None,
            "id": "begin",
            "tag": None,
        },
        "excluded_content": {
            "tags": [],
            "classes": ["reader_article_headline", "print_header", "print_footer", "show-more-layer", "show-more"],
            "ids": [],
        },
        "next_page": {
            "class": "show-more",
            "id": None,
            "tag": "a",
            "data-src": "href",
            "text_contains": "Следующая"
        }
    }
    parsed  = urlparse(DOC_URL)
    BASE_URL = urlunparse((parsed.scheme, parsed.netloc, '/', '', '', ''))
    total_markdown = ""
    while DOC_URL:
        result = await fetch_page(DOC_URL)
        if result and result.get("status") == 200:
            markdown, next_link = parse_result(html=result.get("content", ""), parse_config=PARSE_CONFIG, url=DOC_URL)
            total_markdown += markdown
            DOC_URL = urljoin(BASE_URL, next_link) if next_link else None
    if total_markdown:
        output_file = os.path.join(OUTPUT_DIR_NAME, OUTPUT_FILE_NAME)
        with open(output_file, "w" , encoding="utf-8") as f:
            f.write(total_markdown)
            logger.info(f"Текст успешно сохранен в {output_file}")


if __name__ == "__main__":
    start_time = time.monotonic()
    if TEST_MODE:
        print(f"[!] Замер скорости работы")
        asyncio.run(main())
    if TEST_MODE:
        print(f"[!] Замер скорости работы. Время выполнения: {time.monotonic() - start_time} сек")