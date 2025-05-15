import asyncio
import re
import shutil
import sys
import time
from pprint import pprint

import aiohttp

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup, Comment
import xml.etree.ElementTree as ET
import json
import ijson
import os
import tempfile
from datetime import datetime
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from services.setup_logger import setup_logger
from services.setup_webderiver import get_driver

# Тестовый режим запрос на TEST_REQUEST_LENGTH ссылок
TEST_MODE = True
TEST_REQUEST_LENGTH = 20


SITEMAP_URL = "https://academydpo.org/sitemap.xml"
FILE_PREFIX = SITEMAP_URL.split('/')[2].split(".")[0]

PARENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARSING_OUTPUT_JSON = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_parsed_site.json")
SITEMAP_DATA_JSON = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_sitemap_data.json")
# TEMP_CHUNKS_DIR директория временного хранения файлов-чанков с результатами парсинга
TEMP_CHUNKS_DIR = os.path.join(PARENT_DIR, "chunks", FILE_PREFIX)

# Настройка парсинга Beautiful Soap
CLASSES_OF_BASIC_SEMANTIC_ELEMENTS = (("article", "category"), (None, "main__content"), (None, "main"))
EXCLUDE_TAGS = ("form", "style", "script", "svg")
EXCLUDE_CLASSES = ("sw-review-item", "some-other-class")
BREADCRUMBS_CLASS = ("breadcrumbs", "span")

CONCURRENCY_LIMIT = 10 # количество одновременных запросов к страницам
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


def html_to_markdown(tag) -> str:
    """
    Конвертирует HTML-содержимое в markdown.

    :param tag: HTML тег (элемент BeautifulSoup)
    :return: Строка в формате markdown
    """
    lines = []
    for element in tag.descendants:
        if element.name in ["h1", "h2", "h3"]:
            level = int(element.name[1])
            lines.append(f"{'#' * level} {element.get_text(strip=True)}")
        elif element.name == "p":
            lines.append(element.get_text(strip=True))
        elif element.name == "ul":
            for li in element.find_all("li"):
                lines.append(f"- {li.get_text(strip=True)}")
        elif element.name == "ol":
            for i, li in enumerate(element.find_all("li"), 1):
                lines.append(f"{i}. {li.get_text(strip=True)}")
        elif element.name == "a":
            href = element.get("href")
            text = element.get_text(strip=True)
            if href:
                lines.append(f"[{text}]({href})")
        elif element.name == "strong":
            lines.append(f"**{element.get_text(strip=True)}**")
    return "\n".join(lines)


def extract_article_data(html: str) -> Dict[str, Any]:
    """
    Извлекает данные статьи: категории, контент в markdown и изображения.

    :param html: HTML-содержимое страницы
    :return: Словарь с категориями, контентом и изображениями
    """
    soup = BeautifulSoup(html, "html.parser")

    breadcrumb_class, breadcrumb_tag = BREADCRUMBS_CLASS
    breadcrumbs = soup.find(class_=breadcrumb_class)
    page_categories = []
    if breadcrumbs:
        breadcrumb_tags = breadcrumbs.find_all(name=breadcrumb_tag)
        for tag in breadcrumb_tags:
            if tag.get_text() in page_categories:
                continue
            page_categories.append(tag.get_text())

    content_element = None

    for tag, _class in CLASSES_OF_BASIC_SEMANTIC_ELEMENTS:
        if tag and _class:
            content_element = soup.find(name=tag, class_=_class)
        elif tag:
            content_element = soup.find(name=tag)
        elif _class:
            content_element = soup.find(class_=_class)
        if content_element:
            break

    if not content_element:
        return {"page_categories": page_categories, "page_content": "", "page_images": []}

    for tag in content_element(EXCLUDE_TAGS):
        tag.decompose()
    for element in content_element.find_all(class_=EXCLUDE_CLASSES):
        element.decompose()
    for comment in content_element.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    page_images = []
    for img in content_element.find_all("img"):
        src = img.get("data-src")
        alt = img.get("alt")
        if not src:
            continue
        if src.startswith("/"):
            src = f"https://academydpo.org{src}"
        page_images.append((alt, src))

    markdown_content = html_to_markdown(content_element)
    return {
        "page_categories": page_categories,
        "page_content": markdown_content.strip(),
        "page_images": page_images
    }


def fetch_page_with_selenium(url: str, index: int) -> Dict[str, Any]:
    """
    Загружает страницу и извлекает данные.

    :param url: URL страницы
    :param index: Индекс URL в списке
    :return: Словарь с результатами парсинга
    """
    logger.info(f"Парсинг страницы #{index + 1}: {url}")
    driver = get_driver()
    try:
        driver.get(url)
        # WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "h1")))
        driver.execute_script("return document.readyState")  # Ждать 'complete'
        title_elements = driver.find_elements(By.TAG_NAME, "h1")
        title = title_elements[0].text.strip() if title_elements else None
        html = driver.page_source
        article_data = extract_article_data(html)
        return {
            "url": url,
            "status": 200,
            "title": title,
            "page_categories": article_data["page_categories"],
            "page_content": article_data["page_content"],
            "page_images": list(set(article_data["page_images"]))
        }
    except Exception as e:
        logger.error(f"Ошибка при загрузке страницы #{index + 1} ({url}): {e}")
        return {
            "url": url,
            "status": None,
            "title": None,
            "page_categories": tuple(),
            "page_content": str(e),
            "page_images": []
        }
    finally:
        driver.quit()


async def async_fetch_page_with_selenium(url: str, index: int, executor: ThreadPoolExecutor) -> Dict[str, Any]:
    """
    Асинхронно запускает fetch_page_with_selenium.

    :param url: URL страницы
    :param index: Индекс в списке
    :param executor: Объект ThreadPoolExecutor
    :return: Результат обработки страницы
    """
    async with semaphore:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, fetch_page_with_selenium, url, index)


async def parse_sitemap(session: aiohttp.ClientSession) -> None:
    """
    Проверяет наличие файла SITEMAP_DATA_JSON и верифицирует структуру
    Если файла нет или поврежден, то
    Загружает sitemap и сохраняет в файл SITEMAP_DATA_JSON.

    :param session: Сессия aiohttp
    """

    # Если файл уже есть — проверим его структуру
    if os.path.exists(SITEMAP_DATA_JSON):
        try:
            with open(SITEMAP_DATA_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and all("loc" in item for item in data):
                logger.info(f"[✓] Используется существующий файл ссылок: {SITEMAP_DATA_JSON}")
                return
            else:
                logger.warning(f"[!] Структура файла {SITEMAP_DATA_JSON} повреждена. Перезагружаем sitemap.")
        except Exception as e:
            logger.warning(f"[!] Ошибка при чтении {SITEMAP_DATA_JSON}: {e}. Перезагружаем sitemap.")


    logger.info(f"Загружаем sitemap: {SITEMAP_URL}")
    try:
        async with session.get(SITEMAP_URL) as response:
            text = await response.text()
            root = ET.fromstring(text)
            namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            urls = []
            for url in root.findall("ns:url", namespace):
                loc = url.find("ns:loc", namespace).text
                lastmod = url.find("ns:lastmod", namespace)
                changefreq = url.find("ns:changefreq", namespace)
                priority = url.find("ns:priority", namespace)
                urls.append({
                    "loc": loc,
                    "lastmod": lastmod.text if lastmod is not None else None,
                    "changefreq": changefreq.text if changefreq is not None else None,
                    "priority": priority.text if priority is not None else None,
                    "processed": False
                })
            with open(SITEMAP_DATA_JSON, "w", encoding="utf-8") as f:
                json.dump(urls, f, ensure_ascii=False, indent=4)
            logger.info(f"Сохранено {len(urls)} ссылок в {SITEMAP_DATA_JSON}")
    except Exception as e:
        logger.error(f"Ошибка при парсинге sitemap: {e}")


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

    with ThreadPoolExecutor(max_workers=CONCURRENCY_LIMIT) as executor:
        # to_process = [u for u in urls if not u["processed"]][:TEST_REQUEST_LENGTH if TEST_MODE else None]
        to_process = [
                         u for u in urls
                         if not u["processed"] and u["loc"] not in processed_urls_set
                     ][:TEST_REQUEST_LENGTH if TEST_MODE else None] # отфильтровываются успешно обработанные ссылки
        for chunk in [to_process[i:i + CONCURRENCY_LIMIT] for i in range(0, len(to_process), CONCURRENCY_LIMIT)]:
            tasks = [async_fetch_page_with_selenium(u["loc"], index + i, executor) for i, u in enumerate(chunk)]
            results = await asyncio.gather(*tasks)
            for u, result in zip(chunk, results):
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


def summarize(filename: str) -> Dict[str, Any]:
    """
    Формирует статистику по результатам проверки URL, используя потоковое чтение JSON.
    """
    total = 0
    available = 0
    dates = []
    try:
        with open(filename, mode="r", encoding="utf-8") as f:
            # Потоковое чтение JSON с помощью ijson
            for item in ijson.items(f, "item"):
                total += 1
                if item["status"] == 200:
                    available += 1
                if item["lastmod"]:
                    try:
                        dates.append(datetime.fromisoformat(item["lastmod"]))
                    except ValueError:
                        pass
    except Exception as e:
        logger.error(f"Ошибка при потоковом чтении JSON-файла для статистики: {e}")

    unavailable = total - available
    date_range = (min(dates).date(), max(dates).date()) if dates else (None, None)
    return {
        "Всего ссылок": total,
        "Доступных (200)": available,
        "Недоступных": unavailable,
        "Диапазон дат обновлений": date_range
    }


def merge_chunks_to_output(output_file: str, chunks_dir: str):
    all_data = []
    for filename in sorted(os.listdir(chunks_dir)):
        if filename.startswith("chunk") and filename.endswith(".json"):
            chunk_path = os.path.join(chunks_dir, filename)
            try:
                with open(chunk_path, encoding="utf-8") as f:
                    data = json.load(f)
                    all_data.extend(data)
            except Exception as e:
                logger.warning(f"Ошибка чтения {chunk_path}: {e}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)
    logger.info(f"[✓] Все чанки объединены в {output_file}")

    # Удаляем чанки
    for filename in os.listdir(chunks_dir):
        if filename.startswith("chunk") and filename.endswith(".json"):
            os.remove(os.path.join(chunks_dir, filename))

    logger.info(f"[✓] Все временные чанки удалены")

    if os.path.exists(TEMP_CHUNKS_DIR):
        try:
            shutil.rmtree(TEMP_CHUNKS_DIR)
            logger.info(f"[✓] Временная директория {TEMP_CHUNKS_DIR} удалена.")
        except Exception as e:
            logger.error(f"Ошибка при удалении директории {TEMP_CHUNKS_DIR}: {e}")


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
