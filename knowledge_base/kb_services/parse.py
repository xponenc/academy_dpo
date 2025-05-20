import asyncio
import json
import os
import re
from typing import List, Dict, Any
from urllib.parse import urljoin

import aiohttp
import ijson
from bs4 import BeautifulSoup, Tag, NavigableString, Comment
from selenium.webdriver.common.by import By

import xml.etree.ElementTree as ET

from concurrent.futures import ThreadPoolExecutor


from project.knowledge_base.parsing_config import EXCLUDE_TAGS, EXCLUDE_CLASSES, EXCLUDE_KEYWORDS, STYLE_TAGS, \
    BREADCRUMBS_CLASS, MAIN_URL, SITEMAP_DATA_JSON, SITEMAP_URL, TEMP_CHUNKS_DIR, CONCURRENCY_LIMIT, \
    TEST_REQUEST_LENGTH, TEST_MODE
from services.setup_logger import setup_logger
from services.setup_webderiver import get_driver

# Настройка логирования
LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)

semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)


def clean_soup(soup: BeautifulSoup, url: str) -> BeautifulSoup:
    """
    Очищает HTML-содержимое от ненужных тегов, классов и элементов.

    :param soup: Объект BeautifulSoup с HTML-контентом.
    :param url: URL страницы, используется для преобразования относительных ссылок.
    :return: Очищенный объект BeautifulSoup.
    """
    # Удаление тегов из EXCLUDE_TAGS (например, скрипты, формы)
    for tag in soup(EXCLUDE_TAGS):
        tag.decompose()

    # Удаление элементов с классами из EXCLUDE_CLASSES
    for element in soup.find_all(class_=EXCLUDE_CLASSES):
        element.decompose()

    # Удаление элементов с ID, содержащими ключевые слова из EXCLUDE_KEYWORDS
    for el in soup.find_all(attrs={"id": True}):
        if any(k in el['id'].lower() for k in EXCLUDE_KEYWORDS):
            el.decompose()

    # Удаление HTML-комментариев
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    return soup


def process_images(
        soup: BeautifulSoup,
        url: str,
        data_src_url_name: str = "data-src",
        clear_img: bool = False,
                   ) -> BeautifulSoup:
    """
    Нормализует HTML-содержимое от ненужных тегов, классов и элементов.

    :param data_src_url_name: название параметра у <img> где хранится ссылка на полноразмерного изображение
    :param clear_img: удалять изображение без data_src_url_name
    :param soup: Объект BeautifulSoup с HTML-контентом.
    :param url: URL страницы, используется для преобразования относительных ссылок.
    :return: Очищенный объект BeautifulSoup.
    """
    for img in soup.find_all("img"):
        data_src = img.get(data_src_url_name)
        src = img.get(data_src_url_name)
        if clear_img and not data_src:
            img.decompose()
            continue
        if src and src.startswith("/"):
            src = f"{url}{src}"
            img["src"] = src
        if data_src and data_src.startswith("/"):
            data_src = f"{url}{src}"
            img["data-src"] = data_src

    return soup


def process_http_links(
        soup: BeautifulSoup,
        url: str,
        clear_link_anchor: bool = True,
) -> BeautifulSoup:
    """
    Нормализует ссылки в абсолютные

    :param clear_link_anchor: удалять ссылки на якоря внутри страницы
    :param soup: Объект BeautifulSoup с HTML-контентом.
    :param url: URL страницы, используется для преобразования относительных ссылок.
    :return: Очищенный объект BeautifulSoup.
    """
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#") and clear_link_anchor:
            a.decompose()  # Удаление ссылок, ведущих на внутренние страницы
            continue
        full_url = urljoin(url, href)
        a["href"] = full_url

    return soup


def extract_main_content(soup):
    """
    Извлекает основной контент страницы на основе семантических тегов.

    :param soup: Объект BeautifulSoup с HTML-контентом.
    :return: Тег, содержащий основной контент.
    """
    # candidates = soup.find_all(['article', 'main', 'section', 'div'], recursive=True)
    # main = max(candidates, key=lambda tag: len(tag.find_all(['p', 'h1', 'h2', 'h3', 'ul'])), default=soup.body)
    main = soup.body
    return main


def convert_table_to_markdown(table_tag: Tag) -> List[str]:
    """
    Преобразует HTML-таблицу в формат Markdown.

    :param table_tag: Тег таблицы.
    :return: Список строк с Markdown-представлением таблицы.
    """
    rows = []

    # Извлечение заголовков таблицы
    headers = [th.get_text(strip=True) for th in table_tag.find_all("tr")[0].find_all("th")]
    if headers:
        header_line = "| " + " | ".join(headers) + " |"
        separator_line = "| " + " | ".join(['---'] * len(headers)) + " |"
        rows.append(header_line)
        rows.append(separator_line)

    # Извлечение строк таблицы
    for tr in table_tag.find_all("tr"):
        tds = tr.find_all("td")
        if tds:
            row_line = "| " + " | ".join(td.get_text(strip=True) for td in tds) + " |"
            rows.append(row_line)

    return rows


def parse_line(line):
    """
    Разбирает строку из структурированного вывода на уровень, тег и содержимое.

    :param line: Строка из структурированного вывода.
    :return: Кортеж (уровень, тег, содержимое) или None, если строка не соответствует формату.
    """
    # if re.match(r"^-+|.*|", line):
    if re.match(r"^-+\|.*\|", line):
        level = len(re.match(r"^-+", line).group(0))
        return level, "table_row", line.strip('-').strip()

    # match = re.match(r"(-+)(\[a-zA-Z0-9]+):?\s*(.\*)", line)
    match = re.match(r"(-+)([a-zA-Z0-9]+):?\s*(.*)", line)
    if not match:
        return None
    level = len(match.group(1))
    tag = match.group(2).lower()
    content = match.group(3).strip()
    return level, tag, content


def to_markdown(lines):
    """
    Преобразует структурированный список строк в формат Markdown.

    :param lines: Список строк из структурированного вывода.
    :return: Строка с Markdown-контентом.
    """
    md_lines = []
    ul_stack = []  # Стек для отслеживания уровней списков ul
    li_counters = 0  # Счетчики для нумерации элементов li
    in_table = False  # Флаг для отслеживания нахождения внутри таблицы

    for line in lines:
        parsed = parse_line(line)
        if not parsed:
            continue
        level, tag, content = parsed
        # Отслеживание вложенности списков ul
        if tag == "ul" or tag == "ol":
            ul_stack.append(level)
            continue
        if ul_stack:
            ul_stack = list(filter(lambda item: item < level, ul_stack))
            if not ul_stack:
                li_counters = 0
            # print(f"[!] {ul_stack=} {level=}, {tag=}, {content=} {li_counters=}")

        if tag == "table_row":
            current_ul_level = len(ul_stack)
            indent = " " * 4 * current_ul_level
            if not in_table:
                md_lines.append("")  # Пустая строка перед первой строкой таблицы
                in_table = True
            md_lines.append(f"{indent}{content}")
        else:
            # Сброс флага таблицы при встрече не-строки таблицы
            in_table = False

        if tag == "li":
            # Определение текущего уровня списка ul
            current_ul_level = len(ul_stack)
            # print(ul_stack, level, current_ul_level)
            if current_ul_level == 1:
                # Верхний уровень — нумерованный список
                li_counters += 1
                prefix = f"{li_counters}."
                md_lines.append(f"{prefix} {content}")
            elif level > 1:
                # Вложенный ul — ненумерованный список
                indent = " " * 4 * (current_ul_level - 1)
                md_lines.append(f"{indent}- {content}")
            else:
                md_lines.append(f"- {content}")
                continue
        elif tag == "a":
            match = re.match(r"\[(.+?)\]\((http.*?)\)", content)
            if match:
                md_lines.append(f"[{match.group(1)}]({match.group(2)})")
            else:
                md_lines.append(content)
        elif tag == "img":
            match = re.match(r"!\[(.*?)\]\((.*?)\)", content)
            if match:
                md_lines.append(f"![{match.group(1)}]({match.group(2)})")
            else:
                md_lines.append(f"![Image]({content})")
        # elif tag == "table_row":
        #     current_ul_level = len(ul_stack)
        #     indent = " " * 4 * current_ul_level
        #     md_lines.append("")
        #     md_lines.append(f"{indent}{content}")

        elif tag in {"h1", "h2", "h3", "h4"}:
            hashes = "#" * int(tag[1])
            md_lines.append(f"{hashes} {content}")
        else:
            if content and tag != "table_row":
                md_lines.append(content)

    return "\n".join(md_lines)


def analyze_element(element, level=0, parent_text=None):
    """
    Рекурсивно анализирует элементы HTML и преобразует их в структурированный формат.

    :param element: Текущий элемент HTML.
    :param level: Уровень вложенности для отступов.
    :param parent_text: Список для добавления текста родительского элемента.
    :return: Список строк с описанием элемента.
    """
    output_lines = []

    if isinstance(element, NavigableString):
        return []  # Пропускаем текстовые узлы без тегов

    if element.name and element.name.startswith(':'):
        return []  # Пропускаем псевдоэлементы

    if element.name == "table":
        markdown_rows = convert_table_to_markdown(element)
        indent = '----' * level
        output_lines.append('')  # Разделитель перед таблицей для читаемости
        output_lines.extend([f"{indent}{row}" for row in markdown_rows])
        output_lines.append('')  # Разделитель после таблицы
        return output_lines

    is_style_tag = element.name in STYLE_TAGS

    has_non_style_tags = any(
        isinstance(child, Tag) and not child.name.startswith(':') and child.name not in STYLE_TAGS
        for child in element.children
    )
    # Проверяем, есть ли среди детей теги, которые не являются стилистическими

    direct_text = ' '.join(
        str(child).strip() for child in element.children if isinstance(child, NavigableString)
    ).strip()
    # Собираем текст из текстовых узлов, удаляя лишние пробелы

    child_texts = []

    for child in element.children:
        if isinstance(child, Tag):
            child_result = analyze_element(child, level + (0 if is_style_tag else 1), child_texts)
            output_lines.extend(child_result)

    combined_text = ' '.join(filter(None, [direct_text] + child_texts)).strip()
    # Объединяем текст из детей и текущего элемента, фильтруя пустые строки

    if not is_style_tag:
        indent = '----' * level

        if element.name == "a":
            href = element.get("href", "").strip()
            link_text = combined_text if combined_text else href
            md_link = f"[{link_text}]({href})"
            if parent_text is not None:
                parent_text.append(md_link)
            else:
                output_lines.insert(0, f"{indent}{md_link}")

        elif element.name == "img":
            src = element.get("data-src") or element.get("src", "")
            alt = element.get("alt", "").strip()
            md_image = f"![{alt}]({src})"
            if parent_text is not None:
                parent_text.append(md_image)
            else:
                output_lines.insert(0, f"{indent}{md_image}")

        elif not has_non_style_tags:
            if combined_text:
                output_lines.insert(0, f"{indent}{element.name}: {combined_text}")
            else:
                output_lines.insert(0, f"{indent}{element.name}")

        elif combined_text:
            output_lines.insert(0, f"{indent}{element.name}: {combined_text}")
        else:
            output_lines.insert(0, f"{indent}{element.name}")

    elif combined_text and parent_text is not None:
        parent_text.append(combined_text)

    return output_lines


def extract_article_data(html: str, url: str) -> Dict[str, Any]:
    """
    Извлекает данные статьи: категории, контент в markdown и изображения.

    :param html: HTML-содержимое страницы, которое будет парситься.
    :param url: URL страницы, используется для контекста и возможных преобразований ссылок.
    :return: Словарь с категориями, контентом в формате Markdown и изображениями.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Извлечение категорий из breadcrumbs
    breadcrumb_class, breadcrumb_tag = BREADCRUMBS_CLASS  # BREADCRUMBS_CLASS — это кортеж ("breadcrumbs", "span")
    breadcrumbs = soup.find(class_=breadcrumb_class)
    page_categories = []
    if breadcrumbs:
        breadcrumb_tags = breadcrumbs.find_all(name=breadcrumb_tag)
        for tag in breadcrumb_tags:
            text = tag.get_text(strip=True)  # Извлекаем текст из каждого тега breadcrumbs
            if text and text not in page_categories:  # Проверяем, чтобы текст был непустым и уникальным
                page_categories.append(text)
    # print(f"{page_categories=}")
    # Поиск основного контента
    content_element = extract_main_content(soup)  # extract_main_content выбирает тег с наибольшим количеством контента
    # print(f"{content_element=}")
    cleaned_content_element = clean_soup(soup=content_element,url=url)  # clean_soup очищает HTML от ненужных элементов
    cleaned_content_element = process_images(soup=cleaned_content_element, url=MAIN_URL, clear_img=True)  # clean_soup очищает HTML от ненужных изображений
    cleaned_content_element = process_http_links(soup=cleaned_content_element, url=url, clear_link_anchor=False)  # clean_soup очищает HTML от ненужных ссылок
    # print(f"{cleaned_content_element=}")

    # Преобразование контента в структурированный формат
    html_structure = analyze_element(cleaned_content_element, 0)  # analyze_element рекурсивно разбирает HTML-структуру
    # print(f"{html_structure=}")
    # Преобразование структуры в Markdown
    markdown_content = to_markdown(html_structure)  # to_markdown преобразует структурированный формат в Markdown
    # pprint(f"{markdown_content=}")

    # Извлечение изображений
    page_images = []
    for img in cleaned_content_element.find_all("img"):
        src = img.get("src") or img.get("data-src")  # Получаем src или data-src изображения
        alt = img.get("alt", "")  # Получаем alt-текст изображения
        if src:
            page_images.append((alt, src))  # Добавляем изображение как кортеж (alt, src)

    # logger.info(f"Успешно извлечено {len(markdown_content)} символов контента для URL: {url}")
    return {
        "page_categories": page_categories,
        "page_content": markdown_content.strip(),
        "page_images": page_images
    }



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