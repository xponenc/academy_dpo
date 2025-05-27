import json
import os
import time
from pprint import pprint
from typing import Dict, Any

from knowledge_base.website_parsing.website_services.parse import extract_article_data
from knowledge_base.website_parsing.parsing_config import TEST_MODE
from services.setup_logger import setup_logger
from services.setup_webderiver import get_driver

from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup, Tag, NavigableString, Comment


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
        time.sleep(2)
        # # Найти элемент с прокручиваемым содержимым
        scrollable_element = driver.find_element(By.CLASS_NAME, "textViewer")

        # Прокручивать вниз, пока высота не перестанет меняться
        last_scroll_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_element)

        while True:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_element)
            time.sleep(1.5)  # подождать, пока прогрузятся данные
            new_scroll_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_element)
            if new_scroll_height == last_scroll_height:
                break
            last_scroll_height = new_scroll_height
        content = scrollable_element
        # first_page = content.find_element(By.CLASS_NAME, "page")
        # print(first_page)
        # # Внутри .page — найти элемент с id="p_1"
        # title = first_page.find_element(By.ID, "p_1")
        # print(title)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find(class_='textViewer')
        for tag in main.select(".block.s_9"):
            tag.decompose()  # полностью удаляет из дерева
        clean_text = soup.get_text()
        print(clean_text)
        with open("garant_query.md", "w", encoding="utf-8") as f:
            f.write(clean_text)
        # article_data = extract_article_data(clean_text, url)  # Вызов функции extract_article_data для обработки контента
        article_data = {}
        logger.info(f"Успешно извлечено {len(article_data['page_content'])} символов контента для URL: {url}")

        return {
            "url": url,
            "status": 200,
            # "title": title,
            "page_categories": article_data["page_categories"],
            # "page_content": article_data["page_content"],
            "page_content": clean_text,
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
    with open("test_query.json", "w", encoding="utf-8") as f:
        json_data = [data]
        json.dump(json_data, f, ensure_ascii=False, indent=4)
    with open("test_query.md", "w", encoding="utf-8") as f:
        f.write(data.get("page_content", ""))

if __name__ == "__main__":
    start_time = time.monotonic()
    TEST_URL = "http://ivo.garant.ru/#/document/70291362/paragraph/1:0"
    if TEST_MODE:
        print(f"[!] Замер скорости работы")
    main(url=TEST_URL)
    if TEST_MODE:
        print(f"[!] Замер скорости работы. Время выполнения: {time.monotonic() - start_time} сек")