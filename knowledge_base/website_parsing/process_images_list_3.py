import os
import json
import logging
from logging.handlers import RotatingFileHandler

import requests
import ijson
import numpy as np
import cv2
from PIL import Image
from io import BytesIO
from urllib.parse import urlparse

from knowledge_base.async_selenium_parse_site_5 import FILE_PREFIX
from del_services.setup_logger import setup_logger

# === Конфигурация путей ===
PARENT_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_FILE = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_parsed_site.json")
PARSING_OUTPUT_W_READY_IMAGES_JSON = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_sitemap_data_processed_images.json")
IMAGE_SAVE_ROOT = os.path.join(PARENT_DIR, FILE_PREFIX, "site_images")

LOGS_DIR = os.path.join("logs", "knowledge_base")
LOG_FILE = "process_images.log"
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


# === Кэш проверенных изображений ===
checked_images = {}


def save_image_from_url(url: str, content: bytes) -> str:
    """
    Сохраняет изображение в локальную директорию, соответствующую URL-пути.

    :param url: URL изображения
    :param content: Содержимое изображения в байтах
    :return: Абсолютный путь до сохранённого изображения
    """
    parsed = urlparse(url)
    path = parsed.path.lstrip("/")
    full_path = os.path.join(IMAGE_SAVE_ROOT, path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    with open(full_path, "wb") as f:
        f.write(content)

    logger.info(f"Сохранено изображение: {full_path}")
    return full_path


def process_items():
    """
    Основная функция обработки страниц:
    - Стримит JSON-файл по элементам
    - Показывает изображения пользователю
    - Получает флаг 'y' (recognize) или 'n' (skip)
    - Кэширует результат, сохраняет подходящие изображения
    - Пишет корректный JSON-массив в выходной файл
    """
    with open(INPUT_FILE, "r", encoding="utf-8") as f_in, open(PARSING_OUTPUT_W_READY_IMAGES_JSON, "w", encoding="utf-8") as f_out:
        f_out.write("[\n")
        first = True

        for page in ijson.items(f_in, "item"):
            processed_page_images = []
            page_images = page.get("page_images")

            if page_images:
                for alt, url in page_images:
                    logger.info(f"Обработка изображения: {url}")

                    # Проверка кэша
                    if url in checked_images and checked_images[url] != "error":
                        result = checked_images[url]
                        logger.info(f"Повтор: {url} уже обработано как '{result}'")
                        processed_page_images.append((alt, url, result))
                        continue

                    try:
                        headers = {
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/122.0.0.0 Safari/537.36"
                            )
                        }
                        response = requests.get(url, headers=headers, timeout=10)
                        response.raise_for_status()
                        content = response.content
                        img = Image.open(BytesIO(content))

                        # Конвертация в BGR для OpenCV
                        img_array = np.array(img)
                        if img.mode == "RGBA":
                            img_array = cv2.cvtColor(img_array, cv2.COLOR_RGBA2BGR)
                        else:
                            img_array = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)

                        # Показ изображения
                        window_name = "Image Preview"
                        cv2.imshow(window_name, img_array)
                        cv2.setWindowTitle(window_name, "Press 'y' - accept / 'n' - skip")

                        while True:
                            key = cv2.waitKey(0) & 0xFF

                            if key == ord("y"):
                                result = "recognize"
                                save_image_from_url(url, content)
                                break
                            elif key == ord("n"):
                                result = "skip"
                                break
                            elif key == 27 or cv2.getWindowProperty(window_name, cv2.WND_PROP_VISIBLE) < 1:
                                result = "skip"
                                logger.info("Окно закрыто. Пропуск изображения.")
                                break
                            else:
                                logger.warning("Нажмите только 'y' или 'n'.")

                        cv2.destroyWindow(window_name)

                    except Exception as e:
                        result = "error"
                        logger.error(f"Ошибка при обработке {url}: {e}")

                    # Обновление кэша и результата
                    checked_images[url] = result
                    processed_page_images.append((alt, url, result))
                    logger.info(f"Установлено '{result}' для {url}")

            page["page_images"] = processed_page_images

            if not first:
                f_out.write(",\n")
            else:
                first = False

            f_out.write(json.dumps(page, ensure_ascii=False, indent=4))

        f_out.write("\n]")

    logger.info("Обработка завершена.")


if __name__ == "__main__":
    process_items()
