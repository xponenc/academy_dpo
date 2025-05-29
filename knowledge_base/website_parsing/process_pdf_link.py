import os
import json
import re
import tempfile

import requests
import ijson
import numpy as np
import cv2
from PIL import Image
from io import BytesIO
from urllib.parse import urlparse
import pdfplumber
from pdf2image import convert_from_path
import pytesseract

from knowledge_base.documents_parsing.docscanner.quality_control import evaluate_text_quality
from knowledge_base.website_parsing.parsing_config import FILE_PREFIX, PARSING_OUTPUT_DIR, TEMP_DIR
from services.setup_logger import setup_logger

# === Конфигурация путей ===
INPUT_FILE = os.path.join(PARSING_OUTPUT_DIR, f"academydpo_parsed_data_2025_05_29_09-36.json")
PARSING_OUTPUT_W_READY_IMAGES_JSON = os.path.join(PARSING_OUTPUT_DIR, f"{FILE_PREFIX}_sitemap_data_processed_pdf.json")

LOGS_DIR = os.path.join("logs", "knowledge_base")
LOG_FILE = "process_pdf.log"
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


# Пути для внешних утилит, если нужно
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
poppler_path = r'C:\poppler-24.08.0\Library\bin'  # для pdf2image, можно настроить


# pdfplumber	Извлечение текста, таблиц, линий
# PyPDF2	Объединение, извлечение метаданных
# pdfminer.six	Глубокий парсинг текста и структуры
# PyMuPDF	Быстрый доступ к тексту, изображениям
# pytesseract	OCR (распознавание изображений)
# pdf2image	Конвертация PDF-страниц в изображения

def recognize_pdf(file_path: str) -> dict:
    """
    Распознает текст из PDF файла.
    Сначала пытается извлечь текст напрямую, иначе - OCR по изображениям страниц.

    Args:
        file_path (str): Путь к PDF файлу.

    Returns:
        dict: Содержит ключи "text" и "method" с распознанным текстом и методом распознавания.
    """
    try:
        with pdfplumber.open(file_path) as pdf:
            all_text = ""
            has_text = False
            for page in pdf.pages:
                text = page.extract_text()
                if text and text.strip():
                    has_text = True
                    all_text += text + "\n"
            if has_text:
                logger.info(f"Текст извлечён из PDF напрямую: {file_path}")
                quality_report = evaluate_text_quality(text=text)
                return {"text": all_text, "method": "pdfplumber", "quality_report": quality_report}
    except Exception as e:
        logger.warning(f"pdfplumber не смог обработать файл {file_path}: {e}")

    # OCR fallback
    try:
        images = convert_from_path(file_path, dpi=300, poppler_path=poppler_path)
        ocr_text = ""
        for img in images:
            ocr_text += pytesseract.image_to_string(img, lang="rus+eng") + "\n"
        logger.info(f"Текст распознан OCR из PDF: {file_path}")
        quality_report = evaluate_text_quality(text=ocr_text)
        return {"text": all_text, "method": "ocr", "quality_report": quality_report}
    except Exception as e:
        logger.error(f"Ошибка OCR при обработке PDF: {e}")
        return {"text": "", "method": "ocr_failed"}


def process_pdf():
    """
    Основная функция обработки страниц:
    - Стримит JSON-файл по элементам
    - Находит в page_content ссылки на pdf файлы
    - Получает файл, выполняет парсинг и сохраняет данные в обрабатываемый json элемент страницы
    - Пишет корректный JSON-массив в выходной файл
    """
    pdf_link_pattern = r'\[([^\]]+)\]\((https?://[^\s)]+\.pdf)\)'
    with open(INPUT_FILE, "r", encoding="utf-8") as f_in, open(PARSING_OUTPUT_W_READY_IMAGES_JSON, "w", encoding="utf-8") as f_out:
        f_out.write("[\n")
        first = True

        for page in ijson.items(f_in, "item"):
            content = page.get("page_content")
            pdf_links = re.findall(pdf_link_pattern, content)
            page_inline_pdf = []

            for label, pdf_url in pdf_links:
                print(f"[{label}]({pdf_url})")

                try:
                    headers = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/122.0.0.0 Safari/537.36"
                        )
                    }
                    response = requests.get(pdf_url, headers=headers, timeout=20)
                    response.raise_for_status()

                    # Сохраняем PDF во временный файл
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", dir=TEMP_DIR) as tmp_file:
                        tmp_file.write(response.content)
                        tmp_pdf_path = tmp_file.name

                    # Распознаём PDF
                    result = recognize_pdf(tmp_pdf_path)
                    recognized_content = result.get("text", "").strip()

                except Exception as e:
                    logger.error(f"Ошибка при скачивании или распознавании PDF {pdf_url}: {e}")
                    recognized_content = ""

                finally:
                    # Удаляем временный файл
                    try:
                        if os.path.exists(tmp_pdf_path):
                            os.remove(tmp_pdf_path)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить временный файл {tmp_pdf_path}: {e}")

                # Обновление кэша и результата
                page_inline_pdf.append({
                    "label": label,
                    "url": pdf_url,
                    "recognized_content": recognized_content,
                    "method": result.get('method', 'unknown'),
                    "report": json.dumps(result.get('quality_report', {}), ensure_ascii=False, indent=4)
                })
                logger.info(f"Для {page.get('url')} добавлен PDF {pdf_url}")
            if page_inline_pdf:
                page["page_inline_pdf"] = page_inline_pdf

            if not first:
                f_out.write(",\n")
            else:
                first = False

            f_out.write(json.dumps(page, ensure_ascii=False, indent=4))

        f_out.write("\n]")

    logger.info("Обработка завершена.")


if __name__ == "__main__":
    process_pdf()
