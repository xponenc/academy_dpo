"""
Модуль для скачивания, сохранения, распознавания и контроля изменений файлов.
Включает:
- загрузку файлов по URL
- сохранение файлов во временную директорию
- распознавание текста из файлов PDF, DOCX, изображений
- проверку изменений файла по SHA256
- запись информации в базу данных через SQLAlchemy
- логгирование ключевых операций
"""

import os
import logging
import hashlib
import requests
from datetime import datetime

import pdfplumber
from pdf2image import convert_from_path
import pytesseract
import docx
from PIL import Image
import magic
import filetype

from database import SessionLocal
from models import FileRecord
from utils import detect_file_type

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

TEMP_DIR = "temp"

# Пути для внешних утилит, если нужно
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
poppler_path = r'C:\poppler-24.08.0\Library\bin'  # для pdf2image, можно настроить


def ensure_temp_dir():
    """
    Создает временную директорию для сохранения файлов, если она не существует.
    """
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
        logger.info(f"Создана директория для временных файлов: {TEMP_DIR}")


def download_file(url: str, filename: str) -> str:
    """
    Скачивает файл по URL и сохраняет в TEMP_DIR с указанным именем.

    Args:
        url (str): URL файла для скачивания.
        filename (str): Имя файла для сохранения.

    Returns:
        str: Полный путь сохраненного файла.
    """
    ensure_temp_dir()
    file_path = os.path.join(TEMP_DIR, filename)
    logger.info(f"Начинаю загрузку файла по URL: {url}")
    response = requests.get(url)
    response.raise_for_status()
    with open(file_path, 'wb') as f:
        f.write(response.content)
    logger.info(f"Файл сохранен: {file_path}")
    return file_path


def calculate_sha256(file_path: str) -> str:
    """
    Вычисляет SHA256 хеш для файла.

    Args:
        file_path (str): Путь к файлу.

    Returns:
        str: SHA256 хеш в шестнадцатеричном виде.
    """
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def process_file(file_path):
    """
    Определение типа файла и вызов соответствующей функции обработки.
    :param file_path: путь к локальному файлу
    :return: результат обработки (текст и метод)
    """
    mime_type = detect_file_type(file_path)
    if mime_type == 'application/pdf':
        return process_pdf(file_path)
    elif mime_type and 'word' in mime_type:
        return process_doc(file_path)
    elif mime_type and 'image' in mime_type:
        return process_image(file_path)
    else:
        raise ValueError(f"Неподдерживаемый тип файла: {mime_type}")


def process_pdf(file_path: str) -> dict:
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
                return {"text": all_text, "method": "pdfplumber"}
    except Exception as e:
        logger.warning(f"pdfplumber не смог обработать файл {file_path}: {e}")

    # OCR fallback
    try:
        images = convert_from_path(file_path, dpi=300, poppler_path=poppler_path)
        ocr_text = ""
        for img in images:
            ocr_text += pytesseract.image_to_string(img, lang="rus+eng") + "\n"
        logger.info(f"Текст распознан OCR из PDF: {file_path}")
        return {"text": ocr_text, "method": "ocr"}
    except Exception as e:
        logger.error(f"Ошибка OCR при обработке PDF: {e}")
        return {"text": "", "method": "ocr_failed"}


def process_doc(file_path: str) -> dict:
    """
    Извлекает текст из DOCX файла.

    Args:
        file_path (str): Путь к DOCX файлу.

    Returns:
        dict: Содержит ключи "text" и "method" с распознанным текстом и методом распознавания.
    """
    try:
        doc = docx.Document(file_path)
        text = "\n".join([para.text for para in doc.paragraphs])
        logger.info(f"Текст извлечён из DOCX: {file_path}")
        return {"text": text, "method": "docx"}
    except Exception as e:
        logger.error(f"Ошибка при обработке DOCX: {e}")
        return {"text": "", "method": "docx_failed"}


def process_image(file_path: str) -> dict:
    """
    Распознает текст из изображения с помощью OCR.

    Args:
        file_path (str): Путь к файлу изображения.

    Returns:
        dict: Содержит ключи "text" и "method" с распознанным текстом и методом распознавания.
    """
    try:
        image = Image.open(file_path)
        text = pytesseract.image_to_string(image, lang="rus+eng")
        logger.info(f"Текст распознан OCR из изображения: {file_path}")
        return {"text": text, "method": "image_ocr"}
    except Exception as e:
        logger.error(f"Ошибка при OCR изображения: {e}")
        return {"text": "", "method": "image_ocr_failed"}


def process_file(file_path: str) -> dict:
    """
    Основная функция обработки файла: определяет тип и применяет соответствующий метод распознавания.

    Args:
        file_path (str): Путь к файлу.

    Returns:
        dict: Словарь с распознанным текстом и методом.
    """
    mime_type = detect_file_type(file_path)
    if mime_type == 'application/pdf':
        return process_pdf(file_path)
    elif 'word' in mime_type:
        return process_doc(file_path)
    elif 'image' in mime_type:
        return process_image(file_path)
    else:
        logger.warning(f"Неподдерживаемый тип файла: {mime_type}")
        return {"text": "", "method": "unsupported"}


def save_file_record(url: str, filename: str, file_path: str, recognized_text: str, method: str):
    """
    Сохраняет или обновляет запись о файле в базе данных.
    Проверяет изменения по SHA256.

    Args:
        url (str): URL исходного файла.
        filename (str): Имя файла.
        file_path (str): Локальный путь к сохранённому файлу.
        recognized_text (str): Распознанный текст.
        method (str): Метод распознавания.
    """
    sha256 = calculate_sha256(file_path)
    session = SessionLocal()
    try:
        file_record = session.query(FileRecord).filter_by(filename=filename).first()
        if file_record:
            if file_record.sha256 == sha256:
                logger.info(f"Файл {filename} не изменился, пропускаем обновление.")
                return
            else:
                logger.info(f"Файл {filename} изменился, обновляем запись.")
                file_record.sha256 = sha256
                file_record.last_modified = datetime.utcnow()
                file_record.recognized_text = recognized_text
                file_record.recognition_method = method
        else:
            logger.info(f"Создаем новую запись для файла {filename}.")
            new_record = FileRecord(
                filename=filename,
                url=url,
                sha256=sha256,
                last_modified=datetime.utcnow(),
                recognized_text=recognized_text,
                recognition_method=method
            )
            session.add(new_record)
        session.commit()
    except Exception as e:
        logger.error(f"Ошибка при сохранении записи файла: {e}")
        session.rollback()
    finally:
        session.close()


def main():
    """
    Пример запуска обработки файла из URL.
    """
    test_url = "https://example.com/somefile.pdf"
    filename = test_url.split("/")[-1]

    try:
        local_path = download_file(test_url, filename)
        result = process_file(local_path)
        save_file_record(test_url, filename, local_path, result["text"], result["method"])
    except Exception as e:
        logger.error(f"Ошибка в основном процессе: {e}")


if __name__ == "__main__":
    main()