import hashlib
import requests
import os
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
import filetype
import docx
from PIL import Image

poppler_path = r'C:\poppler-24.08.0\Library\bin'
# Путь к исполняемому файлу Tesseract (если не в PATH)
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# Публичная ссылка Яндекс.Диска
PUBLIC_URL = 'https://disk.yandex.ru/d/eoPtLFz10rw3Hg'

# Создаем папку temp в текущей директории, если её нет
TEMP_DIR = os.path.join(os.getcwd(), 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)


def get_public_resources(public_url, path=''):
    """
    Получение информации по публичной ссылке Яндекс.Диска.
    :param public_url: публичная ссылка на ресурс
    :param path: путь внутри публичного ресурса (директория или файл)
    :return: JSON с информацией о ресурсах
    """
    api_url = 'https://cloud-api.yandex.net/v1/disk/public/resources'
    params = {'public_key': public_url, 'path': path, 'limit': 1000}
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()


def calculate_folder_size(public_url, path=''):
    """
    Рекурсивное вычисление общего размера директории на Яндекс.Диске.
    :param public_url: публичная ссылка на ресурс
    :param path: путь к директории внутри ресурса
    :return: общий размер в байтах
    """
    total_size = 0
    resources = get_public_resources(public_url, path)
    for item in resources['_embedded']['items']:
        if item['type'] == 'dir':
            total_size += calculate_folder_size(public_url, item['path'].replace('disk:', ''))
        else:
            total_size += item.get('size', 0)
    return total_size


def get_file_sha256(download_url):
    """
    Вычисление SHA256 хеша файла по ссылке для скачивания.
    :param download_url: URL файла
    :return: строка с хешем или сообщение об ошибке
    """
    try:
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        sha256 = hashlib.sha256()
        for chunk in response.iter_content(chunk_size=8192):
            sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        return f"Ошибка: {e}"


def detect_file_type(file_path):
    """
    Определение MIME-типа файла по его содержимому (не по расширению).
    Используется библиотека filetype.
    :param file_path: локальный путь к файлу
    :return: MIME-тип или None, если не определён
    """
    kind = filetype.guess(file_path)
    if kind:
        return kind.mime
    return None


def process_pdf(file_path):
    """
    Обработка PDF файла: сначала пытаемся извлечь текст напрямую,
    если не удаётся — применяем OCR.
    :param file_path: путь к PDF файлу
    :return: словарь с текстом и методом обработки
    """
    with pdfplumber.open(file_path) as pdf:
        all_text = ""
        has_text = False

        for page in pdf.pages:
            text = page.extract_text()
            if text and text.strip():
                has_text = True
                all_text += text + "\n"

        if has_text:
            return {"text": all_text, "method": "pdfplumber"}

    # Иначе — OCR
    images = convert_from_path(file_path, dpi=300, poppler_path=poppler_path)
    ocr_text = ""
    for img in images:
        ocr_text += pytesseract.image_to_string(img, lang="rus+eng") + "\n"
    return {"text": ocr_text, "method": "ocr"}


def process_doc(file_path):
    """
    Извлечение текста из DOCX документа.
    :param file_path: путь к DOCX файлу
    :return: словарь с текстом и методом обработки
    """
    doc = docx.Document(file_path)
    text = "\n".join([para.text for para in doc.paragraphs])
    return {"text": text, "method": "docx"}


def process_image(file_path):
    """
    Извлечение текста из изображения с помощью OCR.
    :param file_path: путь к изображению
    :return: словарь с текстом и методом обработки
    """
    image = Image.open(file_path)
    text = pytesseract.image_to_string(image, lang="rus+eng")
    return {"text": text, "method": "image_ocr"}


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


def download_file(download_url, dest_path):
    """
    Скачивание файла по ссылке в указанное место.
    :param download_url: URL для скачивания
    :param dest_path: локальный путь для сохранения
    """
    response = requests.get(download_url, stream=True)
    response.raise_for_status()
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def print_structure(public_url, path='', level=0):
    """
    Рекурсивный обход структуры публичного ресурса Яндекс.Диска,
    скачивание файлов во временную папку, их обработка и вывод информации.
    :param public_url: публичная ссылка
    :param path: текущий путь внутри публичного ресурса
    :param level: уровень вложенности для отступов
    """
    resources = get_public_resources(public_url, path)
    for item in resources['_embedded']['items']:
        item_type = item['type']
        item_name = item['name']
        item_path = item['path'].replace('disk:', '')
        item_modified = item.get('modified', 'н/д')
        item_size = item.get('size', 0)

        indent = "  " * level

        if item_type == 'dir':
            dir_size = calculate_folder_size(public_url, item_path)
            print(f"{indent}📁 {item_name}/ (изменено: {item_modified}) Размер директории: {dir_size} байт")
            print_structure(public_url, item_path, level + 1)
        else:
            download_url = item.get('file')
            file_hash = get_file_sha256(download_url) if download_url else 'недоступен'

            if download_url:
                # Сохраняем файл в папку temp с именем файла
                safe_file_path = os.path.join(TEMP_DIR, item_name)
                try:
                    download_file(download_url, safe_file_path)
                    result = process_file(safe_file_path)
                    method = result.get("method", "неизвестно")
                    text_preview = (result.get("text", "")[:100].replace("\n", " ") + "...") if result.get("text") else ""
                    full_text = result.get("text", "")
                except Exception as e:
                    method = "ошибка"
                    text_preview = str(e)
                finally:
                    # Удаляем файл после обработки
                    if os.path.exists(safe_file_path):
                        os.remove(safe_file_path)
            else:
                method = "нет ссылки для скачивания"
                text_preview = ""
                full_text = ""
                method = "неизвестно"

            print(f"{indent}  📄 {item_name} — {item_size} байт, изменено: {item_modified}, sha256: {file_hash}, обработка: {method}")
            print(f"{indent}    Превью текста: {method}")
            print(f"{indent}    Превью текста: {text_preview}")
            print(full_text)


def main():
    """
    Основная функция запуска.
    """
    print("Начинаем обход структуры Яндекс.Диска и обработку файлов...")
    print_structure(PUBLIC_URL)
    print("Обработка завершена.")


if __name__ == "__main__":
    main()
