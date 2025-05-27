"""
Вспомогательные функции:
- загрузка файла по ссылке
- вычисление sha256
- определение mime типа
"""

import hashlib
import requests
import magic
import filetype

def download_file(url: str, dest_path: str) -> None:
    """
    Загружает файл по URL и сохраняет в dest_path.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def get_file_sha256(file_path: str) -> str:
    """
    Вычисляет SHA256 хеш файла.
    """
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()

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