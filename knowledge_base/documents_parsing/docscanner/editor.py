"""
Простейший модуль для работы с сохранённым распознанным текстом.
Позволяет загрузить, сохранить и редактировать текстовый файл.
(Для более продвинутого варианта можно интегрировать web-редактор)
"""

import os

def read_text_file(path: str) -> str:
    """
    Читает и возвращает содержимое текстового файла.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл {path} не найден")
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def save_text_file(path: str, content: str) -> None:
    """
    Сохраняет контент в текстовый файл.
    """
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)