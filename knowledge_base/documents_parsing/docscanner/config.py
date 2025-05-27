"""
Конфигурация проекта: пути, параметры и константы.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(BASE_DIR, 'temp')
CONTENT_DIR = os.path.join(BASE_DIR, 'content_files')

# Убедимся, что директории существуют
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(CONTENT_DIR, exist_ok=True)

# Путь к Tesseract (если не в PATH)
TESSERACT_CMD = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# URL для тестирования или по умолчанию (публичная ссылка Яндекс.Диска)
DEFAULT_PUBLIC_URL = 'https://disk.yandex.ru/d/eoPtLFz10rw3Hg'

# База данных sqlite
DB_PATH = os.path.abspath(os.path.join(os.getcwd(), ".", "docscanner.db"))
SQLALCHEMY_DATABASE_URI = f"sqlite:///{DB_PATH}"

# Языки для Tesseract OCR
OCR_LANGUAGES = 'rus+eng'