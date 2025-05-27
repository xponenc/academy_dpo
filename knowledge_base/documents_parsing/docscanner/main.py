"""
Точка входа. Запускает обход публичной ссылки Яндекс.Диска,
загружает, распознаёт и сохраняет данные в базу.
"""

import os

from config import DEFAULT_PUBLIC_URL
from database import Base, engine
from logger import logger
from scanner import scan_and_process



def main(public_url=None):
    """
    Главная функция запуска процесса сканирования и обработки.
    """
    Base.metadata.create_all(bind=engine, checkfirst=True)

    if not public_url:
        public_url = DEFAULT_PUBLIC_URL

    logger.info(f"Запуск обработки публичной ссылки: {public_url}")

    scan_and_process(public_url)

    logger.info("Обработка завершена.")

if __name__ == '__main__':
    main()