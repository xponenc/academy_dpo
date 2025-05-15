# logger_setup.py
import os
import logging
from logging.handlers import RotatingFileHandler


def setup_logger(name: str, log_dir: str = "logs", log_file: str = "debug.log") -> logging.Logger:
    """
    Создаёт и настраивает логгер с записью в файл и выводом в консоль.

    :param name: Имя логгера (обычно __name__)
    :param log_dir: Папка для логов
    :param log_file: Имя лог-файла
    :return: Готовый логгер
    """
    parent_dir = os.path.dirname(os.path.dirname(__file__))
    log_dir = os.path.join(parent_dir, log_dir)

    if not os.path.exists(log_dir):
        print(f"create log dir {log_dir}")
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    log_path = os.path.join(log_dir, log_file)

    if not logger.handlers:
        file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger