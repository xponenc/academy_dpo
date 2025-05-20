import os
from typing import Dict, Any
from datetime import datetime

import ijson

from services.setup_logger import setup_logger

# Настройка логирования
LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)


def summarize(filename: str) -> Dict[str, Any]:
    """
    Формирует статистику по результатам проверки URL, используя потоковое чтение JSON.
    """
    total = 0
    available = 0
    dates = []
    try:
        with open(filename, mode="r", encoding="utf-8") as f:
            # Потоковое чтение JSON с помощью ijson
            for item in ijson.items(f, "item"):
                total += 1
                if item["status"] == 200:
                    available += 1
                if item["lastmod"]:
                    try:
                        dates.append(datetime.fromisoformat(item["lastmod"]))
                    except ValueError:
                        pass
    except Exception as e:
        logger.error(f"Ошибка при потоковом чтении JSON-файла для статистики: {e}")

    unavailable = total - available
    date_range = (min(dates).date(), max(dates).date()) if dates else (None, None)
    return {
        "Всего ссылок": total,
        "Доступных (200)": available,
        "Недоступных": unavailable,
        "Диапазон дат обновлений": date_range
    }