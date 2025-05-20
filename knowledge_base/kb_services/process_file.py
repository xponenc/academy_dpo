import json
import os
import shutil

from project.knowledge_base.parsing_config import TEMP_CHUNKS_DIR
from services.setup_logger import setup_logger

# Настройка логирования
LOG_FILE = "site_parsing.log"
LOGS_DIR = os.path.join("logs", "knowledge_base")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)

def merge_chunks_to_output(output_file: str, chunks_dir: str):
    """
    Собирает данные из чанк-файлов в единый файл
    :param output_file: путь к итоговому файлк
    :param chunks_dir: директория с файлами-чанками
    :return:
    """
    all_data = []
    for filename in sorted(os.listdir(chunks_dir)):
        if filename.startswith("chunk") and filename.endswith(".json"):
            chunk_path = os.path.join(chunks_dir, filename)
            try:
                with open(chunk_path, encoding="utf-8") as f:
                    data = json.load(f)
                    all_data.extend(data)
            except Exception as e:
                logger.warning(f"Ошибка чтения {chunk_path}: {e}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)
    logger.info(f"[✓] Все чанки объединены в {output_file}")

    # Удаляем чанки
    for filename in os.listdir(chunks_dir):
        if filename.startswith("chunk") and filename.endswith(".json"):
            os.remove(os.path.join(chunks_dir, filename))

    logger.info(f"[✓] Все временные чанки удалены")

    if os.path.exists(TEMP_CHUNKS_DIR):
        try:
            shutil.rmtree(TEMP_CHUNKS_DIR)
            logger.info(f"[✓] Временная директория {TEMP_CHUNKS_DIR} удалена.")
        except Exception as e:
            logger.error(f"Ошибка при удалении директории {TEMP_CHUNKS_DIR}: {e}")