# Настройка логирования
import os

from services.setup_logger import setup_logger

LOG_FILE = "docscanner.log"
LOGS_DIR = os.path.join("logs", "documents_parsing")
logger = setup_logger(__name__, log_dir=LOGS_DIR, log_file=LOG_FILE)