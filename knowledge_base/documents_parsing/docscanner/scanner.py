"""
Обход публичных ссылок Яндекс.Диска, загрузка файлов, сохранение в temp,
и вызов обработки с контролем изменений.
"""

import os
import logging
import tempfile
from datetime import datetime, timezone
from database import SessionLocal
from file_processor import process_file
from logger import logger
from models import FileVersion, FileRecord
from utils import download_file, get_file_sha256, detect_file_type
from config import TEMP_DIR, CONTENT_DIR

import requests


YANDEX_API = 'https://cloud-api.yandex.net/v1/disk/public/resources'

def get_public_resources(public_url, path=''):
    """
    Получение информации по публичной ссылке Яндекс.Диска
    """
    params = {'public_key': public_url, 'path': path, 'limit': 1000}
    response = requests.get(YANDEX_API, params=params)
    response.raise_for_status()
    return response.json()

def calculate_folder_size(public_url, path=''):
    """
    Рекурсивно вычисляет общий размер директории
    """
    total_size = 0
    resources = get_public_resources(public_url, path)
    for item in resources['_embedded']['items']:
        if item['type'] == 'dir':
            total_size += calculate_folder_size(public_url, item['path'].replace('disk:', ''))
        else:
            total_size += item.get('size', 0)
    return total_size

def scan_and_process(public_url, path='', level=0):
    """
    Рекурсивный обход структуры, загрузка, обработка и контроль изменений.
    """
    session = SessionLocal()
    resources = get_public_resources(public_url, path)

    for item in resources['_embedded']['items']:
        item_type = item['type']
        item_name = item['name']
        item_path = item['path'].replace('disk:', '')
        item_modified_str = item.get('modified', None)
        item_modified = datetime.fromisoformat(item_modified_str) if item_modified_str else None
        item_size = item.get('size', 0)
        indent = "  " * level

        if item_type == 'dir':
            dir_size = calculate_folder_size(public_url, item_path)
            logger.info(f"{indent}📁 {item_name}/ (изменено: {item_modified_str}) Размер директории: {dir_size} байт")
            scan_and_process(public_url, item_path, level + 1)
        else:
            download_url = item.get('file')
            if not download_url:
                logger.warning(f"{indent}  ⚠️ Нет ссылки на скачивание для файла {item_name}")
                continue

            local_file_path = os.path.join(TEMP_DIR, item_name)

            try:
                download_file(download_url, local_file_path)
                file_sha256 = get_file_sha256(local_file_path)

                # Проверка и обновление БД
                db_file = session.query(FileRecord).filter_by(name=item_name).first()
                if db_file:
                    db_last_mod_aware = db_file.last_modified.replace(tzinfo=timezone.utc)
                    if db_file.sha256 == file_sha256 and db_last_mod_aware == item_modified and db_file.latest_version:
                        logger.info(f"{indent}  📄 {item_name} уже обработан, пропускаем.")
                        continue
                    logger.info(f"{indent}  🔄 Обнаружено изменение в файле {item_name}, обновляем.")
                    full_url = public_url
                    if path:
                        full_url = f"{public_url.rstrip('/')}/{path.lstrip('/')}"
                    db_file.path = download_url
                    db_file.sha256 = file_sha256
                    db_file.size = item_size
                    db_file.last_modified = item_modified
                    db_file.url = full_url
                    session.add(db_file)
                    session.commit()

                    # Новая версия
                    processed_data = process_file(local_file_path)
                    if processed_data.get("method") != "unsupported":
                        text_filename = item_name + '.txt'
                        text_path = os.path.join(CONTENT_DIR, text_filename)

                        with open(text_path, 'w', encoding='utf-8') as tf:
                            tf.write(processed_data['text'])

                        version = FileVersion(
                            file_id=db_file.id,
                            text_path=text_path,
                            method=processed_data.get('method', 'unknown')
                        )
                        version.set_quality_report(processed_data.get('quality_report', {}))
                        session.add(version)
                        session.commit()
                        logger.info(f"{indent}  📄 {item_name} распознанный текст сохранен в {text_path}")
                        logger.info(f"{indent}  ✅ Обновлён файл и создана новая версия.")
                    else:
                        logger.warning(f"{indent}  📄 {item_name} неподдерживаемый тип файла")
                else:
                    logger.info(f"{indent}  📄 {item_name} загружен, начинаем обработку.")
                    processed_data = process_file(local_file_path)
                    if processed_data.get("method") != "unsupported":
                        # Запись в базу
                        full_url = public_url
                        if path:
                            full_url = f"{public_url.rstrip('/')}/{path.lstrip('/')}"
                        new_file = FileRecord(
                            name=item_name,
                            path=local_file_path,
                            sha256=file_sha256,
                            size=item_size,
                            last_modified=item_modified,
                            url=full_url,
                        )
                        session.add(new_file)
                        session.commit()
                        logger.info(f"{indent}  📄 {item_name} сохранен в БД")

                        # Сохраняем распознанный текст в файл
                        text_filename = item_name + '.txt'
                        text_path = os.path.join(CONTENT_DIR, text_filename)
                        with open(text_path, 'w', encoding='utf-8') as tf:
                            tf.write(processed_data['text'])

                        # Версия файла
                        version = FileVersion(
                            file_id=new_file.id,
                            text_path=text_path,
                            method=processed_data.get('method', 'unknown')
                        )
                        version.set_quality_report(processed_data.get('quality_report', {}))
                        print(processed_data.get('quality_report', {}))
                        session.add(version)
                        session.commit()
                        logger.info(f"{indent}  📄 {item_name} распознанный текст сохранен в {text_path}")
                        logger.info(f"{indent}  ✅ {item_name} обработан и сохранён.")
                    else:
                        logger.warning(f"{indent}  📄 {item_name} неподдерживаемый тип файла")

            except Exception as e:
                logger.error(f"{indent}  ❌ Ошибка с файлом {item_name}: {e}")

    session.close()