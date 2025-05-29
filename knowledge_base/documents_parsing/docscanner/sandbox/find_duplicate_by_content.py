import os
import hashlib
import argparse

def compute_sha512(filepath):
    """
    Вычисляет SHA-512 для данного файла.
    Читает файл по частям, что позволяет обрабатывать большие файлы.
    """
    sha512 = hashlib.sha512()
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha512.update(chunk)
    except Exception as e:
        print(f"Ошибка при чтении файла {filepath}: {e}")
        return None
    return sha512.hexdigest()

def find_duplicates(root_dir):
    """
    Рекурсивно обходит директорию root_dir и строит словарь:
      ключ   - SHA-512 хэш содержимого файла,
      значение - список полных путей к файлам с данным хэшом.
    """
    hash_map = {}
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            file_hash = compute_sha512(filepath)
            if file_hash is None:
                continue  # Пропускаем файлы с ошибками
            hash_map.setdefault(file_hash, []).append(filepath)
    return hash_map

from collections import defaultdict
import os

def print_duplicates_report(hash_map):
    """
    Выводит отчёт по:
    - дубликатам файлов (по SHA-512),
    - общему числу дубликатов и уникальных файлов,
    - распределению по расширениям.
    """
    total_duplicate_files = 0
    total_duplicate_groups = 0
    ext_counter = defaultdict(int)
    total_files = 0

    for file_hash, files in hash_map.items():
        # Считаем расширения
        for f in files:
            total_files += 1
            ext = os.path.splitext(f)[1].lower() or "[без расширения]"
            ext_counter[ext] += 1

        # Отчёт о дубликатах
        if len(files) > 1:
            total_duplicate_groups += 1
            total_duplicate_files += len(files)
            print(f"\n🔗 SHA-512: {file_hash}")
            for f in files:
                try:
                    size = os.path.getsize(f)
                except Exception:
                    size = "неизвестно"
                print(f"  - {f} (размер: {size} байт)")

    # Отчёт по дубликатам
    if total_duplicate_files > 0:
        print("\n📊 Итоговый отчёт:")
        print(f" - Групп дубликатов: {total_duplicate_groups}")
        print(f" - Файлов-дубликатов: {total_duplicate_files}")
        unique_files = total_files - (total_duplicate_files - total_duplicate_groups)
        print(f" - Уникальных файлов (по содержимому): {unique_files}")
    else:
        print("\n✅ Дубликаты не найдены.")

    # Отчёт по расширениям
    print("\n📂 Распределение файлов по расширениям:")
    for ext, count in sorted(ext_counter.items(), key=lambda x: (-x[1], x[0])):
        print(f" - {ext}: {count} файл(ов)")
    print(f"\n📦 Всего файлов: {total_files}")


if __name__ == '__main__':
    import sys

    DEFAULT_PATH = r"E:\ML\ML_projects\Практика\Академия ДПО\001 - Полный Пакет Документов Академии ДПО"  # ← Укажи здесь свою папку по умолчанию

    parser = argparse.ArgumentParser(
        description="Поиск дублирующихся файлов в директории по содержимому (SHA-512)"
    )
    parser.add_argument(
        'directory',
        nargs='?',
        default=DEFAULT_PATH,
        help=f"Путь к директории (по умолчанию: {DEFAULT_PATH})"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"❌ Ошибка: Директория '{args.directory}' не найдена или не является директорией.")
        sys.exit(1)

    print(f"🔍 Начат поиск дубликатов в директории: {args.directory}")
    hash_map = find_duplicates(args.directory)
    print_duplicates_report(hash_map)
