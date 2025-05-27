"""
summary.py — выводит список всех FileRecord с сортировкой по name,
включая связанные FileVersion и содержимое текстовых файлов.

Требует: database.py и models.py

Запуск:
$ python summary.py
"""

import os
from database import SessionLocal
from models import FileRecord
from sqlalchemy.orm import Session


def indent_text(text: str, spaces: int = 4) -> str:
    """Добавляет отступ ко всем строкам текста."""
    indent = ' ' * spaces
    return '\n'.join(indent + line for line in text.splitlines())


def print_summary(session: Session):
    """Выводит все FileRecord с их версиями и содержимым файлов."""
    file_records = session.query(FileRecord).order_by(FileRecord.name).all()

    if not file_records:
        print("Нет записей FileRecord.")
        return

    for file in file_records:
        print(f"\n=== FileRecord ID: {file.id} ===")
        print(f"Name:          {file.name}")
        print(f"Path:          {file.path}")
        print(f"SHA256:        {file.sha256}")
        print(f"Size:          {file.size}")
        print(f"Last Modified: {file.last_modified}")
        print(f"Created At:    {file.created_at}")

        latest = file.latest_version
        if latest:
            print(f"\n  └── 📌 Последняя версия (ID: {latest.id}, всего версий: {len(file.versions)})")
            print(f"      Method:        {latest.method}")
            print(f"      Processed At:  {latest.processed_at}")
            print(f"      Text Path:     {latest.text_path}")

            if latest.text_path and os.path.isfile(latest.text_path):
                try:
                    with open(latest.text_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        print("      ── Содержимое файла ──")
                        print(indent_text(content, 8))
                except Exception as e:
                    print(f"      [Ошибка чтения файла: {e}]")
            else:
                print("      [Файл не найден]")
        else:
            print("  └── Нет версий.")


def main():
    """Создаёт сессию и выводит сводную информацию."""
    try:
        session = SessionLocal()
        print_summary(session)
    finally:
        session.close()


if __name__ == "__main__":
    main()