"""
summary.py — выводит список всех FileRecord с сортировкой по name,
включая связанные FileVersion и содержимое текстовых файлов.

Требует: database.py и models.py

Запуск:
$ python summary.py
"""
import csv
import os
from urllib.parse import quote, urlparse, unquote

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
        print(f"Cloud Dir:     {file.url}")
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
            if latest.quality_report:
                quality_report = latest.get_quality_report()
                print(f"      Recognition quality report:")
                for key, value in quality_report.items():
                    print(f"                     {key}: {value}")

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


def extract_relative_path(cloud_url: str) -> str:
    parsed = urlparse(cloud_url)
    full_path = unquote(parsed.path)  # Декодируем %20 → пробелы и кириллицу
    parts = full_path.strip("/").split("/", 2)  # ['d', 'eoPtLFz10rw3Hg', '96/уставные доки/...']
    if len(parts) == 3:
        return parts[2]  # Возвращает: '96/уставные доки/уставные доки'
    return ""

def export_summary_to_csv(session: Session, output_path="summary.csv", preview_chars=1000):
    """Экспортирует данные FileRecord в CSV, сортируя по Cloud Dir."""
    file_records = session.query(FileRecord).order_by(FileRecord.url).all()

    if not file_records:
        print("Нет записей FileRecord.")
        return

    with open(output_path, mode='w', encoding='utf-8', newline='') as csvfile:
        fieldnames = [
            "File ID", "Name", "Cloud Dir",
            "SHA256", "Size", "Last Modified", "Created At DB",
            "Version ID", "Method", "Processed At", "Text Path",
            "Valid Words Ratio", "Quality Report", "Preview Text",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for file in file_records:
            latest = file.latest_version
            if latest:
                report = latest.get_quality_report() if latest.quality_report else {}

                # Превью содержимого файла
                preview = ""
                if latest.text_path and os.path.isfile(latest.text_path):
                    try:
                        with open(latest.text_path, 'r', encoding='utf-8') as f:
                            preview = f.read(preview_chars).replace("\n", " ").strip()
                            if len(preview) == preview_chars:
                                preview += "..."
                    except Exception as e:
                        preview = f"[Ошибка чтения файла: {e}]"
                else:
                    preview = "[Файл не найден]"

                link_url = quote(f"{file.url}/{file.name}", safe=':/')
                dir_display_path = extract_relative_path(file.url)
                dir_encoded_url = quote(file.url, safe=':/')

                if report:
                    quality_lines = [
                        f"total_chars: {report.get('total_chars', '')}",
                        f"total_words: {report.get('total_words', '')}",
                        f"valid_words_count: {report.get('valid_words_count', '')}",
                        f"invalid_words_count: {report.get('invalid_words_count', '')}",
                        f"trash_chars_ratio: {report.get('trash_chars_ratio', '')}",
                        f"invalid_words: {', '.join(report.get('invalid_words', []))}",
                        "most_common_words: " + ', '.join(
                            f"{w[0]}({w[1]})" for w in report.get('most_common_words', []))
                    ]
                    quality_report_str = '\n'.join(quality_lines)
                else:
                    quality_report_str = ""

                writer.writerow({
                    "File ID": file.id,
                    "Name": f'=HYPERLINK("{link_url}"; "{file.name}")',
                    "Cloud Dir": f'=HYPERLINK( "{dir_encoded_url}"; "{dir_display_path}")',
                    "SHA256": file.sha256,
                    "Size": file.size,
                    "Last Modified": file.last_modified,
                    "Created At DB": file.created_at,
                    "Version ID": latest.id,
                    "Method": latest.method,
                    "Processed At": latest.processed_at,
                    "Text Path": latest.text_path,
                    "Valid Words Ratio": report.get("valid_words_ratio"),
                    "Quality Report": quality_report_str,
                    "Preview Text": preview,
                })

    print(f"✅ CSV экспорт завершён: {output_path}")


def save_summary_to_markdown(session: Session, output_path="summary.md"):
    """Сохраняет сводную информацию о файлах в Markdown-файл."""
    file_records = session.query(FileRecord).order_by(FileRecord.name).all()

    if not file_records:
        print("Нет записей FileRecord.")
        return

    with open(output_path, "w", encoding="utf-8") as md:
        md.write("# 📄 Отчёт по FileRecord\n")

        for file in file_records:
            md.write(f"\n## 📁 FileRecord ID: {file.id} — `{file.name}`\n\n")
            md.write(f"- **Cloud Dir:** `{file.url}`\n")
            md.write(f"- **Path:** `{file.path}`\n")
            md.write(f"- **SHA256:** `{file.sha256}`\n")
            md.write(f"- **Size:** `{file.size}` bytes\n")
            md.write(f"- **Last Modified:** `{file.last_modified}`\n")
            md.write(f"- **Created At:** `{file.created_at}`\n")

            latest = file.latest_version
            if latest:
                md.write(f"\n### 🕒 Последняя версия (ID: {latest.id}, всего версий: {len(file.versions)})\n")
                md.write(f"- **Method:** `{latest.method}`\n")
                md.write(f"- **Processed At:** `{latest.processed_at}`\n")
                md.write(f"- **Text Path:** `{latest.text_path}`\n")

                if latest.quality_report:
                    quality_report = latest.get_quality_report()
                    md.write("\n#### 📊 Quality Report\n\n")
                    for key, value in quality_report.items():
                        if isinstance(value, list):
                            value = ', '.join(str(v) for v in value)
                        md.write(f"- **{key}**: `{value}`\n")

                if latest.text_path and os.path.isfile(latest.text_path):
                    try:
                        with open(latest.text_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            md.write("\n#### 📝 Содержимое файла\n\n")
                            md.write("```\n")
                            md.write(content)
                            md.write("\n```\n")
                    except Exception as e:
                        md.write(f"\n⚠️ Ошибка чтения файла: `{e}`\n")
                else:
                    md.write("\n⚠️ Файл не найден\n")
            else:
                md.write("\n❌ Нет версий\n")

    print(f"Markdown-отчёт сохранён в: {output_path}")


def main():
    """Создаёт сессию и выводит сводную информацию."""
    try:
        session = SessionLocal()
        # print_summary(session)
        save_summary_to_markdown(session, output_path="summary.md")
        # export_summary_to_csv(session, output_path="summary.csv")
    finally:
        session.close()


if __name__ == "__main__":
    main()