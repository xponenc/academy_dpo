from docx import Document
from docx.oxml.text.paragraph import CT_P
from docx.oxml.table import CT_Tbl
from docx.text.paragraph import Paragraph
from docx.table import Table

def read_docx_in_order(path):
    """
        Читает содержимое .docx файла с сохранением порядка появления элементов.

        Поддерживает:
        - Абзацы (paragraphs)
        - Таблицы (tables)

        Возвращает:
            List[str]: Список строк, каждая строка — абзац или строка таблицы.
                       Ячейки таблиц разделены символом ` | `.
        Пример результата:
            [
                "Полное наименование",
                "Общество с ограниченной ответственностью",
                "ИНН | КПП",
                "12345678 | 87654321"
            ]

        Аргументы:
            path (str): Путь к .docx файлу
        """
    doc = Document(path)
    result = ""

    for element in doc.element.body:
        if isinstance(element, CT_P):
            paragraph = Paragraph(element, doc)
            text = paragraph.text.strip()
            if text:
                result+=f"{text}\n"
        elif isinstance(element, CT_Tbl):
            table = Table(element, doc)
            for row in table.rows:
                cells = [cell.text.strip() for cell in row.cells]
                row_text = " | ".join(cells)
                result+=f"{row_text}\n"

    return result

# Использование
print(read_docx_in_order("sJI5_Polnoe.docx"))