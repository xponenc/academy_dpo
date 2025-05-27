1. Определить тип документа (PDF, DOC(X), изображение, другое)
2. Ветвление по типу:
    - PDF
        - Если содержит текст → извлечь текст (pdfplumber / PyMuPDF)
        - Если скан → OCR (pdf2image + pytesseract / EasyOCR)
    - DOC(X)
        - Использовать python-docx или textract
    - Изображение (JPEG/PNG/TIFF)
        - OCR (pytesseract / EasyOCR / DocTR)
3. Вернуть текст + структуру (или объект с мета-данными)