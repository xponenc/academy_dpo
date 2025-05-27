import pdfplumber
from PIL import Image
import pytesseract

with pdfplumber.open("your_file.pdf") as pdf:
    for i, page in enumerate(pdf.pages):
        # Извлекаем изображение страницы
        pil_image = page.to_image(resolution=300).original

        # Распознаём текст с изображения
        text = pytesseract.image_to_string(pil_image, lang='rus+eng')  # Укажи нужный язык

        print(f"--- Страница {i + 1} ---")
        print(text)