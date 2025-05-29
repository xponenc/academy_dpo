import os
import re
import pymorphy2
import enchant
from collections import Counter



# pymorphy2 — основной морфологический анализатор
# pip install pymorphy2
# pymorphy2-dicts-ru — русские словари для pymorphy2 (нужны для нормальной работы)
# pip install pymorphy2-dicts-ru
# “Enchant” — это название библиотеки для проверки орфографии (например, Python-биндинг к библиотеке Enchant)
# Она помогает проверять правильность написания слов, может использоваться для оценки качества текста.
# pip install pyenchant

# print("Доступные языки словарей:")
# print(enchant.list_languages())  # Выведет список доступных языков
# print(enchant.__file__)
#
# try:
#     d = enchant.Dict("ru_RU")
#     print("Русский словарь доступен!")
# except enchant.errors.DictNotFoundError:
#     print("Русский словарь не найден!")
#     # скачать словарь тут https://github.com/LibreOffice/dictionaries/blob/master/ru_RU/ru_RU.dic
#     #                     https://github.com/LibreOffice/dictionaries/blob/master/ru_RU/ru_RU.aff
#     # положить в .venv/Lib/site-packages/enchant/data/mingw64/share/enchant/hunspell




# Инициализируем анализаторы
morph = pymorphy2.MorphAnalyzer()
os.environ["ENCHANT_CONFIG_DIR"] = os.path.join(os.path.dirname(__file__), "ENCHANT_DICTIONARY")
d = enchant.Dict("ru_RU")


def clean_text(text: str) -> tuple[str, float]:
    """
    Очищает текст от нежелательных символов, оставляя только русские и латинские буквы,
    дефисы и пробелы.

    Вычисляет долю "мусорных" символов (все, что не входит в разрешённый набор).

    Args:
        text (str): Входной текст.

    Returns:
        tuple[str, float]:
            - очищенный текст,
            - доля мусорных символов (float от 0 до 1).
    """
    invalid_pattern = r'[^а-яА-ЯёЁa-zA-Z\- ]'  # разрешены русские/латинские буквы, дефис, пробел
    total_chars = len(text)
    if total_chars == 0:
        return "", 0.0
    invalid_chars = re.findall(invalid_pattern, text)
    invalid_ratio = len(invalid_chars) / total_chars
    cleaned = re.sub(invalid_pattern, ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()  # убираем лишние пробелы
    return cleaned, invalid_ratio

def is_valid_word_combined(word: str) -> bool:
    """
    Проверяет слово на валидность двумя способами:
    1) Через pymorphy2 — слово не должно быть "UNKN" (неизвестное) и длина > 1
    2) Через enchant — слово должно проходить орфографическую проверку.

    Считаем слово валидным, если оно прошло хотя бы одну из проверок.

    Args:
        word (str): Слово для проверки.

    Returns:
        bool: True если слово валидно, False иначе.
    """
    parsed = morph.parse(word)
    morph_valid = ('UNKN' not in parsed[0].tag) and (len(word) > 1)
    if morph_valid:
        return morph_valid
    enchant_valid = d.check(word)
    return enchant_valid

def evaluate_text_quality(text: str) -> dict:
    """
    Основная функция оценки качества распознанного текста.

    Процесс:
    - Выводит исходный текст (для отладки),
    - Очищает текст и вычисляет долю мусорных символов,
    - Делит текст на слова,
    - Определяет валидные и невалидные слова по комбинированной проверке,
    - Считает частоту слов.

    Args:
        text (str): Входной распознанный текст.

    Returns:
        dict: Словарь с результатами оценки:
            - total_chars: количество символов в исходном тексте,
            - trash_chars_ratio: доля мусорных символов,
            - total_words: количество слов,
            - valid_words_count: количество валидных слов,
            - invalid_words_count: количество невалидных слов,
            - valid_words_ratio: доля валидных слов,
            - invalid_words: список невалидных слов (для анализа),
            - most_common_words: 10 самых частых слов с количеством.
    """
    cleaned_text, trash_ratio = clean_text(text.lower())
    words = cleaned_text.split()
    total_words = len(words)
    if total_words == 0:
        return {"error": "Текст пуст после очистки"}

    valid_words = [w for w in words if is_valid_word_combined(w)]
    invalid_words = [w for w in words if not is_valid_word_combined(w)]
    word_freq = Counter(words)

    return {
        "valid_words_ratio": len(valid_words) / total_words,
        "total_chars": len(text),
        "total_words": total_words,
        "valid_words_count": len(valid_words),
        "invalid_words_count": len(invalid_words),
        "trash_chars_ratio": trash_ratio,
        "invalid_words": invalid_words,
        "most_common_words": word_freq.most_common(10)
    }

if __name__ == "__main__":
    # Читаем распознанный текст из файла
    with open("content_files/CCF11052023.pdf.txt", encoding="utf-8") as f:
        ocr_text = f.read()

    result = evaluate_text_quality(ocr_text)

    for key, value in result.items():
        print(f"{key}: {value}")
