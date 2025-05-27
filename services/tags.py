import re

import ijson
from keybert import KeyBERT
import uuid

# kw_model = KeyBERT()
kw_model = KeyBERT(model="paraphrase-multilingual-MiniLM-L12-v2")

def create_chunk(text, url, category, program, section):
    tags = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3), top_n=10)
    tags = [kw[0] for kw in tags] + [program.lower(), category.lower()]
    return {
        "text": text,
        "metadata": {
            "chunk_id": f"{category.lower()}_{program.lower()}_{section.lower()}_{uuid.uuid4().hex[:8]}",
            "category": category,
            "program": program,
            "section": section,
            "page_url": url,
            "version": "2025-05-25_v1",
            "last_updated": "2025-05-25T20:51:00CEST",
            "language": "ru",
            "content_type": "program_specific",
            "tags": tags,
            "source": "academydpo.org",
            "is_active": True
        }
    }


def preprocess_text(text):
    # Добавляем пробелы перед и после URL
    text = re.sub(r'(https[^\s]+)', r' \1 ', text)
    # Добавляем точки в конце предложений, если их нет
    # text = re.sub(r'([а-яА-Яa-zA-Z])\s+([А-Я])', r'\1. \2', text)
    # Удаляем лишние пробелы
    text = re.sub(r'\s+', ' ', text).strip()
    # Удаляем нестандартные символы, оставляя буквы, цифры и пробелы
    text = re.sub(r'[^\w\s.,!?]', '', text)
    return text


def create_tags(filename):
    with open(filename, mode="r", encoding="utf-8") as f:
        # Потоковое чтение JSON с помощью ijson
        for item in ijson.items(f, "item"):
            content = item.get("page_content")
            text =  preprocess_text(content)
            print(text)
            # tags = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), stop_words="russian", top_n=10)
            tags = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), diversity=0.5, top_n=10)
            return tags

if __name__ == "__main__":
    file_with_parsed_data = "../knowledge_base/website_parsing/output_results/academydpo_parsed_data_2025_05_23_00-13.json"
    tags = create_tags(file_with_parsed_data)
    print(tags)