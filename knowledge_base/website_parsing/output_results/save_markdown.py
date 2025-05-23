import json
import os

output_markdown_file = os.path.join(os.path.dirname(os.path.dirname(__file__))),

with open("academydpo_parsed_site.json", 'r', encoding='utf-8') as f:
    data = json.load(f)

# Открываем Markdown-файл для записи
with open("../academy_dpo_test.md", 'w', encoding='utf-8') as f:
    for item in data:
        print(item)
        loc = item.get('loc', '')
        content = item.get('page_content', '')
        print(content)
        f.write(f'[! page_url {loc}]\n\n')
        f.write(f'{content}\n\n\n')