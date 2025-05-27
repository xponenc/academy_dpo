
import ijson

from knowledge_base.website_parsing.parsing_config import PARSING_OUTPUT_JSON

target_url = "https://academydpo.org/kadrovoe-deloproizvodstvo/upravlenie-chelovecheskimi-resursami-hr"
target = None
with open(PARSING_OUTPUT_JSON, mode="r", encoding="utf-8") as f:
    for i, item in enumerate(ijson.items(f, "item")):
        if item.get("loc") == target_url:
            target = item
            break

if target:
    for key, value in target.items():
        if key == "page_content":
            print(key, value)
    md = target.get("page_content")
    with open("test.md", "w", encoding="utf-8") as f:
        f.write(md)

