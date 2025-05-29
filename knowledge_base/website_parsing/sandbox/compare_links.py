import json
import re

import ijson
from collections import Counter

pattern_link = r'\[([^\]]+)\]\((https?://[^\)]+)\)'
pattern_internal_link = r'\[([^\]]+)\]\((https://academydpo\.org[^\)]*)\)'
pattern_internal_link_without_docs = r'\[([^\]]+)\]\((https://academydpo\.org/(?![^)]+\.(jpg|jpeg|png|gif|webp|pdf|docx|xlsx))[^)]+)\)'

print("Страница karta-sajta:")
karta_sajta_links = []
with open("page_karta-sajta.json", encoding="utf-8") as f:
    data = json.load(f)
    page_karta_sajta_content = data[0].get("page_content")

    matches = re.findall(pattern_link, page_karta_sajta_content)
    karta_sajta_links = re.findall(pattern_internal_link, page_karta_sajta_content)
    print("\t - обнаружено ссылок: ", len(matches))
    unique_urls = set(item[1] for item in matches)
    print("\t - обнаружено уникальных ссылок: ", len(unique_urls))
    print("\t - обнаружено внутренних ссылок: ", len(karta_sajta_links))

    # # Выводим в нужном формате
    # for label, url in matches:
    #     print(f"Label: {label}\nURL: {url}\n")

print("Страница sitemap.xml:")

xml_map_links = []
page_founded_links = []
page_founded_internal_links = []
page_founded_internal_links_not_docs = []
with open("academydpo_parsed_data_2025_05_29_09-36.json", encoding="utf-8") as f:
    for item in ijson.items(f, "item"):
        url = item.get("loc")
        title = item.get("title")
        page_content = item.get("page_content")
        xml_map_links.append((title, url))

        url_matches = re.findall(pattern_link, page_content)
        page_founded_links.extend(url_matches)

        internal_url_matches = re.findall(pattern_internal_link, page_content)
        page_founded_internal_links.extend(internal_url_matches)

        internal_url_not_docs_matches = re.findall(pattern_internal_link_without_docs, page_content)
        page_founded_internal_links_not_docs.extend(internal_url_not_docs_matches)

print("\t - обнаружено ссылок в xml: ", len(xml_map_links))
unique_urls = set(item[1] for item in xml_map_links)
print("\t - обнаружено уникальных ссылок в xml: ", len(unique_urls))

print("Страницы sitemap.xml не попавшие в karta-sajta:")
xml_just_url = set(item[1] for item in xml_map_links)
karta_sajta_url = set(item[1] for item in karta_sajta_links)
lost_urls = xml_just_url.difference(karta_sajta_url)
print("\t - не попало в Карту:", len(lost_urls))
print("\t\tСписок не попавших в Карту страниц:")
lost_pages = []
for title, url in xml_map_links:
    if url in lost_urls:
        lost_pages.append((title, url))

for label, url in lost_pages:
    print(f"\t\t\t{label}: {url}")


lost_urls = karta_sajta_url.difference(xml_just_url)
print("\t - не попало в XML:", len(lost_urls))
print("\t\tСписок не попавших в XML страниц:")
lost_pages_for_xmlmap = []
for title, url in karta_sajta_links:
    if url in lost_urls:
        lost_pages_for_xmlmap.append((title, url))

for label, url in lost_pages_for_xmlmap:
    print(f"\t\t\t{label}: {url}")

full_xml = xml_map_links + lost_pages_for_xmlmap
print(f"{len(full_xml)=}")






print("\t - обнаружено при парсинге страниц ссылок (c ссылками на документы и popup): ", len(page_founded_links))
unique_urls = set(item[1] for item in page_founded_links)
print("\t - обнаружено при парсинге страниц уникальных ссылок (c ссылками на документы и popup): ", len(unique_urls))

print("\t - обнаружено при парсинге страниц внутренних ссылок (c ссылками на документы и popup): ", len(page_founded_internal_links))
unique_urls = set(item[1] for item in page_founded_internal_links)
print("\t - обнаружено при парсинге страниц уникальных внутренних ссылок (c ссылками на документы и popup): ", len(unique_urls))

print("\t - обнаружено при парсинге страниц внутренних ссылок ( без документов): ", len(page_founded_internal_links_not_docs))
unique_urls = set(item[1] for item in page_founded_internal_links_not_docs)
print("\t - обнаружено при парсинге страниц уникальных внутренних ссылок ( без документов): ", len(unique_urls))

page_founded_internal_links_cleaned = [(label, url) for label, url, _ in page_founded_internal_links_not_docs if "#popup:" not in url]
print("\t - обнаружено при парсинге страниц внутренних ссылок ( без документов и popup): ", len(page_founded_internal_links_cleaned))
unique_urls = set(item[1] for item in page_founded_internal_links_cleaned)
print("\t - обнаружено при парсинге страниц уникальных внутренних ссылок ( без документов и popup): ", len(unique_urls))

if len(page_founded_links) != len(page_founded_internal_links):
    print("Внешние ссылки:")
    external_links = set(page_founded_links).difference(set(page_founded_internal_links))
    for label, url in external_links:
        print(f"\t\t\t{label}: {url}")
#
# # Извлекаем только label
# # labels = [label for label, url in page_founded_links]
#
# # Анализируем топ общий
# counter = Counter(page_founded_links)
# sorted_counts = counter.most_common()
# print("\t\tТоп - 10 по цитированию:")
# for label, count in sorted_counts[:10]:
#     print(f"\t\t\t{label}: {count}")
#
# # Анализируем топ без ссылок на документы
# counter = Counter(page_founded_internal_links_cleaned)
# sorted_counts = counter.most_common()
# print("\t\tТоп - 10 по цитированию (без документов и popup):")
# for label, count in sorted_counts:
#     print(f"\t\t\t{label}: {count}")


site_parsed_urls = set(item[1] for item in page_founded_internal_links_cleaned)
print(len(site_parsed_urls))
full_xml_urls =  set(item[1] for item in full_xml)
print(len(full_xml_urls))

lost_urls = site_parsed_urls.difference(full_xml_urls)
print("\t - не попало в full-XML из парсинга сайта:", len(lost_urls))
print("\t\tСписок не попавших в XML страниц:")
lost_pages_for_full_xmlmap = []
page_founded_internal_links_cleaned_unique = {url: label for label, url in page_founded_internal_links_cleaned}
for url, title in page_founded_internal_links_cleaned_unique.items():
    if url in lost_urls:
        lost_pages_for_full_xmlmap.append((title, url))
for label, url in lost_pages_for_full_xmlmap:
    print(f"\t\t\t{label}: {url}")

if lost_pages_for_xmlmap or lost_pages_for_full_xmlmap:
    with open("academydpo_sitemap_data.json", "r", encoding="utf-8") as f_in, open("academydpo_sitemap_data_full.json", "w", encoding="utf-8") as f_out:
        f_out.write("[\n")
        first = True

        for item in ijson.items(f_in, "item"):
            if not first:
                f_out.write(",\n")
            f_out.write(json.dumps(item, ensure_ascii=False, indent=4))
            first = False
        if lost_pages_for_xmlmap:
            for label, url in lost_pages_for_xmlmap:
                if not first:
                    f_out.write(",\n")
                new_item_data = {
                    "loc": url,
                    "processed": False
                }
                f_out.write(json.dumps(new_item_data, ensure_ascii=False, indent=4))
                first = False
        if lost_pages_for_full_xmlmap:
            for label, url in lost_pages_for_full_xmlmap:
                if not first:
                    f_out.write(",\n")
                new_item_data = {
                    "loc": url,
                    "processed": False
                }
                f_out.write(json.dumps(new_item_data, ensure_ascii=False, indent=4))
                first = False

        f_out.write("\n]")

lost_urls = full_xml_urls.difference(site_parsed_urls)
print("\t - не попало в список url парсинга сайта из XML:", len(lost_urls))
print("\t\tСписок не попавших в XML страниц:")
lost_pages = []
for title, url in full_xml:
    if url in lost_urls:
        lost_pages.append((title, url))
for label, url in lost_pages:
    print(f"\t\t\t{label}: {url}")