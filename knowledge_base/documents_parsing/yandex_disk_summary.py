import os
from collections import defaultdict
import requests


PUBLIC_URL = 'https://disk.yandex.ru/d/eoPtLFz10rw3Hg'


# Глобальный счётчик расширений
extension_counter = defaultdict(int)


def get_public_resources(public_url, path=''):
    """
    Получение информации по публичной ссылке Яндекс.Диска.
    :param public_url: публичная ссылка на ресурс
    :param path: путь внутри публичного ресурса (директория или файл)
    :return: JSON с информацией о ресурсах
    """
    api_url = 'https://cloud-api.yandex.net/v1/disk/public/resources'
    params = {'public_key': public_url, 'path': path, 'limit': 1000}
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()


def print_structure(public_url, path='', level=0):
    """
    Рекурсивный обход структуры публичного ресурса Яндекс.Диска,
    скачивание файлов во временную папку, их обработка и вывод информации.
    """
    resources = get_public_resources(public_url, path)
    for item in resources['_embedded']['items']:
        item_type = item['type']
        item_name = item['name']
        item_path = item['path'].replace('disk:', '')
        item_modified = item.get('modified', 'н/д')
        item_size = item.get('size', 0)

        indent = "  " * level

        if item_type == 'dir':
            print(f"{indent}📁 {item_name}/ (изменено: {item_modified})")
            print_structure(public_url, item_path, level + 1)
        else:
            # Подсчёт по расширению
            ext = os.path.splitext(item_name)[1].lower()
            extension_counter[ext] += 1

            print(f"{indent}  📄 {item_name} — {item_size} байт, изменено: {item_modified}, ")


def main():
    """
    Основная функция запуска.
    """
    print("Начинаем обход структуры Яндекс.Диска и обработку файлов...")
    print_structure(PUBLIC_URL)
    print("\nОбработка завершена.")

    print("\n📊 Распределение файлов по расширениям:")
    for ext, count in sorted(extension_counter.items(), key=lambda x: (-x[1], x[0])):
        ext_display = ext if ext else "[без расширения]"
        print(f"- {ext_display}: {count} файл(ов)")


if __name__ == "__main__":
    main()
