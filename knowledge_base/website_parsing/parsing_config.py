import os

MAIN_URL = "https://academydpo.org"
SITEMAP_URL = "https://academydpo.org/sitemap.xml"

# Тестовый режим запрос на TEST_REQUEST_LENGTH ссылок
TEST_MODE = True
TEST_REQUEST_LENGTH = 30

FILE_PREFIX = SITEMAP_URL.split('/')[2].split(".")[0]
PARENT_DIR = os.path.dirname(os.path.abspath(__file__))

# PARSING_OUTPUT_JSON - файл с сохраняемыми данными парсинга сайта
PARSING_OUTPUT_JSON = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_parsed_site.json")

# SITEMAP_DATA_JSON - файл с сохраняемыми данными карты сайта
SITEMAP_DATA_JSON = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_sitemap_data.json")

# TEMP_CHUNKS_DIR директория временного хранения файлов-чанков с результатами парсинга
TEMP_CHUNKS_DIR = os.path.join(PARENT_DIR, "chunks", FILE_PREFIX)

CONCURRENCY_LIMIT = 5 # количество одновременных запросов к страницам

# CLASSES_OF_BASIC_SEMANTIC_ELEMENTS = (("article", "category"), (None, "main__content"), (None, "main"))
BREADCRUMBS_CLASS = ("breadcrumbs", "span")

# EXCLUDE_KEYWORDS элементы с данными id будут исключены из обработки
EXCLUDE_KEYWORDS = ("preload", )

# EXCLUDE_TAGS элементы с данными именами будут исключены из обработки
EXCLUDE_TAGS = [
    "footer", "header", "nav", "menu", "sidebar", "breadcrumb",
    "popup", "modal", "banner", "ad", "subscribe", "widget",
    "cookie", "social", "share", "logo", "script", "style", "form", "input", "iframe", "svg", "noscript",
    "button", "select", "option", "canvas", "link", "meta"
]
# EXCLUDE_CLASSES элементы с данными классами будут исключены из обработки
EXCLUDE_CLASSES = ("coast_block", "express_test_marquiz", "order_tel", "cf7_form", "yandex")
# "coast_block" отзывы"
# "express_test_marquiz" форма обратной связи Экспресс-тест
# "order_tel" форма обратной связи Звонок с телефона
# "cf7_form" форма обратной связи Звонок с телефона
# "yandex" отзывы"

# STYLE_TAGS - стилистические теги которые игнорируются при обработке, из них забирается текст
STYLE_TAGS = {'strong', 'b', 'i', 'em', 'u', 'span'}