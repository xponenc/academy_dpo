import os

MAIN_URL = "https://academydpo.org"
SITEMAP_URL = "https://academydpo.org/sitemap.xml"

# Тестовый режим запрос на TEST_REQUEST_LENGTH ссылок
TEST_MODE = False
TEST_REQUEST_LENGTH = 20

FILE_PREFIX = SITEMAP_URL.split('/')[2].split(".")[0]
PARENT_DIR = os.path.dirname(os.path.abspath(__file__))

TEMP_DIR = os.path.join(PARENT_DIR, 'temp')
# Убедимся, что директории существуют
os.makedirs(TEMP_DIR, exist_ok=True)

# PARSING_OUTPUT_DIR - директория с сохраняемыми данными парсинга сайта
PARSING_OUTPUT_DIR = os.path.join(PARENT_DIR, "output_results")

# PARSING_OUTPUT_JSON - файл с сохраняемыми данными парсинга сайта
PARSING_OUTPUT_JSON = f"{FILE_PREFIX}_parsed_data"

# SITEMAP_DATA_JSON - файл с сохраняемыми данными карты сайта
SITEMAP_DATA_JSON = os.path.join(PARENT_DIR, f"{FILE_PREFIX}_sitemap_data.json")

# TEMP_CHUNKS_DIR директория временного хранения файлов-чанков с результатами парсинга
TEMP_CHUNKS_DIR = os.path.join(PARSING_OUTPUT_DIR, "chunks", FILE_PREFIX)

CONCURRENCY_LIMIT = 10 # количество одновременных запросов к страницам

# CLASSES_OF_BASIC_SEMANTIC_ELEMENTS = (("article", "category"), (None, "main__content"), (None, "main"))
BREADCRUMBS_CLASS = ("breadcrumbs", "span")

# EXCLUDE_KEYWORDS элементы с данными id будут исключены из обработки
EXCLUDE_KEYWORDS = ("preload", )

# EXCLUDE_TAGS элементы с данными именами будут исключены из обработки
EXCLUDE_TAGS = [
    "footer", "header", "nav", "menu", "sidebar",
    "popup", "modal", "banner", "ad", "subscribe", "widget",
    "cookie", "social", "share", "logo", "script", "style", "form", "input", "iframe", "svg", "noscript",
    "button", "select", "option", "canvas", "link", "meta", "jdiv"
]
# EXCLUDE_CLASSES элементы с данными классами будут исключены из обработки
EXCLUDE_CLASSES = ("header__top", "coast_block", "express_test_marquiz",
                   "order_tel", "cf7_form", "sw-app", "modal", "calc", "category__info-sale", "breadcrumbs")
# "coast_block" отзывы"
# "express_test_marquiz" форма обратной связи Экспресс-тест
# "order_tel" форма обратной связи Звонок с телефона
# "cf7_form" форма обратной связи Звонок с телефона
# "sw-app" отзывы
# "modal" модальное окно
# "calc" форма расчета стоимости
#"category__info-sale" реклама над заголовком
#"breadcrumbs" хлебные крошки

# STYLE_TAGS - стилистические теги которые игнорируются при обработке, из них забирается текст
STYLE_TAGS = {'strong', 'b', 'i', 'em', 'u', 'span'}