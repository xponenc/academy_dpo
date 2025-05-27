docscanner_project/
├── docscanner/
│   ├── __init__.py
│   ├── config.py          # Конфигурация путей и параметров
│   ├── database.py        # Инициализация SQLAlchemy
│   ├── models.py          # Модели базы данных
│   ├── processing.py      # Распознавание и обработка файлов
│   ├── utils.py           # Утилиты: хеши, MIME, загрузка
│   ├── scanner.py         # Обход Яндекс.Диска и сохранение
│   ├── editor.py          # Работа с файлами текста
│   └── main.py            # Точка входа
├── temp/                  # Каталог для временных файлов
└── README.md              # Инструкция по проекту