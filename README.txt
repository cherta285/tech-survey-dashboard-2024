# Tech Survey Dashboard Project

## Описание
Дашборд для анализа технологических трендов среди разработчиков.

## Структура проекта
```
tech_survey_project/
├── data/
│   ├── raw/              # Исходные данные
│   └── processed/        # Обработанные данные
├── scripts/              # Python скрипты
├── bigquery/             # SQL запросы и схемы
├── looker_studio/        # Документация по дашборду
├── docs/                 # Общая документация
├── credentials.json      # Google Cloud credentials (НЕ КОММИТИТЬ!)
├── .env                  # Переменные окружения (НЕ КОММИТИТЬ!)
├── requirements.txt      # Python зависимости
└── README.md             # Этот файл
```

## Быстрый старт
1. Установите зависимости: `pip install -r requirements.txt`
2. Настройте `.env` файл
3. Разместите исходный CSV в `data/raw/`
4. Запустите обработку данных

## Контакты
[Pomogalova Tatyana]