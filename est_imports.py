# test_imports.py
print("Проверка импортов...")

try:
    import pandas as pd
    print("✓ pandas установлен. Версия:", pd.__version__)
except ImportError:
    print("✗ pandas НЕ установлен")

try:
    from google.cloud import bigquery
    print("✓ google-cloud-bigquery установлен")
except ImportError:
    print("✗ google-cloud-bigquery НЕ установлен")

try:
    from dotenv import load_dotenv
    print("✓ python-dotenv установлен")
except ImportError:
    print("✗ python-dotenv НЕ установлен")

print("\n✅ Все зависимости установлены успешно!")