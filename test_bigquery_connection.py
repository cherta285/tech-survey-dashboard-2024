# test_bigquery_connection.py
import os
from google.cloud import bigquery
from dotenv import load_dotenv

print("="*70)
print("ПРОВЕРКА ПОДКЛЮЧЕНИЯ К BIGQUERY")
print("="*70)

# Загрузка переменных окружения
load_dotenv()

# Получение переменных
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
project_id = os.getenv('GCP_PROJECT_ID')

print(f"\n📁 Credentials файл: {credentials_path}")
print(f"🔑 Project ID: {project_id}")

# Проверка существования credentials файла
if not os.path.exists(credentials_path):
    print(f"\n❌ ОШИБКА: Файл {credentials_path} не найден!")
    exit(1)
else:
    print(f"✓ Credentials файл найден")

# Установка переменной окружения для Google Cloud
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

try:
    # Создание клиента BigQuery
    print("\n🔄 Подключение к BigQuery...")
    client = bigquery.Client(project=project_id)
    
    # Тестовый запрос
    query = """
        SELECT 
            'Connection successful!' as message,
            CURRENT_TIMESTAMP() as timestamp
    """
    
    print("🔄 Выполнение тестового запроса...")
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        print(f"\n✅ {row.message}")
        print(f"⏰ Время: {row.timestamp}")
    
    print("\n" + "="*70)
    print("✅ ПОДКЛЮЧЕНИЕ К BIGQUERY УСПЕШНО!")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ОШИБКА ПОДКЛЮЧЕНИЯ: {e}")
    print("\nВозможные причины:")
    print("1. Неправильный путь к credentials.json")
    print("2. Неправильный Project ID")
    print("3. BigQuery API не включен")
    print("4. Service Account не имеет нужных прав")